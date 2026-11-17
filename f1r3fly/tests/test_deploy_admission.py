from concurrent import futures
from contextlib import contextmanager

import grpc
import pytest

from f1r3fly.client import (
    DeployAdmissionRefused, DeploySubmissionUnknown, F1r3flyClient,
    F1r3flyClientException,
)
from f1r3fly.crypto import PrivateKey
from f1r3fly.pb.DeployServiceV1_pb2 import DeployResponse
from f1r3fly.pb.DeployServiceV1_pb2_grpc import (
    DeployServiceServicer, add_DeployServiceServicer_to_server,
)
from f1r3fly.pb.ServiceError_pb2 import ServiceError
from f1r3fly.util import create_deploy_data


@contextmanager
def submission_client(message=None, status=None, missing=False, details=None,
                      retry_after=None):
    seen = []

    class Service(DeployServiceServicer):
        def doDeploy(self, request, context):
            seen.append(request.SerializeToString())
            if retry_after is not None:
                context.set_trailing_metadata((("retry-after", retry_after),))
            if status is not None:
                context.abort(status, details or "submission response unavailable")
            if missing:
                return DeployResponse()
            if message is not None:
                return DeployResponse(error=ServiceError(messages=[message]))
            return DeployResponse(result=request.sig.hex())

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_DeployServiceServicer_to_server(Service(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with F1r3flyClient("127.0.0.1", port) as client:
            yield client, seen
    finally:
        server.stop(0).wait()


@pytest.fixture
def signed():
    return create_deploy_data(PrivateKey.generate(), "Nil", 1, 10000, 1)


@pytest.mark.parametrize("code", ["deploy_pool_busy", "deploy_admission_busy"])
def test_explicit_refusal_keeps_code_id_and_original_bytes(signed, code):
    original = signed.SerializeToString()
    with submission_client(message=code + ": retry later") as (client, seen):
        with pytest.raises(DeployAdmissionRefused) as caught:
            client.send_deploy(signed)
        assert caught.value.code == code
        assert caught.value.deploy_id == signed.sig.hex()
        assert isinstance(caught.value, F1r3flyClientException)
        assert seen == [original]
        assert signed.SerializeToString() == original


@pytest.mark.parametrize("message", [
    "unavailable", "text mentions deploy_pool_busy: later",
    "prefix_deploy_admission_busy: busy",
])
def test_other_service_errors_are_not_classified_as_admission(signed, message):
    with submission_client(message=message) as (client, seen):
        with pytest.raises(F1r3flyClientException) as caught:
            client.send_deploy(signed)
        assert type(caught.value) is F1r3flyClientException
        assert len(seen) == 1


@pytest.mark.parametrize("status", [
    grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE,
])
def test_rpc_failure_is_unknown_without_retry(signed, status):
    with submission_client(status=status) as (client, seen):
        with pytest.raises(DeploySubmissionUnknown) as caught:
            client.send_deploy(signed)
        assert caught.value.deploy_id == signed.sig.hex()
        assert isinstance(caught.value.__cause__, grpc.RpcError)
        assert seen == [signed.SerializeToString()]


def test_missing_response_is_unknown(signed):
    with submission_client(missing=True) as (client, _):
        with pytest.raises(DeploySubmissionUnknown) as caught:
            client.send_deploy(signed)
        assert caught.value.deploy_id == signed.sig.hex()


def test_accepted_deploy_returns_signed_id(signed):
    with submission_client() as (client, seen):
        assert client.send_deploy(signed) == signed.sig.hex()
        assert seen == [signed.SerializeToString()]


@pytest.mark.parametrize("code,retryable", [
    ("invalid_deploy_window", True),
    ("deploy_too_large", False),
    ("deploy_expired", False),
])
def test_other_node_refusal_codes_are_classified(signed, code, retryable):
    with submission_client(message=code + ": not accepted") as (client, seen):
        with pytest.raises(DeployAdmissionRefused) as caught:
            client.send_deploy(signed)
        assert caught.value.code == code
        assert caught.value.retryable is retryable
        assert caught.value.retry_after is None
        assert len(seen) == 1


def test_node_busy_refusals_allow_the_same_deploy(signed):
    with submission_client(message="deploy_pool_busy: busy") as (client, _):
        with pytest.raises(DeployAdmissionRefused) as caught:
            client.send_deploy(signed)
        assert caught.value.retryable is True


@pytest.mark.parametrize("details,congestion,retryable", [
    ("deploy queue is full: the validator is taking fewer deploys than arrive, so ...",
     "validator", False),
    ("deploy queue is full: the validator has created no block while the shard moved on, so ...",
     "validator_idle", False),
    ("deploy queue is full: the shard's finality is holding the validator back, so ...",
     "shard", False),
    ("deploy queue is full: the signed deploys the queue holds fill its storage budget, so ...",
     "storage", False),
    ("deploy queue is full for this deployer: it holds 4 of the 4 deploys one deployer may hold",
     "deployer_share", True),
])
def test_full_deploy_queue_is_a_refusal_with_wait_and_cause(signed, details, congestion, retryable):
    with submission_client(status=grpc.StatusCode.RESOURCE_EXHAUSTED, details=details,
                           retry_after="12") as (client, seen):
        with pytest.raises(DeployAdmissionRefused) as caught:
            client.send_deploy(signed)
        refusal = caught.value
        assert refusal.code == "deploy_queue_full"
        assert refusal.congestion == congestion
        assert refusal.retryable is retryable
        assert refusal.retry_after == 12.0
        assert refusal.deploy_id == signed.sig.hex()
        assert isinstance(refusal.__cause__, grpc.RpcError)
        assert seen == [signed.SerializeToString()]


@pytest.mark.parametrize("details,code,retry_after", [
    ("deploy queue is not connected to its node; retry in 5s", "node_unavailable", "5"),
    ("deploy queue is shutting down", "deploy_queue_stopped", None),
])
def test_unavailable_deploy_queue_is_a_retryable_refusal(signed, details, code, retry_after):
    with submission_client(status=grpc.StatusCode.UNAVAILABLE, details=details,
                           retry_after=retry_after) as (client, _):
        with pytest.raises(DeployAdmissionRefused) as caught:
            client.send_deploy(signed)
        assert caught.value.code == code
        assert caught.value.retryable is True
        assert caught.value.congestion is None
        assert caught.value.retry_after == (float(retry_after) if retry_after else None)


@pytest.mark.parametrize("status,details", [
    (grpc.StatusCode.RESOURCE_EXHAUSTED, "message length too large"),
    (grpc.StatusCode.INTERNAL, "queue store failed"),
    (grpc.StatusCode.UNKNOWN, "deploy queue is full: the shard's finality"),
])
def test_other_rpc_statuses_stay_unknown(signed, status, details):
    with submission_client(status=status, details=details) as (client, _):
        with pytest.raises(DeploySubmissionUnknown):
            client.send_deploy(signed)


def test_a_deploy_already_known_returns_its_id(signed):
    message = "Deploy already known: " + signed.sig.hex()
    with submission_client(message=message) as (client, _):
        assert client.send_deploy(signed) == signed.sig.hex()


def test_another_deploy_already_known_is_not_acceptance(signed):
    with submission_client(message="Deploy already known: 00ff") as (client, _):
        with pytest.raises(F1r3flyClientException) as caught:
            client.send_deploy(signed)
        assert type(caught.value) is F1r3flyClientException


def test_a_rejected_deploy_ends_the_finalization_wait():
    from f1r3fly.pb.DeployServiceCommon_pb2 import DeployFinalizationStatusInfo
    from f1r3fly.polling import (
        DEPLOY_STATE_REJECTED, DeployError, wait_for_deploy_finalized,
    )

    class QueueClient:
        calls = 0

        def deploy_finalization_status(self, deploy_id):
            QueueClient.calls += 1
            return DeployFinalizationStatusInfo(state=DEPLOY_STATE_REJECTED)

    with pytest.raises(DeployError, match="Rejected"):
        wait_for_deploy_finalized(QueueClient(), "ab" * 32, timeout=30, interval=0.01)
    assert QueueClient.calls == 1
