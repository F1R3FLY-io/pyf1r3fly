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
def submission_client(message=None, status=None, missing=False):
    seen = []

    class Service(DeployServiceServicer):
        def doDeploy(self, request, context):
            seen.append(request.SerializeToString())
            if status is not None:
                context.abort(status, "submission response unavailable")
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
