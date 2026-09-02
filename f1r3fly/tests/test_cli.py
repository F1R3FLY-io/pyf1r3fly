import json

import grpc
import pytest
from click.testing import CliRunner

from f1r3fly.__main__ import cli
from f1r3fly.crypto import PrivateKey
from f1r3fly.pb.CasperMessage_pb2 import DeployDataProto
from f1r3fly.pb.DeployServiceV1_pb2 import DeployResponse
from f1r3fly.pb.DeployServiceV1_pb2_grpc import DeployServiceServicer
from f1r3fly.util import (
    assemble_single_signer_deploy_data, create_deploy_data, verify_deploy_data,
)

from .test_client import deploy_service

key = PrivateKey.generate()


def test_get_vault_addr_from_private():
    runner = CliRunner()
    result = runner.invoke(cli, ['get-vault-addr', '--input-type', 'private', '--input',
                                 "1000000000000000000000000000000000000000000000000000000000000000"])
    assert result.exit_code == 0
    assert result.output == 'Vault Address is : 1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL\n'

    result = runner.invoke(cli, ["--json-output",'get-vault-addr', '--input-type', 'private', '--input',
                                 "1000000000000000000000000000000000000000000000000000000000000000"])
    assert result.exit_code == 0
    j = json.loads(result.output)
    assert j['vaultAddress'] == "1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL"

def test_get_vault_addr_from_pub():
    runner = CliRunner()
    result = runner.invoke(cli, ['get-vault-addr', '--input-type', 'public', '--input',
                                 "0408ea9666139527a8c1dd94ce4f071fd23c8b350c5a4bb33748c4ba111faccae0620efabbc8ee2782e24e7c0cfb95c5d735b783be9cf0f8e955af34a30e62b945"])
    assert result.exit_code == 0
    assert result.output == 'Vault Address is : 1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL\n'

    result = runner.invoke(cli, ["--json-output", 'get-vault-addr', '--input-type', 'public', '--input',
                                 "0408ea9666139527a8c1dd94ce4f071fd23c8b350c5a4bb33748c4ba111faccae0620efabbc8ee2782e24e7c0cfb95c5d735b783be9cf0f8e955af34a30e62b945"])
    assert result.exit_code == 0
    j = json.loads(result.output)
    assert j['vaultAddress'] == "1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL"

def test_get_vault_addr_from_eth():
    runner = CliRunner()
    result = runner.invoke(cli, ['get-vault-addr', '--input-type', 'eth', '--input',
                                 "7b2419e0ee0bd034f7bf24874c12512acac6e21c"])
    assert result.exit_code == 0
    assert result.output == 'Vault Address is : 1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL\n'

    result = runner.invoke(cli, ["--json-output", 'get-vault-addr', '--input-type', 'eth', '--input',
                                 "7b2419e0ee0bd034f7bf24874c12512acac6e21c"])
    assert result.exit_code == 0
    j = json.loads(result.output)
    assert j['vaultAddress'] == "1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL"

def test_get_vault_addr_from_err():
    runner = CliRunner()
    result = runner.invoke(cli, ['get-vault-addr', '--input-type', 'eth', '--input',
                                 "7b2419e0ee0bd034f7bf24874c12512acac6e1c"])
    assert result.exit_code == 1
    assert result.output == ''


@pytest.mark.parametrize("key,terms,valid_after_block_no,timestamp_millis", [
    (key, "@0!(2)", 1, 1000),
    (key, "@0!(2) | @1!(1)", 10, 1000),
    (key, "@0!(2)", 10, 3000),
])
def test_sign_deploy(key: PrivateKey, terms: str,
                     valid_after_block_no: int, timestamp_millis: int):
    runner = CliRunner()
    result = runner.invoke(cli, ["--json-output",'sign-deploy', '--private-key', key.to_hex(),
                                 '--term', terms,
                                 "--valid-after-block-number", valid_after_block_no,
                                 "--timestamp", timestamp_millis,
                                 "--shard-id", "root",
                                 "--sig-algorithm", "secp256k1"
                                 ])
    assert result.exit_code == 0
    sig = json.loads(result.output)

    unsigned = DeployDataProto(
        term=terms,
        validAfterBlockNumber=valid_after_block_no,
        timestamp=timestamp_millis,
        shardId='root',
        language='rholang',
    )
    data = assemble_single_signer_deploy_data(
        unsigned,
        key.get_public_key(),
        bytes.fromhex(sig['signature']),
    )
    assert data.deployId.hex() == sig['deployId']
    assert verify_deploy_data(
        key.get_public_key(),
        bytes.fromhex(sig['signature']),
        data,
    )


def test_sign_deploy_text_output_distinguishes_signature_and_id() -> None:
    private_key = PrivateKey.from_hex("01" * 32)
    runner = CliRunner()
    result = runner.invoke(cli, [
        'sign-deploy',
        '--private-key', private_key.to_hex(),
        '--term', 'Nil',
        '--valid-after-block-number', '1',
        '--timestamp', '1',
    ])

    assert result.exit_code == 0
    assert "The deploy signature is : " in result.output
    assert "The deploy ID is : " in result.output


@pytest.mark.parametrize("key,terms,valid_after_block_no,timestamp_millis", [
    (key, "@0!(2)", 1, 1000),
    (key, "@0!(2) | @1!(1)", 10, 1000),
    (key, "@0!(2)", 10, 3000),
])
def test_submit_deploy(key: PrivateKey, terms: str,
                       valid_after_block_no: int, timestamp_millis: int):
    class DummyDeploySerivce(DeployServiceServicer):
        def doDeploy(self, request: DeployDataProto, context: grpc.ServicerContext) -> DeployResponse:
            return DeployResponse(result=request.deployId.hex())

    with deploy_service(DummyDeploySerivce()) as (server, port):
        signed = create_deploy_data(
            key,
            terms,
            valid_after_block_no,
            timestamp_millis,
            shard_id="root",
        )
        signature = signed.authorizationV61.witnesses[0].signature.hex()
        runner = CliRunner()
        result = runner.invoke(cli, ["--json-output", 'submit-deploy', '--deployer', key.get_public_key().to_hex(),
                                     '--term', terms,
                                     "--valid-after-block-number", valid_after_block_no,
                                     "--timestamp", timestamp_millis,
                                     "--shard-id", "root",
                                     "--sig-algorithm", "secp256k1",
                                     "--sig", signature,
                                     "--host", 'localhost',
                                     "--port", port
                                     ])
        assert result.exit_code == 0
        res = json.loads(result.output)
        assert res['deployID'] == signed.deployId.hex()
