import hashlib
from typing import Callable

import pytest

from f1r3fly.crypto import (
    BlockHashScheme, gen_block_hash_from_block, gen_deploys_hash_from_block,
)
from f1r3fly.pb.CasperMessage_pb2 import (
    BlockMessageProto, ProcessedDeployProto, ProcessedSystemDeployProto,
)

MERKLE_SCHEMES = ('merkle-v1', 'merkle-v1-justifications')


def empty_block() -> BlockMessageProto:
    block = BlockMessageProto(
        sender=bytes(33), sigAlgorithm='secp256k1', shardId='test-shard',
    )
    block.header.timestamp = 1_700_000_000_000
    block.header.version = 1
    block.body.state.preStateHash = bytes(32)
    block.body.state.postStateHash = bytes(32)
    return block


def populated_block() -> BlockMessageProto:
    block = empty_block()
    block.header.parentsHashList.extend([b'a' * 32, b'b' * 32])
    block.header.extraBytes = b'header'
    block.body.state.blockNumber = 42
    block.body.state.bonds.add(validator=b'a' * 33, stake=100)
    block.body.state.bonds.add(validator=b'b' * 33, stake=200)
    block.body.extraBytes = b'body'
    block.body.mergeBase = b'm' * 32
    block.body.appliedFromScope.extend([b'a' * 64, b'b' * 64])
    block.body.rejectedDeploys.add(sig=b'c' * 64, carrier=b'x' * 32)
    block.body.rejectedDeploys.add(sig=b'd' * 64, duplicate=True, carrier=b'y' * 32)
    for i in range(2):
        pd = block.body.deploys.add()
        pd.deploy.deployer = b'p' * 65
        pd.deploy.term = 'Nil'
        pd.deploy.timestamp = i + 1
        pd.deploy.sig = bytes([i]) * 64
        pd.deploy.sigAlgorithm = 'secp256k1'
        pd.deploy.phloPrice = 1
        pd.deploy.phloLimit = 1000
        pd.deploy.shardId = 'test-shard'
        pd.deploy.expirationTimestamp = 1_700_000_100_000
        pd.deploy.parameters.add(name='answer').value.int_value = 42
        pd.cost.cost = 7
        pd.deployLog.add().produce.hash = b'e' * 32
    block.body.systemDeploys.add().systemDeploy.closeBlockSystemDeploy.SetInParent()
    slash = block.body.systemDeploys.add().systemDeploy.slashSystemDeploy
    slash.invalidBlockHash = b'i' * 32
    slash.issuerPublicKey = b'p' * 65
    slash.targetActivationEpoch = 123
    block.body.systemDeploys.add(errorMsg='failed')
    block.justifications.add(validator=b'a' * 33, latestBlockHash=b'j' * 32)
    block.justifications.add(validator=b'b' * 33, latestBlockHash=b'k' * 32)
    return block


@pytest.mark.parametrize('scheme,expected', [
    ('legacy', 'a62563994a4cba3a130b8f1cdf85e40d8813d852beb04fbe92e4df8f84cd3efa'),
    ('merkle-v1', 'e1959261b81b13412671713f51402b701fee1e1e261970e0f9215df87b051d23'),
    ('merkle-v1-justifications', '249c70aa27fa8353d0373e13d6e9fc8ed4f50fc77d40bf901531fdbd4bc3884f'),
])
def test_empty_block_golden(scheme: BlockHashScheme, expected: str) -> None:
    # Merkle constants are the Rust hash_block_empty_golden_hex vectors
    # before #790 and at #790 head 277e83a2c48e2c3bc884588cc3e756744f170eff.
    assert gen_block_hash_from_block(empty_block(), scheme=scheme).hex() == expected


def test_legacy_default_is_preserved() -> None:
    block = populated_block()
    assert gen_block_hash_from_block(block) == gen_block_hash_from_block(block, scheme='legacy')
    expected = hashlib.blake2b(
        b''.join(pd.SerializeToString() for pd in block.body.deploys), digest_size=32,
    ).digest()
    assert gen_deploys_hash_from_block(block) == expected


@pytest.mark.parametrize('scheme', MERKLE_SCHEMES)
def test_merkle_deploys_root_domains_and_odd_tail(scheme: BlockHashScheme) -> None:
    block = empty_block()
    assert gen_deploys_hash_from_block(block, scheme=scheme) == bytes(32)
    user = bytes.fromhex('0a0512034e696c12020807')
    close = bytes.fromhex('0a021200')
    failed = bytes.fromhex('1a046f6f7073')
    block.body.deploys.append(ProcessedDeployProto.FromString(user))
    leaf_user = hashlib.blake2b(b'\x10' + user, digest_size=32).digest()
    assert gen_deploys_hash_from_block(block, scheme=scheme) == leaf_user
    block.body.systemDeploys.append(ProcessedSystemDeployProto.FromString(close))
    block.body.systemDeploys.append(ProcessedSystemDeployProto.FromString(failed))
    leaf_close = hashlib.blake2b(b'\x11' + close, digest_size=32).digest()
    leaf_failed = hashlib.blake2b(b'\x11' + failed, digest_size=32).digest()
    pair = hashlib.blake2b(b'\x01' + leaf_user + leaf_close, digest_size=32).digest()
    expected = hashlib.blake2b(b'\x01' + pair + leaf_failed, digest_size=32).digest()
    assert gen_deploys_hash_from_block(block, scheme=scheme) == expected


@pytest.mark.parametrize('scheme', MERKLE_SCHEMES)
@pytest.mark.parametrize('path,value', [
    ('header.timestamp', -1), ('header.version', 2),
    ('header.extraBytes', b'changed'),
    ('body.state.preStateHash', b'c' * 32),
    ('body.state.postStateHash', b'c' * 32),
    ('body.state.blockNumber', -(2 ** 63)),
    ('body.extraBytes', b'changed'), ('body.mergeBase', b'c' * 32),
    ('sender', b'c' * 33), ('seqNum', -(2 ** 31)),
    ('sigAlgorithm', 'other'), ('shardId', 'other'), ('extraBytes', b'changed'),
])
def test_header_and_state_fields_are_committed(
        scheme: BlockHashScheme, path: str, value: object,
) -> None:
    block = populated_block()
    original = gen_block_hash_from_block(block, scheme=scheme)
    target = block
    fields = path.split('.')
    for field in fields[:-1]:
        target = getattr(target, field)
    setattr(target, fields[-1], value)
    assert gen_block_hash_from_block(block, scheme=scheme) != original


@pytest.mark.parametrize('scheme', MERKLE_SCHEMES)
@pytest.mark.parametrize('mutation', [
    lambda b: setattr(b.body.state.bonds[0], 'stake', -1),
    lambda b: setattr(b.body.state.bonds[0], 'validator', b'c' * 33),
    lambda b: setattr(b.body.rejectedDeploys[0], 'duplicate', True),
    lambda b: setattr(b.body.rejectedDeploys[0], 'carrier', b'z' * 32),
    lambda b: setattr(b.body.rejectedDeploys[0], 'sig', b'z' * 64),
    lambda b: b.body.appliedFromScope.append(b'z' * 64),
    lambda b: setattr(b.body.deploys[0].deploy, 'expirationTimestamp', 0),
    lambda b: setattr(b.body.deploys[0].deploy.parameters[0].value, 'int_value', 43),
    lambda b: setattr(b.body.deploys[0].cost, 'cost', 8),
    lambda b: setattr(b.body.deploys[0].deployLog[0].produce, 'hash', b'z' * 32),
    lambda b: setattr(b.body.deploys[0], 'errored', True),
    lambda b: setattr(b.body.deploys[0], 'systemDeployError', 'error'),
    lambda b: setattr(b.body.systemDeploys[1].systemDeploy.slashSystemDeploy,
                      'targetActivationEpoch', 124),
    lambda b: setattr(b.body.systemDeploys[2], 'errorMsg', 'other failure'),
    lambda b: b.body.deploys.reverse(),
    lambda b: b.body.systemDeploys.reverse(),
])
def test_body_subtrees_are_committed(
        scheme: BlockHashScheme, mutation: Callable[[BlockMessageProto], object],
) -> None:
    block = populated_block()
    original = gen_block_hash_from_block(block, scheme=scheme)
    mutation(block)
    assert gen_block_hash_from_block(block, scheme=scheme) != original


@pytest.mark.parametrize('scheme', MERKLE_SCHEMES)
def test_unordered_subtrees_and_excluded_fields(scheme: BlockHashScheme) -> None:
    block = populated_block()
    original = gen_block_hash_from_block(block, scheme=scheme)
    block.body.state.bonds.reverse()
    block.body.rejectedDeploys.reverse()
    block.body.appliedFromScope.reverse()
    block.sig = b'signature'
    block.blockHash = b'supplied hash'
    serialized = block.SerializeToString()
    assert gen_block_hash_from_block(block, scheme=scheme) == original
    assert block.SerializeToString() == serialized


@pytest.mark.parametrize('mutation', [
    lambda b: setattr(b.justifications[0], 'validator', b'c' * 33),
    lambda b: setattr(b.justifications[0], 'latestBlockHash', b'z' * 32),
    lambda b: b.justifications.pop(),
    lambda b: b.justifications.add(validator=b'c' * 33, latestBlockHash=b'z' * 32),
    lambda b: b.justifications.reverse(),
])
def test_justifications_are_committed_only_in_the_new_profile(
        mutation: Callable[[BlockMessageProto], object],
) -> None:
    block = populated_block()
    before = gen_block_hash_from_block(block, scheme='merkle-v1')
    after = gen_block_hash_from_block(block, scheme='merkle-v1-justifications')
    mutation(block)
    assert gen_block_hash_from_block(block, scheme='merkle-v1') == before
    assert gen_block_hash_from_block(block, scheme='merkle-v1-justifications') != after


def test_justification_validator_boundary_is_committed() -> None:
    block = empty_block()
    justification = block.justifications.add(validator=b'a', latestBlockHash=b'bc')
    original = gen_block_hash_from_block(block, scheme='merkle-v1-justifications')
    justification.validator = b'ab'
    justification.latestBlockHash = b'c'
    assert gen_block_hash_from_block(block, scheme='merkle-v1-justifications') != original


@pytest.mark.parametrize('scheme', MERKLE_SCHEMES)
@pytest.mark.parametrize('path', ['header', 'body', 'body.state'])
def test_missing_required_messages_are_rejected(scheme: BlockHashScheme, path: str) -> None:
    block = empty_block()
    if path == 'body.state':
        block.body.ClearField('state')
    else:
        block.ClearField(path)
    with pytest.raises(ValueError, match='requires'):
        gen_block_hash_from_block(block, scheme=scheme)


@pytest.mark.parametrize('scheme', MERKLE_SCHEMES)
@pytest.mark.parametrize('nested', [False, True])
def test_unknown_protobuf_fields_are_rejected(scheme: BlockHashScheme, nested: bool) -> None:
    block = populated_block()
    target = block.body.deploys[0].deploy if nested else block
    target.MergeFromString(b'\xf8\x07\x01')
    with pytest.raises(ValueError, match='Unsupported protobuf fields'):
        gen_block_hash_from_block(block, scheme=scheme)


def test_unknown_scheme_is_rejected() -> None:
    for helper in (gen_block_hash_from_block, gen_deploys_hash_from_block):
        with pytest.raises(ValueError, match='Unsupported block hash scheme'):
            helper(empty_block(), scheme='merkle-v2')
