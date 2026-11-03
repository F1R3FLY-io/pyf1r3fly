import os

import pytest
from ecdsa.keys import BadSignatureError

from f1r3fly.crypto import (
    PrivateKey, PublicKey, _hash_leaf, _merkle_root, blake2b_32,
    gen_block_hash_from_block, verify_vault_address,
)
from f1r3fly.pb.CasperMessage_pb2 import CLAIM_FAMILY_EJECT, BlockMessageProto


@pytest.mark.parametrize('input_str,expected_output', [
    ('', '0e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a8'),
    ('foo', 'b8fe9f7f6255a6fa08f668ab632a8d081ad87983c77cd274e48ce450f0b349fd'),
    (
            'sRHyrQZuEdI5WyvJD1YwCVaUpFEHCCZXF7HIIPMUIABwwr6EWNPRB1s3XkYzobM8TxHA2NHpxjzUzIxk',
            'cb44058e45aeca88339b16c53d3e2544246bc9a6aa4f831cf2099d70037aba61'
    ),
])
def test_blake2b_32(input_str: str, expected_output: str) -> None:
    actual_output = blake2b_32(input_str.encode()).digest().hex()
    assert expected_output == actual_output


keys_identities = [
    (
        '1000000000000000000000000000000000000000000000000000000000000000',
        '0408ea9666139527a8c1dd94ce4f071fd23c8b350c5a4bb33748c4ba111faccae0620efabbc8ee2782e24e7c0cfb95c5d735b783be9cf0f8e955af34a30e62b945',
        '1111cnoFDAa7GubxBMHpPLbbediPegnjSdZwNjxg9oqYvSvSmfqQL'
    ),
    (
        '1111111111111111111111111111111111111111111111111111111111111111',
        '044f355bdcb7cc0af728ef3cceb9615d90684bb5b2ca5f859ab0f0b704075871aa385b6b1b8ead809ca67454d9683fcf2ba03456d6fe2c4abe2b07f0fbdbb2f1c1',
        '111125UED2qbF3aman9PYvfeTk81TuP4fVL5nYsEXtFyTtaLPcHW61'
    ),
    (
        '2222222222222222222222222222222222222222222222222222222222222222',
        '04466d7fcae563e5cb09a0d1870bb580344804617879a14949cf22285f1bae3f276728176c3c6431f8eeda4538dc37c865e2784f3a9e77d044f33e407797e1278a',
        '1111yoPaoSxvmNRGDpP9GUGToHaCdUFEu3KdbNgTuyydUgg6cty7g'
    )
]


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_privatekey_function(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    key = PrivateKey.from_hex(key_hex)
    pub_key = key.get_public_key()
    actual_pub_key_hex = pub_key.to_bytes().hex()
    assert pub_key_hex == actual_pub_key_hex


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_publickey_from_hex(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    key = PrivateKey.from_hex(key_hex)
    pub_key = PublicKey.from_hex(pub_key_hex)
    expected_pub_key_bytes = key.get_public_key().to_bytes()
    actual_pub_key_bytes = pub_key.to_bytes()
    assert expected_pub_key_bytes == actual_pub_key_bytes


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_key_to_hex(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    private_key = PrivateKey.from_hex(key_hex)
    assert private_key.to_hex() == key_hex
    public_key = PublicKey.from_hex(pub_key_hex)
    assert public_key.to_hex() == pub_key_hex


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_sign_verify_good(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    key = PrivateKey.from_hex(key_hex)
    pub_key = PublicKey.from_hex(pub_key_hex)
    message = 'hello f1r3fly'.encode()
    signature = key.sign(message)
    pub_key.verify(signature, message)


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_sign_verify_bad(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    key = PrivateKey.from_hex(key_hex)
    pub_key = PublicKey.from_hex(pub_key_hex)
    sign_message = 'hello f1r3fly'.encode()
    verify_message = 'hello world'.encode()
    signature = key.sign(sign_message)
    with pytest.raises(BadSignatureError):
        pub_key.verify(signature, verify_message)


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_sign_block_hash_verify_good(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    key = PrivateKey.from_hex(key_hex)
    pub_key = PublicKey.from_hex(pub_key_hex)
    message = 'hello f1r3fly'.encode()
    digest = blake2b_32(message).digest()
    signature = key.sign_block_hash(digest)
    pub_key.verify_block_hash(signature, digest)


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_sign_block_hash_verify_bad(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    key = PrivateKey.from_hex(key_hex)
    pub_key = PublicKey.from_hex(pub_key_hex)
    sign_message = 'hello f1r3fly'.encode()
    verify_message = 'hello world'.encode()
    block_hash = blake2b_32(sign_message).digest()
    signature = key.sign_block_hash(block_hash)
    verify_digest = blake2b_32(verify_message).digest()
    with pytest.raises(BadSignatureError):
        pub_key.verify_block_hash(signature, verify_digest)


@pytest.mark.parametrize("key_hex,pub_key_hex,vault_address", keys_identities)
def test_address(key_hex: str, pub_key_hex: str, vault_address: str) -> None:
    pub_key = PublicKey.from_hex(pub_key_hex)
    actual_address = pub_key.get_vault_address()
    assert vault_address == actual_address


def test_private_key_from_eth_path():
    key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources/key.json')
    PrivateKey.from_eth_keyfile(key_path, 'testpassword')

def test_vault_address_validation():
    for i in range(20):
        key = PrivateKey.generate()
        assert verify_vault_address(key.get_public_key().get_vault_address()) is True


def _empty_block() -> BlockMessageProto:
    block = BlockMessageProto()
    block.header.timestamp = 1_700_000_000_000
    block.header.version = 1
    block.body.state.preStateHash = bytes(32)
    block.body.state.postStateHash = bytes(32)
    block.sender = bytes(33)
    block.sigAlgorithm = 'secp256k1'
    block.shardId = 'test-shard'
    return block


def _leaf(byte: int) -> bytes:
    return _hash_leaf(0x00, bytes([byte]))


def _interior(left: bytes, right: bytes) -> bytes:
    return blake2b_32(bytes([0x01]) + left + right).digest()


def test_block_hash_matches_node_empty_block_golden() -> None:
    assert gen_block_hash_from_block(_empty_block()).hex() == (
        '249c70aa27fa8353d0373e13d6e9fc8ed4f50fc77d40bf901531fdbd4bc3884f'
    )


def test_merkle_root_of_no_leaves_is_zero() -> None:
    assert _merkle_root([]) == bytes(32)


def test_merkle_root_of_one_leaf_is_the_leaf() -> None:
    assert _merkle_root([_leaf(1)]) == _leaf(1)


def test_merkle_root_promotes_an_odd_last_leaf() -> None:
    a, b, c = _leaf(1), _leaf(2), _leaf(3)
    assert _merkle_root([a, b, c]) == _interior(_interior(a, b), c)


def test_merkle_root_pairs_four_leaves() -> None:
    a, b, c, d = _leaf(1), _leaf(2), _leaf(3), _leaf(4)
    assert _merkle_root([a, b, c, d]) == _interior(_interior(a, b), _interior(c, d))


def _with_bonds(order: list) -> BlockMessageProto:
    block = _empty_block()
    for validator, stake in order:
        bond = block.body.state.bonds.add()
        bond.validator = validator
        bond.stake = stake
    return block


def test_block_hash_ignores_bond_order() -> None:
    bonds = [(bytes([0x22]) * 33, 7), (bytes([0x11]) * 33, 5)]
    assert gen_block_hash_from_block(_with_bonds(bonds)) == gen_block_hash_from_block(
        _with_bonds(list(reversed(bonds)))
    )


def test_block_hash_ignores_rejected_deploy_and_applied_from_scope_order() -> None:
    sigs = [bytes([0x22]) * 64, bytes([0x11]) * 64]
    first, second = _empty_block(), _empty_block()
    for block, order in ((first, sigs), (second, list(reversed(sigs)))):
        for sig in order:
            block.body.rejectedDeploys.add().sig = sig
            block.body.appliedFromScope.append(sig)
    assert gen_block_hash_from_block(first) == gen_block_hash_from_block(second)


def test_block_hash_depends_on_leaky_bucket_vote_order() -> None:
    first, second = _empty_block(), _empty_block()
    for block, targets in ((first, (0x11, 0x22)), (second, (0x22, 0x11))):
        for target in targets:
            vote = block.body.leakyBucketVotes.add()
            vote.family = CLAIM_FAMILY_EJECT
            vote.target = bytes([target]) * 33
    assert gen_block_hash_from_block(first) != gen_block_hash_from_block(second)


def test_block_hash_commits_to_leaky_bucket_votes() -> None:
    voted = _empty_block()
    voted.body.leakyBucketVotes.add().family = CLAIM_FAMILY_EJECT
    assert gen_block_hash_from_block(voted) != gen_block_hash_from_block(_empty_block())