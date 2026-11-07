import hashlib
import random
import struct
from typing import Any, Optional, Sequence

import bitcoin.base58
from ecdsa import SigningKey, VerifyingKey
from ecdsa.curves import SECP256k1
from ecdsa.util import sigdecode_der, sigencode_der_canonize
from eth_hash.auto import keccak
from eth_keyfile import extract_key_from_keyfile

from f1r3fly.pb.CasperMessage_pb2 import (
    BlockCommitmentProto, BlockMessageProto,
)

VAULTADDR_LENGTH = 40


def blake2b_32(data: bytes = b'') -> hashlib.blake2b:
    return hashlib.blake2b(data, digest_size=32)


BLOCK_HASH_SCHEME_VERSION = 0x02

_INTERIOR_DOMAIN = 0x01
_TOP_DOMAIN = 0x02
_LEAF_PROCESSED_DEPLOY = 0x10
_LEAF_PROCESSED_SYSTEM_DEPLOY = 0x11
_LEAF_HEADER = 0x12
_LEAF_REJECTED_DEPLOY = 0x13
_LEAF_APPLIED_FROM_SCOPE_SIG = 0x14
_LEAF_BOND = 0x15
_LEAF_JUSTIFICATION = 0x16
_LEAF_LEAKY_BUCKET_VOTE = 0x17

_EMPTY_ROOT = bytes(32)


def _hash_leaf(domain: int, data: bytes) -> bytes:
    return blake2b_32(bytes([domain]) + data).digest()


def _merkle_root(leaves: Sequence[bytes]) -> bytes:
    if not leaves:
        return _EMPTY_ROOT
    level = list(leaves)
    while len(level) > 1:
        parents = [
            blake2b_32(bytes([_INTERIOR_DOMAIN]) + level[i] + level[i + 1]).digest()
            for i in range(0, len(level) - 1, 2)
        ]
        if len(level) % 2:
            parents.append(level[-1])
        level = parents
    return level[0]


def _deploys_root(block: BlockMessageProto) -> bytes:
    leaves = [
        _hash_leaf(_LEAF_PROCESSED_DEPLOY, deploy.SerializeToString())
        for deploy in block.body.deploys
    ] + [
        _hash_leaf(_LEAF_PROCESSED_SYSTEM_DEPLOY, deploy.SerializeToString())
        for deploy in block.body.systemDeploys
    ]
    return _merkle_root(leaves)


def block_commitment(block: BlockMessageProto) -> BlockCommitmentProto:
    """What the block's hash and signature commit to."""
    body = block.body
    state = body.state
    commitment = BlockCommitmentProto(
        preStateHash=state.preStateHash,
        postStateHash=state.postStateHash,
        bondsRoot=_merkle_root([
            _hash_leaf(_LEAF_BOND, bond.validator + struct.pack('<q', bond.stake))
            for bond in sorted(state.bonds, key=lambda bond: bond.validator)
        ]),
        justificationsRoot=_merkle_root([
            _hash_leaf(
                _LEAF_JUSTIFICATION,
                struct.pack('<I', len(j.validator)) + j.validator + j.latestBlockHash,
            )
            for j in block.justifications
        ]),
        blockNumber=state.blockNumber,
        rejectedRoot=_merkle_root([
            _hash_leaf(_LEAF_REJECTED_DEPLOY, rejected.SerializeToString())
            for rejected in sorted(body.rejectedDeploys, key=lambda rejected: rejected.sig)
        ]),
        appliedFromScopeRoot=_merkle_root([
            _hash_leaf(_LEAF_APPLIED_FROM_SCOPE_SIG, sig)
            for sig in sorted(body.appliedFromScope)
        ]),
        leakyBucketVotesRoot=_merkle_root([
            _hash_leaf(_LEAF_LEAKY_BUCKET_VOTE, vote.SerializeToString())
            for vote in body.leakyBucketVotes
        ]),
        mergeBase=body.mergeBase,
        bodyExtraBytes=body.extraBytes,
        sender=block.sender,
        sigAlgorithm=block.sigAlgorithm,
        seqNum=block.seqNum,
        shardId=block.shardId,
        extraBytes=block.extraBytes,
        deploysRoot=_deploys_root(block),
        sig=block.sig,
    )
    commitment.header.CopyFrom(block.header)
    return commitment


def _field(data: bytes) -> bytes:
    return struct.pack('<I', len(data)) + data


def _root(name: str, data: bytes) -> bytes:
    if len(data) != 32:
        raise ValueError(f'{name} is {len(data)} bytes, not 32')
    return data


def _header_preimage(commitment: BlockCommitmentProto) -> bytes:
    return b''.join([
        bytes([BLOCK_HASH_SCHEME_VERSION]),
        _field(commitment.header.SerializeToString()),
        _field(commitment.preStateHash),
        _field(commitment.postStateHash),
        _root('bondsRoot', commitment.bondsRoot),
        _root('justificationsRoot', commitment.justificationsRoot),
        struct.pack('<q', commitment.blockNumber),
        _root('rejectedRoot', commitment.rejectedRoot),
        _root('appliedFromScopeRoot', commitment.appliedFromScopeRoot),
        _root('leakyBucketVotesRoot', commitment.leakyBucketVotesRoot),
        _field(commitment.mergeBase),
        _field(commitment.bodyExtraBytes),
        _field(commitment.sender),
        _field(commitment.sigAlgorithm.encode()),
        struct.pack('<i', commitment.seqNum),
        _field(commitment.shardId.encode()),
        _field(commitment.extraBytes),
    ])


def commitment_hash(commitment: BlockCommitmentProto) -> bytes:
    """The node's Merkle block hash (scheme v1) of a block commitment."""
    header_leaf = _hash_leaf(_LEAF_HEADER, _header_preimage(commitment))
    deploys_root = _root('deploysRoot', commitment.deploysRoot)
    return blake2b_32(bytes([_TOP_DOMAIN]) + header_leaf + deploys_root).digest()


def gen_block_hash_from_block(block: BlockMessageProto) -> bytes:
    return commitment_hash(block_commitment(block))


def verify_vault_address(vault_address: str) -> bool:
    addrBytes = bitcoin.base58.decode(vault_address)
    payload = addrBytes[:-4]
    checksum = addrBytes[-4:]
    checksumCalc = blake2b_32(payload).digest()
    cal = checksumCalc[:4]
    return checksum == cal and len(addrBytes) == VAULTADDR_LENGTH


def generate_vault_addr_from_eth(eth_address: str) -> str:
    prefix = b'\0\0\0\0'
    pub_key_hash = keccak(bytes.fromhex(eth_address))
    payload = prefix + pub_key_hash
    payload_chksum = blake2b_32(payload).digest()[:4]
    addr_bytes = payload + payload_chksum
    return bitcoin.base58.encode(addr_bytes)


class PublicKey:

    @classmethod
    def from_bytes(cls, b: bytes) -> 'PublicKey':
        if len(b) == 65:
            b = b[1:]  # remove 0x04 prefix
        return cls(VerifyingKey.from_string(b, curve=SECP256k1))

    @classmethod
    def from_hex(cls, s: str) -> 'PublicKey':
        return cls.from_bytes(bytes.fromhex(s))

    def __init__(self, _pub_key: VerifyingKey):
        self._pub_key = _pub_key

    def verify(self, signature: bytes, data: bytes) -> bool:
        return self._pub_key.verify(
            signature, data, hashfunc=blake2b_32, sigdecode=sigdecode_der
        )

    def verify_block_hash(self, signature: bytes, block_hash: bytes) -> bool:
        return self._pub_key.verify_digest(signature, block_hash, sigdecode=sigdecode_der)

    def to_hex(self, lower: bool = True) -> str:
        return self.to_bytes().hex().lower() if lower else self.to_bytes().hex()

    def to_bytes(self) -> bytes:
        return b'\x04' + self._pub_key.to_string()

    def get_vault_address(self) -> str:
        eth_address = self.get_eth_address()
        return generate_vault_addr_from_eth(eth_address)

    def get_eth_address(self) -> str:
        return keccak(self.to_bytes()[1:]).hex()[-40:]

    def __hash__(self) -> int:
        return hash(self.to_bytes())

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.to_bytes() == other.to_bytes()


class PrivateKey:
    @classmethod
    def from_eth_keyfile(cls, path: str, password: Optional[str] = None) -> 'PrivateKey':
        # eth-keyfile expects password as bytes
        password_bytes = password.encode('utf-8') if password else b''
        key_bytes = extract_key_from_keyfile(path, password_bytes)
        return cls.from_bytes(key_bytes)

    @classmethod
    def generate(cls) -> 'PrivateKey':
        return cls(SigningKey.generate(curve=SECP256k1))

    @classmethod
    def from_bytes(cls, b: bytes) -> 'PrivateKey':
        return cls(SigningKey.from_string(b, curve=SECP256k1))

    @classmethod
    def from_hex(cls, s: str) -> 'PrivateKey':
        return cls.from_bytes(bytes.fromhex(s))

    @classmethod
    def from_seed(cls, seed: int) -> 'PrivateKey':
        rand = random.Random(seed)
        key_bytes = rand.getrandbits(32 * 8).to_bytes(32, 'big')
        return cls.from_bytes(key_bytes)

    def __init__(self, _key: SigningKey):
        self._key = _key

    def sign(self, data: bytes) -> bytes:
        return self._key.sign(
            data, hashfunc=blake2b_32, sigencode=sigencode_der_canonize
        )

    def sign_deterministic(self, data: bytes, extra_entropy: bytes = b'') -> bytes:
        return self._key.sign_deterministic(data, hashfunc=blake2b_32, sigencode=sigencode_der_canonize,
                                            extra_entropy=extra_entropy)

    def sign_block_hash(self, block_hash: bytes) -> bytes:
        return self._key.sign_digest(block_hash, sigencode=sigencode_der_canonize)

    def to_bytes(self) -> bytes:
        return self._key.to_string()

    def to_hex(self, lower: bool = True) -> str:
        return self.to_bytes().hex().lower() if lower else self.to_bytes().hex()

    def get_public_key(self) -> PublicKey:
        return PublicKey(self._key.get_verifying_key())

    def __hash__(self) -> int:
        return hash(self.to_bytes())

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__) and self.to_bytes() == other.to_bytes()
