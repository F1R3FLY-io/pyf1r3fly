import hashlib
import random
from typing import Any, Iterable, Literal, Optional

import bitcoin.base58
from ecdsa import SigningKey, VerifyingKey
from ecdsa.curves import SECP256k1
from ecdsa.util import sigdecode_der, sigencode_der_canonize
from eth_hash.auto import keccak
from eth_keyfile import extract_key_from_keyfile
from google.protobuf.message import Message
from google.protobuf.wrappers_pb2 import Int32Value, StringValue

from f1r3fly.pb.CasperMessage_pb2 import BlockMessageProto

VAULTADDR_LENGTH = 40
BlockHashScheme = Literal['legacy', 'merkle-v1', 'merkle-v1-justifications']


def blake2b_32(data: bytes = b'') -> hashlib.blake2b:
    return hashlib.blake2b(data, digest_size=32)


def gen_deploys_hash_from_block(
        block: BlockMessageProto, *, scheme: BlockHashScheme = 'legacy',
) -> bytes:
    """Return the legacy deploy hash or the selected Merkle deploys root.

    Merkle profiles include user deploys followed by system deploys, in
    commit order. The legacy default preserves existing callers.
    """
    if scheme == 'legacy':
        hash_obj = b''.join(
            deploy.SerializeToString() for deploy in block.body.deploys
        )
        return blake2b_32(hash_obj).digest()
    _check_merkle_block(block, scheme)
    return _merkle_deploys_root(block)


def gen_block_hash_from_block(
        block: BlockMessageProto, *, scheme: BlockHashScheme = 'legacy',
) -> bytes:
    """Hash a canonical node-produced block using an explicit profile.

    ``legacy`` retains the original flat hash. ``merkle-v1`` matches the
    Rust recipe before node PR #790; ``merkle-v1-justifications`` includes
    its justification commitment. Header.version does not identify these
    profiles, so no automatic detection or fallback is performed.

    This computes a digest, not block validity or signature validity.
    """
    if scheme != 'legacy':
        _check_merkle_block(block, scheme)
        return _merkle_block_hash(block, scheme)
    signed_obj = b''.join([block.header.SerializeToString(),
                           block.body.SerializeToString(),
                           block.sender,
                           StringValue(value=block.sigAlgorithm).SerializeToString(),
                           Int32Value(value=block.seqNum).SerializeToString(),
                           StringValue(value=block.shardId).SerializeToString(),
                           block.extraBytes])
    return blake2b_32(signed_obj).digest()


def _check_merkle_block(
        block: BlockMessageProto, scheme: BlockHashScheme,
) -> None:
    if scheme not in ('merkle-v1', 'merkle-v1-justifications'):
        raise ValueError(f'Unsupported block hash scheme: {scheme}')
    if not (block.HasField('header') and block.HasField('body')
            and block.body.HasField('state')):
        raise ValueError('Merkle hashing requires a block header, body and state')
    known = BlockMessageProto()
    known.CopyFrom(block)
    known.DiscardUnknownFields()
    if known != block:
        raise ValueError('Unsupported protobuf fields in block; update bindings')


def _hash_leaf(domain: int, payload: bytes) -> bytes:
    return blake2b_32(bytes([domain]) + payload).digest()


def _hash_proto_leaf(domain: int, message: Message) -> bytes:
    return _hash_leaf(domain, message.SerializeToString(deterministic=True))


def _merkle_root(leaves: Iterable[bytes]) -> bytes:
    level = list(leaves)
    if not level:
        return bytes(32)
    while len(level) > 1:
        next_level = [
            _hash_leaf(0x01, level[i] + level[i + 1])
            for i in range(0, len(level) - 1, 2)
        ]
        if len(level) % 2:
            next_level.append(level[-1])
        level = next_level
    return level[0]


def _merkle_deploys_root(block: BlockMessageProto) -> bytes:
    return _merkle_root([
        *(_hash_proto_leaf(0x10, pd) for pd in block.body.deploys),
        *(_hash_proto_leaf(0x11, psd) for psd in block.body.systemDeploys),
    ])


def _merkle_block_hash(
        block: BlockMessageProto, scheme: BlockHashScheme,
) -> bytes:
    body = block.body
    state = body.state
    bonds_root = _merkle_root(
        _hash_leaf(0x15, bond.validator + bond.stake.to_bytes(8, 'little', signed=True))
        for bond in sorted(state.bonds, key=lambda bond: bond.validator)
    )
    justifications_root = b''
    if scheme == 'merkle-v1-justifications':
        justifications_root = _merkle_root(
            _hash_leaf(0x16, len(j.validator).to_bytes(4, 'little')
                       + j.validator + j.latestBlockHash)
            for j in block.justifications
        )
    rejected_root = _merkle_root(
        _hash_proto_leaf(0x13, rejected)
        for rejected in sorted(body.rejectedDeploys, key=lambda rejected: rejected.sig)
    )
    applied_root = _merkle_root(
        _hash_leaf(0x14, sig) for sig in sorted(body.appliedFromScope)
    )
    header_preimage = b''.join([
        b'\x01', block.header.SerializeToString(deterministic=True),
        state.preStateHash, state.postStateHash, bonds_root,
        justifications_root, state.blockNumber.to_bytes(8, 'little', signed=True),
        rejected_root, applied_root, body.mergeBase, body.extraBytes,
        block.sender, block.sigAlgorithm.encode('utf-8'),
        block.seqNum.to_bytes(4, 'little', signed=True),
        block.shardId.encode('utf-8'), block.extraBytes,
    ])
    return _hash_leaf(0x02, _hash_leaf(0x12, header_preimage)
                      + _merkle_deploys_root(block))


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
