import hashlib
import time
from typing import Iterable, List

from .crypto import PrivateKey, PublicKey
from .pb.CasperMessage_pb2 import DeployDataProto
from .pb.RhoTypes_pb2 import CostSignature


def ordered_authority_presentations(
        presentations: Iterable[CostSignature],
) -> List[CostSignature]:
    def validate(signature: CostSignature) -> None:
        value = signature.WhichOneof("value")
        if value is None:
            raise ValueError("authority presentation is missing its signature")
        if value == "bound_level":
            raise ValueError(
                "authority presentation contains an unresolved bound signature"
            )
        if value == "unit" and not signature.unit:
            raise ValueError(
                "authority presentation contains a non-canonical unit signature"
            )
        if value == "compound":
            if len(signature.compound.elements) < 2:
                raise ValueError(
                    "authority presentation contains a malformed compound signature"
                )
            for element in signature.compound.elements:
                validate(element)

    ordered = []
    for presentation in presentations:
        clone = CostSignature()
        clone.CopyFrom(presentation)
        validate(clone)
        ordered.append(clone)

    ordered.sort(key=lambda item: item.SerializeToString(deterministic=True))
    for left, right in zip(ordered, ordered[1:]):
        if left == right:
            raise ValueError(
                "authority presentations must be strictly ordered and unique"
            )
    return ordered


def _gen_deploy_sig_content(data: DeployDataProto) -> bytes:
    signed_data = DeployDataProto()
    signed_data.term = data.term
    signed_data.timestamp = data.timestamp
    signed_data.validAfterBlockNumber = data.validAfterBlockNumber
    signed_data.shardId = data.shardId
    signed_data.expirationTimestamp = data.expirationTimestamp
    signed_data.authorityPresentations.extend(
        ordered_authority_presentations(data.authorityPresentations)
    )
    return signed_data.SerializeToString()


def sign_deploy_data(key: PrivateKey, data: DeployDataProto) -> bytes:
    return key.sign(_gen_deploy_sig_content(data))


def verify_deploy_data(key: PublicKey, sig: bytes, data: DeployDataProto) -> bool:
    return key.verify(sig, _gen_deploy_sig_content(data))


def blake2b_256_hex(data: bytes) -> str:
    """Compute Blake2b-256 hash and return hex string."""
    return hashlib.blake2b(data, digest_size=32).hexdigest()


def blake2b_256_hex_file(
        path: str,
        chunk_size: int = 1024 * 1024,
) -> str:
    """Compute Blake2b-256 hash of a file without loading it entirely.

    Reads the file in ``chunk_size`` increments and feeds each chunk
    into a streaming Blake2b hasher.

    Args:
        path: Filesystem path to the file.
        chunk_size: Read buffer size (default 1 MB).

    Returns:
        Hex-encoded Blake2b-256 digest.
    """
    h = hashlib.blake2b(digest_size=32)
    with open(path, 'rb') as f:
        while True:
            buf = f.read(chunk_size)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def create_deploy_data(
        key: PrivateKey,
        term: str,
        valid_after_block_no: int = -1,
        timestamp_millis: int = -1,
        shard_id: str = '',
        expiration_timestamp: int = 0,
        authority_presentations: Iterable[CostSignature] = (),
) -> DeployDataProto:
    if timestamp_millis < 0:
        timestamp_millis = int(time.time() * 1000)
    data = DeployDataProto(
        deployer=key.get_public_key().to_bytes(),
        term=term,
        validAfterBlockNumber=valid_after_block_no,
        timestamp=timestamp_millis,
        shardId=shard_id,
        expirationTimestamp=expiration_timestamp,
        sigAlgorithm='secp256k1',
        authorityPresentations=ordered_authority_presentations(
            authority_presentations
        ),
    )
    data.sig = sign_deploy_data(key, data)
    return data
