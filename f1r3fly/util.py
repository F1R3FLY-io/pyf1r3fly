import hashlib
import time
from typing import Optional

from .crypto import PrivateKey, PublicKey
from .pb.CasperMessage_pb2 import DeployDataProto
from .pb.DeployServiceV1_pb2 import FileUploadMetadata


def _gen_deploy_sig_content(data: DeployDataProto) -> bytes:
    signed_data = DeployDataProto()
    signed_data.term = data.term
    signed_data.timestamp = data.timestamp
    signed_data.phloLimit = data.phloLimit
    signed_data.phloPrice = data.phloPrice
    signed_data.validAfterBlockNumber = data.validAfterBlockNumber
    signed_data.shardId = data.shardId
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
        phlo_price: int,
        phlo_limit: int,
        valid_after_block_no: int = -1,
        timestamp_millis: int = -1,
        shard_id: str = '',
) -> DeployDataProto:
    if timestamp_millis < 0:
        timestamp_millis = int(time.time() * 1000)
    data = DeployDataProto(
        deployer=key.get_public_key().to_bytes(),
        term=term,
        phloPrice=phlo_price,
        phloLimit=phlo_limit,
        validAfterBlockNumber=valid_after_block_no,
        timestamp=timestamp_millis,
        shardId=shard_id,
        sigAlgorithm='secp256k1',
    )
    data.sig = sign_deploy_data(key, data)
    return data


def create_file_upload_metadata(
        key: PrivateKey,
        file_hash: str,
        file_size: int,
        file_name: str,
        phlo_price: int,
        phlo_limit: int,
        valid_after_block_no: int = -1,
        timestamp_millis: int = -1,
        shard_id: str = '',
        term: Optional[str] = None,
) -> FileUploadMetadata:
    """Build a signed FileUploadMetadata proto for the uploadFile RPC.

    The server maps metadata fields to a DeployDataProto and validates the
    client signature via DeployData.from(). We replicate that mapping here
    to produce a valid signature.

    If ``term`` is not provided, the standard file-registration Rholang
    term is generated automatically.
    """
    if timestamp_millis < 0:
        timestamp_millis = int(time.time() * 1000)

    if term is None:
        term = (
            f'new ret, file(`rho:io:file`) in {{'
            f' file!("register", "{file_hash}", {file_size}, "{file_name}", *ret)'
            f' }}'
        )

    # Build a DeployDataProto with the same fields the server will reconstruct
    # from the metadata (see SyntheticDeploy.metadataToDeployProto in Scala).
    deploy_data = DeployDataProto(
        deployer=key.get_public_key().to_bytes(),
        term=term,
        phloPrice=phlo_price,
        phloLimit=phlo_limit,
        validAfterBlockNumber=valid_after_block_no,
        timestamp=timestamp_millis,
        shardId=shard_id,
        sigAlgorithm='secp256k1',
    )
    deploy_data.sig = sign_deploy_data(key, deploy_data)

    return FileUploadMetadata(
        deployer=key.get_public_key().to_bytes(),
        timestamp=timestamp_millis,
        sig=deploy_data.sig,
        sigAlgorithm='secp256k1',
        phloPrice=phlo_price,
        phloLimit=phlo_limit,
        validAfterBlockNumber=valid_after_block_no,
        shardId=shard_id,
        fileName=file_name,
        fileSize=file_size,
        fileHash=file_hash,
        term=term,
    )

