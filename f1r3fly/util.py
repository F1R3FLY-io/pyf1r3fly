import time
from typing import Optional, Sequence

from .crypto import PrivateKey, PublicKey
from .pb.CasperMessage_pb2 import (
    DeployDataProto, DeployParameter, RholangValue,
)

__all__ = [
    'sign_deploy_data',
    'verify_deploy_data',
    'create_deploy_data',
    'DeployParameter',
    'RholangValue',
]


def _gen_deploy_sig_content(data: DeployDataProto) -> bytes:
    signed_data = DeployDataProto()
    signed_data.term = data.term
    signed_data.timestamp = data.timestamp
    signed_data.phloLimit = data.phloLimit
    signed_data.phloPrice = data.phloPrice
    signed_data.validAfterBlockNumber = data.validAfterBlockNumber
    # Include parameters in signed content if present
    for param in data.parameters:
        signed_data.parameters.append(param)
    return signed_data.SerializeToString()


def sign_deploy_data(key: PrivateKey, data: DeployDataProto) -> bytes:
    return key.sign(_gen_deploy_sig_content(data))


def verify_deploy_data(key: PublicKey, sig: bytes, data: DeployDataProto) -> bool:
    return key.verify(sig, _gen_deploy_sig_content(data))


def create_deploy_data(
        key: PrivateKey,
        term: str,
        phlo_price: int,
        phlo_limit: int,
        valid_after_block_no: int = -1,
        timestamp_millis: int = -1,
        parameters: Optional[Sequence[DeployParameter]] = None,
) -> DeployDataProto:
    """Create a signed deploy data proto.

    Args:
        key: Private key used for signing.
        term: Rholang source code to deploy.
        phlo_price: Price per unit of phlo.
        phlo_limit: Maximum phlo to consume.
        valid_after_block_no: Block number after which deploy is valid.
        timestamp_millis: Timestamp in milliseconds. Defaults to current time.
        parameters: Optional typed parameters accessible via URI syntax
            (e.g., ``new myBytes(`rho:deploy:param:myBytes`) in { ... }``).

    Returns:
        Signed DeployDataProto ready for submission.
    """
    if timestamp_millis < 0:
        timestamp_millis = int(time.time() * 1000)
    data = DeployDataProto(
        deployer=key.get_public_key().to_bytes(),
        term=term,
        phloPrice=phlo_price,
        phloLimit=phlo_limit,
        validAfterBlockNumber=valid_after_block_no,
        timestamp=timestamp_millis,
        sigAlgorithm='secp256k1',
    )
    if parameters:
        for param in parameters:
            data.parameters.append(param)
    data.sig = sign_deploy_data(key, data)
    return data
