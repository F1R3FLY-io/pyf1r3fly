import hashlib
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

from .crypto import PrivateKey, PublicKey
from .pb.CasperMessage_pb2 import (
    SIGNATURE_SCHEME_V61_SECP256K1, AllOfPolicyV61, AuthorizationPolicyV61,
    DeployAuthorizationV61, DeployDataProto, PrincipalV61, SignatureWitnessV61,
    ThresholdPolicyV61,
)
from .pb.RhoTypes_pb2 import CostSignature

_AUTHORIZATION_FORMAT_VERSION = 0x00060001
_ENVELOPE_PROTOCOL_VERSION = 6
_ENVELOPE_COMMITMENT_DOMAIN = b"f1r3fly:casper:deploy-envelope:v6.1"
_ENVELOPE_SIGNATURE_DOMAIN = b"f1r3fly:casper:deploy-envelope-signature:v6.1"


@dataclass(frozen=True)
class DeploySigner:
    public_key: PublicKey
    private_key: Optional[PrivateKey] = None

    @classmethod
    def selected(cls, private_key: PrivateKey) -> 'DeploySigner':
        return cls(private_key.get_public_key(), private_key)


def _lp64(value: bytes) -> bytes:
    return len(value).to_bytes(8, 'big') + value


def _canonical_cost_signature_bytes(signature: CostSignature) -> bytes:
    value = signature.WhichOneof("value")
    if value == "unit" and signature.unit:
        return b"\x00"
    if value == "ground":
        return b"\x01" + _lp64(signature.ground)
    if value == "quote":
        encoded = signature.quote.SerializeToString(deterministic=True)
        return b"\x02" + _lp64(encoded)
    if value == "name":
        encoded = signature.name.SerializeToString(deterministic=True)
        return b"\x03" + _lp64(encoded)
    if value == "compound":
        children = [
            _canonical_cost_signature_bytes(child)
            for child in signature.compound.elements
        ]
        if len(children) < 2:
            raise ValueError(
                "authority presentation contains a malformed compound signature"
            )
        if any(
            child.WhichOneof("value") in {"compound", "unit"}
            for child in signature.compound.elements
        ):
            raise ValueError(
                "authority compound must be flat and contain no unit"
            )
        if children != sorted(children):
            raise ValueError(
                "authority compound elements must be canonically ordered"
            )
        return (
            b"\x04"
            + len(children).to_bytes(4, 'big')
            + b"".join(_lp64(child) for child in children)
        )
    if value == "bound_level":
        raise ValueError(
            "authority presentation contains an unresolved bound signature"
        )
    if value == "unit":
        raise ValueError(
            "authority presentation contains a non-canonical unit signature"
        )
    raise ValueError("authority presentation is missing its signature")


def ordered_authority_presentations(
        presentations: Iterable[CostSignature],
) -> List[CostSignature]:
    ordered = []
    for presentation in presentations:
        clone = CostSignature()
        clone.CopyFrom(presentation)
        _canonical_cost_signature_bytes(clone)
        ordered.append(clone)

    ordered.sort(key=_canonical_cost_signature_bytes)
    for left, right in zip(ordered, ordered[1:]):
        if left == right:
            raise ValueError(
                "authority presentations must be strictly ordered and unique"
            )
    return ordered


def _gen_deploy_sig_content(data: DeployDataProto) -> bytes:
    if data.language != "rholang":
        raise ValueError("protocol-v6 deploy language must be rholang")
    if data.timestamp < 0:
        raise ValueError("protocol-v6 deploy timestamp must be nonnegative")
    if data.validAfterBlockNumber < 0:
        raise ValueError("protocol-v6 valid-after block must be nonnegative")
    if not data.shardId:
        raise ValueError("protocol-v6 shard ID must be nonempty")
    if data.expirationTimestamp < 0:
        raise ValueError("protocol-v6 expiration timestamp must be nonnegative")
    supplied_presentations = list(data.authorityPresentations)
    presentations = ordered_authority_presentations(supplied_presentations)
    if supplied_presentations != presentations:
        raise ValueError(
            "authority presentations must be strictly ordered and unique"
        )
    intent = bytearray((1).to_bytes(2, 'big'))
    intent.append(1)
    intent.extend(_lp64(data.term.encode('utf-8')))
    intent.extend(data.timestamp.to_bytes(8, 'big'))
    intent.extend(data.validAfterBlockNumber.to_bytes(8, 'big'))
    intent.extend(_lp64(data.shardId.encode('utf-8')))
    if data.expirationTimestamp == 0:
        intent.append(0)
    else:
        intent.append(1)
        intent.extend(data.expirationTimestamp.to_bytes(8, 'big'))
    intent.extend(len(presentations).to_bytes(4, 'big'))
    for presentation in presentations:
        intent.extend(_lp64(_canonical_cost_signature_bytes(presentation)))
    return bytes(intent)


def _principal_bytes(public_key: PublicKey) -> bytes:
    encoded = public_key.to_bytes()
    if len(encoded) != 65 or encoded[0] != 4:
        raise ValueError(
            "protocol-v6 secp256k1 public keys must use uncompressed SEC1"
        )
    return (
        SIGNATURE_SCHEME_V61_SECP256K1.to_bytes(2, 'big')
        + len(encoded).to_bytes(4, 'big')
        + encoded
    )


def _envelope_commitment(
        data: DeployDataProto,
        signers: Sequence[DeploySigner],
        threshold: int,
        bitmap: bytes,
) -> bytes:
    total = len(signers)
    policy = bytearray()
    if threshold == total:
        policy.append(1)
        policy.extend(total.to_bytes(4, 'big'))
    else:
        policy.append(2)
        policy.extend(threshold.to_bytes(4, 'big'))
        policy.extend(total.to_bytes(4, 'big'))
    for signer in signers:
        policy.extend(_principal_bytes(signer.public_key))
    preimage = bytearray(_lp64(_ENVELOPE_COMMITMENT_DOMAIN))
    preimage.extend(_ENVELOPE_PROTOCOL_VERSION.to_bytes(2, 'big'))
    preimage.extend(_lp64(_gen_deploy_sig_content(data)))
    preimage.extend(_lp64(bytes(policy)))
    preimage.extend(len(bitmap).to_bytes(4, 'big'))
    preimage.extend(bitmap)
    return hashlib.blake2b(preimage, digest_size=32).digest()


def _envelope_signing_hash(deploy_id: bytes) -> bytes:
    preimage = bytearray(_lp64(_ENVELOPE_SIGNATURE_DOMAIN))
    preimage.extend(_ENVELOPE_PROTOCOL_VERSION.to_bytes(2, 'big'))
    preimage.extend(
        SIGNATURE_SCHEME_V61_SECP256K1.to_bytes(2, 'big')
    )
    preimage.extend(deploy_id)
    return hashlib.blake2b(preimage, digest_size=32).digest()


def authorize_deploy_data(
        data: DeployDataProto,
        signers: Sequence[DeploySigner],
        threshold: Optional[int] = None,
) -> DeployDataProto:
    if not signers:
        raise ValueError("protocol-v6 authorization requires at least one signer")
    canonical = sorted(signers, key=lambda signer: _principal_bytes(signer.public_key))
    principals = [_principal_bytes(signer.public_key) for signer in canonical]
    if any(left == right for left, right in zip(principals, principals[1:])):
        raise ValueError("protocol-v6 authorization contains a duplicate principal")
    total = len(canonical)
    required = total if threshold is None else threshold
    if required < 1 or required > total:
        raise ValueError("protocol-v6 threshold must satisfy 1 <= threshold <= members")
    selected = [signer.private_key is not None for signer in canonical]
    if sum(selected) < required or (required == total and not all(selected)):
        raise ValueError("protocol-v6 authorization does not meet its threshold")
    for signer in canonical:
        if (
            signer.private_key is not None
            and signer.private_key.get_public_key() != signer.public_key
        ):
            raise ValueError("protocol-v6 signer private/public key mismatch")
    bitmap = bytearray((total + 7) // 8)
    for index, present in enumerate(selected):
        if present:
            bitmap[index // 8] |= 1 << (index % 8)
    output = DeployDataProto()
    output.CopyFrom(data)
    output.deployer = b""
    output.sig = b""
    output.sigAlgorithm = ""
    del output.cosigners[:]
    output.cosigner_threshold = 0
    output.ClearField("sig_algebra")
    output.language = "rholang"
    presentations = ordered_authority_presentations(
        output.authorityPresentations
    )
    del output.authorityPresentations[:]
    output.authorityPresentations.extend(presentations)
    deploy_id = _envelope_commitment(output, canonical, required, bytes(bitmap))
    signing_hash = _envelope_signing_hash(deploy_id)
    principal_messages = [
        PrincipalV61(
            scheme=SIGNATURE_SCHEME_V61_SECP256K1,
            publicKey=signer.public_key.to_bytes(),
        )
        for signer in canonical
    ]
    if required == total:
        policy = AuthorizationPolicyV61(
            allOf=AllOfPolicyV61(members=principal_messages)
        )
    else:
        policy = AuthorizationPolicyV61(
            threshold=ThresholdPolicyV61(
                minimum=required,
                members=principal_messages,
            )
        )
    witnesses = []
    for index, signer in enumerate(canonical):
        if signer.private_key is None:
            continue
        witnesses.append(
            SignatureWitnessV61(
                memberIndex=index,
                signature=signer.private_key.sign_digest_deterministic(
                    signing_hash
                ),
            )
        )
    output.deployId = deploy_id
    output.authorizationV61.CopyFrom(
        DeployAuthorizationV61(
            formatVersion=_AUTHORIZATION_FORMAT_VERSION,
            policy=policy,
            presenceBitmap=bytes(bitmap),
            witnesses=witnesses,
        )
    )
    return output


def assemble_single_signer_deploy_data(
        data: DeployDataProto,
        public_key: PublicKey,
        signature: bytes,
) -> DeployDataProto:
    signer = DeploySigner(public_key)
    output = DeployDataProto()
    output.CopyFrom(data)
    output.deployer = b""
    output.sig = b""
    output.sigAlgorithm = ""
    del output.cosigners[:]
    output.cosigner_threshold = 0
    output.ClearField("sig_algebra")
    output.language = "rholang"
    presentations = ordered_authority_presentations(
        output.authorityPresentations
    )
    del output.authorityPresentations[:]
    output.authorityPresentations.extend(presentations)
    deploy_id = _envelope_commitment(output, [signer], 1, b"\x01")
    if not public_key.verify_canonical_digest(
        signature,
        _envelope_signing_hash(deploy_id),
    ):
        raise ValueError("protocol-v6 deploy signature failed verification")
    output.deployId = deploy_id
    output.authorizationV61.CopyFrom(
        DeployAuthorizationV61(
            formatVersion=_AUTHORIZATION_FORMAT_VERSION,
            policy=AuthorizationPolicyV61(
                allOf=AllOfPolicyV61(
                    members=[
                        PrincipalV61(
                            scheme=SIGNATURE_SCHEME_V61_SECP256K1,
                            publicKey=public_key.to_bytes(),
                        )
                    ]
                )
            ),
            presenceBitmap=b"\x01",
            witnesses=[
                SignatureWitnessV61(
                    memberIndex=0,
                    signature=signature,
                )
            ],
        )
    )
    return output


def sign_deploy_data(key: PrivateKey, data: DeployDataProto) -> bytes:
    authorized = authorize_deploy_data(data, [DeploySigner.selected(key)])
    data.CopyFrom(authorized)
    return data.authorizationV61.witnesses[0].signature


def verify_deploy_data(key: PublicKey, sig: bytes, data: DeployDataProto) -> bool:
    try:
        if (
            len(data.deployId) != 32
            or data.deployer
            or data.sig
            or data.sigAlgorithm
            or data.cosigners
            or data.cosigner_threshold != 0
            or data.HasField("sig_algebra")
            or not data.HasField("authorizationV61")
        ):
            return False
        authorization = data.authorizationV61
        if (
            authorization.formatVersion != _AUTHORIZATION_FORMAT_VERSION
            or authorization.policy.WhichOneof("policy") != "allOf"
            or len(authorization.policy.allOf.members) != 1
            or authorization.presenceBitmap != b"\x01"
            or len(authorization.witnesses) != 1
        ):
            return False
        member = authorization.policy.allOf.members[0]
        witness = authorization.witnesses[0]
        if (
            member.scheme != SIGNATURE_SCHEME_V61_SECP256K1
            or member.publicKey != key.to_bytes()
            or witness.memberIndex != 0
            or witness.signature != sig
        ):
            return False
        expected_deploy_id = _envelope_commitment(
            data,
            [DeploySigner(key)],
            1,
            b"\x01",
        )
    except (OverflowError, ValueError):
        return False
    if data.deployId != expected_deploy_id:
        return False
    return key.verify_canonical_digest(
        sig,
        _envelope_signing_hash(data.deployId),
    )


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
        shard_id: str = 'root',
        expiration_timestamp: int = 0,
        authority_presentations: Iterable[CostSignature] = (),
) -> DeployDataProto:
    if timestamp_millis < 0:
        timestamp_millis = int(time.time() * 1000)
    data = DeployDataProto(
        term=term,
        validAfterBlockNumber=valid_after_block_no,
        timestamp=timestamp_millis,
        shardId=shard_id,
        expirationTimestamp=expiration_timestamp,
        language='rholang',
        authorityPresentations=ordered_authority_presentations(
            authority_presentations
        ),
    )
    return authorize_deploy_data(data, [DeploySigner.selected(key)])
