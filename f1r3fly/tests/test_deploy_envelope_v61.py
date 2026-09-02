import json
from pathlib import Path

import pytest
from ecdsa.curves import SECP256k1
from ecdsa.util import sigdecode_der, sigencode_der

from f1r3fly.crypto import PrivateKey
from f1r3fly.pb.CasperMessage_pb2 import DeployDataProto, SignatureWitnessV61
from f1r3fly.pb.RhoTypes_pb2 import CostSignature
from f1r3fly.util import (
    DeploySigner, _gen_deploy_sig_content, assemble_single_signer_deploy_data,
    authorize_deploy_data, create_deploy_data, verify_deploy_data,
)


def test_protocol_v61_shared_vector() -> None:
    vector_path = (
        Path(__file__).parents[2]
        / "test-vectors"
        / "deploy-envelope-v6.1.json"
    )
    vector = json.loads(vector_path.read_text(encoding="utf-8"))
    case = vector["positive"]["threshold2Of3Selected0And2"]
    members = case["members"]
    signers = []
    for member in members:
        key = PrivateKey.from_hex(member["privateKeyHex"])
        signers.append(
            DeploySigner(
                public_key=key.get_public_key(),
                private_key=key if member["selected"] else None,
            )
        )
    data = DeployDataProto(
        term=case["term"],
        language=case["language"],
        timestamp=case["timestamp"],
        validAfterBlockNumber=case["validAfterBlockNumber"],
        shardId=case["shardId"],
        expirationTimestamp=case["expirationTimestamp"],
    )
    envelope = authorize_deploy_data(
        data,
        signers,
        threshold=case["threshold"],
    )
    assert _gen_deploy_sig_content(envelope).hex() == case["intentHex"]
    assert envelope.authorizationV61.presenceBitmap.hex() == case["presenceBitmapHex"]
    assert envelope.deployId.hex() == case["deployIdHex"]
    assert envelope.SerializeToString().hex() == case["protobufHex"]
    assert [
        witness.signature.hex()
        for witness in envelope.authorizationV61.witnesses
    ] == [
        member["signatureHex"]
        for member in members
        if member["selected"]
    ]


def test_protocol_v61_rejects_noncanonical_high_s_signature() -> None:
    key = PrivateKey.from_hex("01" * 32)
    data = DeployDataProto(
        term="Nil",
        language="rholang",
        timestamp=1,
        validAfterBlockNumber=1,
        shardId="root",
    )
    envelope = authorize_deploy_data(data, [DeploySigner.selected(key)])
    signature = envelope.authorizationV61.witnesses[0].signature
    r, s = sigdecode_der(signature, SECP256k1.order)
    high_s = sigencode_der(r, SECP256k1.order - s, SECP256k1.order)

    with pytest.raises(
        ValueError,
        match="protocol-v6 deploy signature failed verification",
    ):
        assemble_single_signer_deploy_data(
            data,
            key.get_public_key(),
            high_s,
        )

    assert not verify_deploy_data(key.get_public_key(), high_s, envelope)


def test_protocol_v61_constructor_rejects_invalid_policies() -> None:
    first = PrivateKey.from_hex("01" * 32)
    second = PrivateKey.from_hex("02" * 32)
    data = DeployDataProto(
        term="Nil",
        language="rholang",
        timestamp=1,
        validAfterBlockNumber=1,
        shardId="root",
    )

    with pytest.raises(ValueError, match="at least one signer"):
        authorize_deploy_data(data, [])
    with pytest.raises(ValueError, match="duplicate principal"):
        authorize_deploy_data(
            data,
            [DeploySigner.selected(first), DeploySigner.selected(first)],
        )
    with pytest.raises(ValueError, match="does not meet its threshold"):
        authorize_deploy_data(
            data,
            [
                DeploySigner.selected(first),
                DeploySigner(second.get_public_key()),
            ],
            threshold=2,
        )
    with pytest.raises(ValueError, match="1 <= threshold <= members"):
        authorize_deploy_data(
            data,
            [DeploySigner.selected(first)],
            threshold=2,
        )


def test_protocol_v61_constructor_clears_legacy_authorization() -> None:
    key = PrivateKey.from_hex("01" * 32)
    data = DeployDataProto(
        deployer=b"legacy-deployer",
        term="Nil",
        timestamp=1,
        validAfterBlockNumber=1,
        shardId="root",
        sig=b"legacy-signature",
        sigAlgorithm="secp256k1",
        cosigner_threshold=1,
    )
    envelope = authorize_deploy_data(data, [DeploySigner.selected(key)])

    assert envelope.deployer == b""
    assert envelope.sig == b""
    assert envelope.sigAlgorithm == ""
    assert list(envelope.cosigners) == []
    assert envelope.cosigner_threshold == 0
    assert not envelope.HasField("sig_algebra")


def test_protocol_v61_constructor_uses_the_root_shard_by_default() -> None:
    key = PrivateKey.from_hex("01" * 32)
    envelope = create_deploy_data(
        key,
        "Nil",
        valid_after_block_no=1,
        timestamp_millis=1,
    )

    assert envelope.shardId == "root"


def test_protocol_v61_verifier_checks_the_transmitted_envelope() -> None:
    key = PrivateKey.from_hex("01" * 32)
    envelope = authorize_deploy_data(
        DeployDataProto(
            term="Nil",
            language="rholang",
            timestamp=1,
            validAfterBlockNumber=1,
            shardId="root",
        ),
        [DeploySigner.selected(key)],
    )
    signature = envelope.authorizationV61.witnesses[0].signature

    mutations = []

    wrong_format = DeployDataProto()
    wrong_format.CopyFrom(envelope)
    wrong_format.authorizationV61.formatVersion += 1
    mutations.append(wrong_format)

    wrong_bitmap = DeployDataProto()
    wrong_bitmap.CopyFrom(envelope)
    wrong_bitmap.authorizationV61.presenceBitmap = b"\x03"
    mutations.append(wrong_bitmap)

    wrong_witness = DeployDataProto()
    wrong_witness.CopyFrom(envelope)
    wrong_witness.authorizationV61.witnesses[0].memberIndex = 1
    mutations.append(wrong_witness)

    extra_witness = DeployDataProto()
    extra_witness.CopyFrom(envelope)
    extra_witness.authorizationV61.witnesses.append(
        SignatureWitnessV61(memberIndex=0, signature=signature)
    )
    mutations.append(extra_witness)

    mixed_legacy = DeployDataProto()
    mixed_legacy.CopyFrom(envelope)
    mixed_legacy.sigAlgorithm = "secp256k1"
    mutations.append(mixed_legacy)

    for mutation in mutations:
        assert not verify_deploy_data(
            key.get_public_key(),
            signature,
            mutation,
        )


def test_protocol_v61_verifier_rejects_noncanonical_authority_order() -> None:
    key = PrivateKey.from_hex("01" * 32)
    envelope = authorize_deploy_data(
        DeployDataProto(
            term="Nil",
            language="rholang",
            timestamp=1,
            validAfterBlockNumber=1,
            shardId="root",
            authorityPresentations=[
                CostSignature(ground=b"a"),
                CostSignature(ground=b"b"),
            ],
        ),
        [DeploySigner.selected(key)],
    )
    signature = envelope.authorizationV61.witnesses[0].signature
    reordered = DeployDataProto()
    reordered.CopyFrom(envelope)
    reordered.authorityPresentations.reverse()

    assert not verify_deploy_data(
        key.get_public_key(),
        signature,
        reordered,
    )
