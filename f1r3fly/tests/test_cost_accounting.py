import hashlib
from unittest.mock import Mock, call

import pytest

from f1r3fly.cost_accounting import (
    AUTHORITY_ACCOUNTING_PROTOCOL_VERSION, BYTE_COST_SCHEDULE_DIGEST,
    BYTE_COST_SCHEDULE_VERSION, CAPABILITIES_REGISTRY_URI, CapabilityAPI,
    CapabilityRegistration, CostAuthorityEvidence,
    CostAuthorityFundingCertificate, ExchangeAPI, FundingSlotAPI,
    FundingSlotGrant, ground_authority_presentation,
)
from f1r3fly.crypto import PrivateKey
from f1r3fly.pb.CasperMessage_pb2 import (
    COST_AUTHORITY_BYTE_EVENT_KIND_COMM, DEPLOY_ADMISSION_STATUS_EXECUTED,
    DEPLOY_ADMISSION_STATUS_REJECTED, CostAuthorityByteEventProto,
    CostAuthorityEventProto, CostAuthorityFundingCertificateProto,
    CostAuthorityPhysicalEventDrawProto, CostAuthorityResourceProto,
    CostAuthorityStackReservationProto, CostAuthorityWitnessProto,
    ProcessedDeployProto,
)
from f1r3fly.pb.DeployServiceCommon_pb2 import DeployInfo
from f1r3fly.pb.RhoTypes_pb2 import CostAuthority, CostRegion, CostSignature
from f1r3fly.vault import VaultAPI

SLOT = b"s" * 32
GATEWAY = b"g" * 32
STACK = b"k" * 32
EVENT = b"e" * 32
PRE = b"p" * 32
POST = b"o" * 32
PROGRAM = b"m" * 32
RESERVATION = b"r" * 32
BYTE_EVENT = b"b" * 32
GATEWAY_PUBLIC_KEY = PrivateKey.from_seed(7).get_public_key().to_bytes()


def authority() -> CostAuthority:
    return CostAuthority(
        regions=[
            CostRegion(
                instance_id=b"i" * 32,
                signature=CostSignature(ground=b"slot"),
            )
        ]
    )


def valid_processed_deploy() -> ProcessedDeployProto:
    certificate_proto = CostAuthorityFundingCertificateProto(
        protocolVersion=AUTHORITY_ACCOUNTING_PROTOCOL_VERSION,
        programHash=PROGRAM,
        preStateRoot=PRE,
        reservationId=RESERVATION,
        demand=[CostAuthorityResourceProto(key=SLOT, amount=2)],
        allocation=[CostAuthorityResourceProto(key=SLOT, amount=2)],
        feeAllocation=[CostAuthorityResourceProto(key=GATEWAY, amount=1)],
        feeRecipient=b"proposer",
        byteCostScheduleVersion=BYTE_COST_SCHEDULE_VERSION,
        byteCostScheduleDigest=BYTE_COST_SCHEDULE_DIGEST,
        stackReservations=[
            CostAuthorityStackReservationProto(stackId=STACK, popCount=1)
        ],
    )
    certificate = CostAuthorityFundingCertificate.from_proto(certificate_proto)
    witness = CostAuthorityWitnessProto(
        protocolVersion=AUTHORITY_ACCOUNTING_PROTOCOL_VERSION,
        certificateId=certificate.certificate_id(),
        preStateRoot=PRE,
        postStateRoot=POST,
        events=[
            CostAuthorityEventProto(
                eventId=EVENT,
                debit=[CostAuthorityResourceProto(key=SLOT, amount=1)],
                authority=authority(),
            )
        ],
        realized=[CostAuthorityResourceProto(key=SLOT, amount=1)],
        physicalDraws=[
            CostAuthorityPhysicalEventDrawProto(
                eventId=EVENT,
                stackIds=[STACK],
            )
        ],
        byteCostScheduleVersion=BYTE_COST_SCHEDULE_VERSION,
        byteCostScheduleDigest=BYTE_COST_SCHEDULE_DIGEST,
    )
    return ProcessedDeployProto(
        preStateHash=PRE,
        postStateHash=POST,
        admissionStatus=DEPLOY_ADMISSION_STATUS_EXECUTED,
        authorityFundingCertificate=certificate_proto,
        authorityCostWitness=witness,
    )


def valid_byte_processed_deploy() -> ProcessedDeployProto:
    processed = valid_processed_deploy()
    certificate = processed.authorityFundingCertificate
    certificate.byteCostBound = 3
    certificate.byteAllocation.add(key=SLOT, amount=3)
    witness = processed.authorityCostWitness
    witness.certificateId = CostAuthorityFundingCertificate.from_proto(
        certificate
    ).certificate_id()
    witness.byteEvents.add(
        eventId=BYTE_EVENT,
        kind=COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
        authority=authority(),
        amount=3,
    )
    witness.byteCost = 3
    witness.byteSettlement.add(key=SLOT, amount=3)
    return processed


def test_public_deploy_info_preserves_validated_cost_evidence() -> None:
    processed = valid_byte_processed_deploy()
    deploy = DeployInfo(
        preStateHash=processed.preStateHash,
        postStateHash=processed.postStateHash,
        admissionStatus=processed.admissionStatus,
        authorityFundingCertificate=processed.authorityFundingCertificate,
        authorityCostWitness=processed.authorityCostWitness,
    )

    evidence = CostAuthorityEvidence.from_processed_deploy(deploy)

    assert evidence.certificate.protocol_version == (
        AUTHORITY_ACCOUNTING_PROTOCOL_VERSION
    )
    assert evidence.certificate.byte_cost_schedule_version == (
        BYTE_COST_SCHEDULE_VERSION
    )
    assert evidence.certificate.byte_cost_schedule_digest == (
        BYTE_COST_SCHEDULE_DIGEST
    )
    assert evidence.pre_state_root == PRE
    assert evidence.post_state_root == POST
    assert evidence.byte_cost == 3
    assert evidence.byte_settlement == {SLOT: 3}


def test_funding_slot_grant_renders_native_lollipop_workflow() -> None:
    grant = FundingSlotGrant(
        trigger_channel='agent-"trigger',
        slot_address_channel="agent-slot-address",
        completion_channel="agent-complete",
        gateway_public_key=GATEWAY_PUBLIC_KEY,
        outer_address_channel="agent-outer-address",
    )
    source = grant.install_term('@"agent-ran"!(request)')

    assert 'for (@request, deployerId <= @"agent-\\"trigger")' in source
    assert 'DeployerIdOps!("pubKeyBytes", *deployerId, *publicKeyCh)' in source
    assert f'"{GATEWAY_PUBLIC_KEY.hex()}".hexToBytes()' in source
    assert "if (publicKey ==" in source
    assert "entry!(request)" in source
    assert "{% for (@accepted <- entry)" in source
    assert "%}[ entry -o slot ]" in source
    assert "entry :: ()" in source
    assert source.index("if (publicKey ==") < source.index("%}[ entry -o slot ]")
    assert source.index("%}[ entry -o slot ]") < source.index("entry!(request)")
    assert 'VaultAddress!("fromUnforgeable", *entry, *entryAddressCh)' in source
    assert '@"agent-outer-address"!!(entryAddress)' in source
    assert 'VaultAddress!("fromUnforgeable", *slot, *slotAddressCh)' in source
    assert '@"agent-slot-address"!!(slotAddress)' in source
    assert (
        grant.trigger_term("7")
        == """new deployerId(`rho:system:deployerId`) in {
  @"agent-\\"trigger"!(7, *deployerId)
}"""
    )


def test_vault_batch_transfer_renders_one_native_atomic_operation() -> None:
    client = Mock()
    client.deploy_with_vabn_filled.return_value = "deploy-id"
    key = Mock()

    deploy_id = VaultAPI(client).transfer_batch_ensure(
        "source",
        (("outer", 11), ("slot", 29)),
        key,
    )

    assert deploy_id == "deploy-id"
    source = client.deploy_with_vabn_filled.call_args.args[1]
    assert '@SystemVault!("findOrCreate", "source", *vaultCh)' in source
    assert '@vault!("transferBatch", [("outer", 11), ("slot", 29)]' in source
    assert source.count('"transferBatch"') == 1


@pytest.mark.parametrize(
    "source, transfers, message",
    (
        ("", (("outer", 1),), "from_addr must not be empty"),
        ("source", (), "transfers must not be empty"),
        ("source", (("", 1),), "addresses must not be empty"),
        ("source", (("same", 1), ("same", 2)), "addresses must be distinct"),
        ("source", (("outer", 0),), "positive signed 64-bit"),
        ("source", (("outer", 1 << 63),), "positive signed 64-bit"),
        (
            "source",
            (("outer", (1 << 63) - 1), ("slot", 1)),
            "total exceeds the signed 64-bit",
        ),
    ),
)
def test_vault_batch_transfer_rejects_ambiguous_requests(
    source: str,
    transfers: tuple[tuple[str, int], ...],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        VaultAPI(Mock()).transfer_batch_ensure(source, transfers, Mock())


def test_funding_slot_api_funds_both_located_purses_atomically() -> None:
    client = Mock()
    client.deploy_with_vabn_filled.return_value = "funding-deploy"
    grant = FundingSlotGrant(
        "trigger",
        "slot-address-channel",
        "completion",
        GATEWAY_PUBLIC_KEY,
        "outer-address-channel",
    )

    deploy_id = FundingSlotAPI(client).fund(
        grant,
        "source-address",
        13,
        17,
        Mock(),
        resolved_addresses=("outer-address", "slot-address"),
    )

    assert deploy_id == "funding-deploy"
    source = client.deploy_with_vabn_filled.call_args.args[1]
    assert '[("outer-address", 13), ("slot-address", 17)]' in source
    assert source.count('"transferBatch"') == 1
    client.read_channel.assert_not_called()


def test_funding_slot_api_can_resolve_addresses_with_a_readonly_client() -> None:
    client = Mock()
    client.read_channel.side_effect = ("outer-address", "slot-address")
    client.deploy_with_vabn_filled.return_value = "funding-deploy"
    grant = FundingSlotGrant(
        "trigger",
        "slot-address-channel",
        "completion",
        GATEWAY_PUBLIC_KEY,
        "outer-address-channel",
    )

    deploy_id = FundingSlotAPI(client).fund(
        grant,
        "source-address",
        13,
        17,
        Mock(),
        block_hash="finalized-block",
    )

    assert deploy_id == "funding-deploy"
    assert client.read_channel.call_args_list == [
        call("outer-address-channel", "finalized-block"),
        call("slot-address-channel", "finalized-block"),
    ]


def test_funding_slot_api_rejects_nonpositive_component_funding() -> None:
    grant = FundingSlotGrant(
        "trigger",
        "slot-address-channel",
        "completion",
        GATEWAY_PUBLIC_KEY,
    )
    with pytest.raises(ValueError, match="funding amounts must be positive"):
        FundingSlotAPI(Mock()).fund(grant, "source", 0, 1, Mock())


def test_funding_slot_grant_rejects_empty_source_and_channel() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        FundingSlotGrant("", "address", "done", GATEWAY_PUBLIC_KEY).install_term("Nil")
    with pytest.raises(ValueError, match="must not be empty"):
        FundingSlotGrant(
            "trigger", "address", "done", GATEWAY_PUBLIC_KEY
        ).install_term("  ")
    with pytest.raises(ValueError, match="65-byte uncompressed"):
        FundingSlotGrant("trigger", "address", "done", b"short")
    with pytest.raises(ValueError, match="valid secp256k1 point"):
        FundingSlotGrant("trigger", "address", "done", b"\x04" + b"g" * 64)
    with pytest.raises(ValueError, match="public channels must be distinct"):
        FundingSlotGrant("trigger", "trigger", "done", GATEWAY_PUBLIC_KEY)


def test_exchange_renders_two_sided_conserving_swap() -> None:
    source = ExchangeAPI.integer_carrier_swap_term(7, 11)

    assert "rl!(`rho:lang:exchange`, *exchangeCh)" in source
    assert "clientCarrier!(7)" in source
    assert "vaultCarrier!(11)" in source
    assert '@Exchange!("swap", *clientCarrier, *vaultCarrier, *swapCh)' in source
    assert "deployId!((clientAfter, vaultAfter))" in source


def test_capability_registration_uses_canonical_registry_and_bounds() -> None:
    registration = CapabilityRegistration(
        from_signature=b"from",
        to_signature=b"to",
        transformer_body="for (@value <- fromCh) { toCh!(value + 1) }",
        uses_bound=3,
    )
    source = CapabilityAPI.register_term(registration)

    assert CAPABILITIES_REGISTRY_URI.endswith(
        "bm117qf6d3j7z8mhcgr86ezrwf1cgmjfjhyoiuyjxhiqpt6q3wrhj1"
    )
    assert "rl!(`rho:system:capabilities`, *capabilitiesCh)" in source
    assert '"66726f6d".hexToBytes()' in source
    assert '"746f".hexToBytes()' in source
    assert "contract transformer(fromCh, toCh)" in source
    assert "for (@value <- fromCh) { toCh!(value + 1) }" in source
    assert "bundle+{*transformer}" in source
    assert "      3," in source

    with pytest.raises(ValueError, match="non-negative"):
        CapabilityRegistration(b"from", b"to", "Nil", -1)


def test_ground_authority_presentation_is_typed() -> None:
    presentation = ground_authority_presentation(b"payer")
    assert presentation.WhichOneof("value") == "ground"
    assert presentation.ground == b"payer"


def test_cost_authority_evidence_preserves_exact_resource_maps() -> None:
    evidence = CostAuthorityEvidence.from_processed_deploy(
        valid_processed_deploy()
    )
    assert evidence.executed
    assert evidence.demand == {SLOT: 2}
    assert evidence.allocation == {SLOT: 2}
    assert evidence.fee_allocation == {GATEWAY: 1}
    assert evidence.realized == {SLOT: 1}
    assert evidence.settlement == {}
    assert evidence.stack_reservations == ((STACK, 1),)
    assert evidence.pre_state_root == PRE
    assert evidence.post_state_root == POST
    assert evidence.witness.events[0].event_id == EVENT
    assert evidence.witness.physical_draws[0].stack_ids == (STACK,)
    assert evidence.byte_cost == 0
    assert evidence.byte_cost_bound == 0
    assert evidence.byte_allocation == {}
    assert evidence.byte_settlement == {}


def test_cost_authority_certificate_id_has_a_stable_golden_vector() -> None:
    processed = valid_processed_deploy()
    certificate = CostAuthorityFundingCertificate.from_proto(
        processed.authorityFundingCertificate
    )

    assert certificate.certificate_id().hex() == (
        "1a6cbf75519760b1729bf5a5f0c876c6a2af8e7bf492cbffb40f27d5dd060eef"
    )


def test_cost_authority_evidence_preserves_typed_byte_accounting() -> None:
    evidence = CostAuthorityEvidence.from_processed_deploy(
        valid_byte_processed_deploy()
    )

    assert evidence.certificate.byte_cost_schedule_version == 1
    assert (
        evidence.certificate.byte_cost_schedule_digest
        == BYTE_COST_SCHEDULE_DIGEST
    )
    assert evidence.byte_cost == 3
    assert evidence.byte_cost_bound == 3
    assert evidence.byte_allocation == {SLOT: 3}
    assert evidence.byte_settlement == {SLOT: 3}
    assert evidence.witness.byte_events[0].event_id == BYTE_EVENT
    assert evidence.witness.byte_events[0].amount == 3


def test_cost_authority_evidence_rejects_byte_schedule_drift() -> None:
    certificate_drift = valid_processed_deploy()
    certificate_drift.authorityFundingCertificate.byteCostScheduleVersion += 1
    with pytest.raises(ValueError, match="unsupported byte-cost schedule version"):
        CostAuthorityEvidence.from_processed_deploy(certificate_drift)

    witness_drift = valid_processed_deploy()
    witness_drift.authorityCostWitness.byteCostScheduleDigest = b"x" * 32
    with pytest.raises(
        ValueError,
        match="unsupported byte-cost witness schedule digest",
    ):
        CostAuthorityEvidence.from_processed_deploy(witness_drift)


def test_cost_authority_evidence_rejects_invalid_byte_events() -> None:
    zero = valid_byte_processed_deploy()
    zero.authorityCostWitness.byteEvents[0].amount = 0
    zero.authorityCostWitness.byteCost = 0
    with pytest.raises(ValueError, match="amount must be positive"):
        CostAuthorityEvidence.from_processed_deploy(zero)

    unknown = valid_byte_processed_deploy()
    unknown.authorityCostWitness.byteEvents[0].kind = 3
    with pytest.raises(ValueError, match="kind is not recognized"):
        CostAuthorityEvidence.from_processed_deploy(unknown)

    mismatch = valid_byte_processed_deploy()
    mismatch.authorityCostWitness.byteCost = 2
    with pytest.raises(ValueError, match="does not equal byte-accounting events"):
        CostAuthorityEvidence.from_processed_deploy(mismatch)


def test_cost_authority_evidence_rejects_noncanonical_byte_events() -> None:
    unordered = valid_byte_processed_deploy()
    unordered.authorityCostWitness.byteEvents.add(
        eventId=b"a" * 32,
        kind=COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
        authority=authority(),
        amount=1,
    )
    unordered.authorityCostWitness.byteCost = 4
    with pytest.raises(ValueError, match="must be canonically ordered"):
        CostAuthorityEvidence.from_processed_deploy(unordered)

    conflict = valid_byte_processed_deploy()
    conflict.authorityCostWitness.byteEvents.add(
        eventId=BYTE_EVENT,
        kind=COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
        authority=authority(),
        amount=4,
    )
    conflict.authorityCostWitness.byteCost = 7
    with pytest.raises(ValueError, match="event identity conflicts"):
        CostAuthorityEvidence.from_processed_deploy(conflict)


def test_cost_authority_evidence_rejects_byte_event_sum_overflow() -> None:
    overflow = valid_byte_processed_deploy()
    del overflow.authorityCostWitness.byteEvents[:]
    overflow.authorityCostWitness.byteEvents.extend(
        [
            CostAuthorityByteEventProto(
                eventId=b"a" * 32,
                kind=COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
                authority=authority(),
                amount=(1 << 64) - 1,
            ),
            CostAuthorityByteEventProto(
                eventId=b"b" * 32,
                kind=COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
                authority=authority(),
                amount=(1 << 64) - 1,
            ),
        ]
    )
    with pytest.raises(ValueError, match="sum overflows uint64"):
        CostAuthorityEvidence.from_processed_deploy(overflow)


def test_cost_authority_evidence_rejects_excess_byte_settlement() -> None:
    excessive_cost = valid_byte_processed_deploy()
    excessive_cost.authorityFundingCertificate.byteCostBound = 2
    excessive_cost.authorityCostWitness.certificateId = (
        CostAuthorityFundingCertificate.from_proto(
            excessive_cost.authorityFundingCertificate
        ).certificate_id()
    )
    with pytest.raises(ValueError, match="byte cost exceeds certified bound"):
        CostAuthorityEvidence.from_processed_deploy(excessive_cost)

    excessive_settlement = valid_byte_processed_deploy()
    excessive_settlement.authorityFundingCertificate.byteAllocation[0].amount = 2
    excessive_settlement.authorityCostWitness.certificateId = (
        CostAuthorityFundingCertificate.from_proto(
            excessive_settlement.authorityFundingCertificate
        ).certificate_id()
    )
    with pytest.raises(
        ValueError,
        match="byte settlement exceeds certified allocation",
    ):
        CostAuthorityEvidence.from_processed_deploy(excessive_settlement)


def test_cost_authority_evidence_rejects_root_mismatch_and_duplicates() -> None:
    mismatched = valid_processed_deploy()
    mismatched.authorityCostWitness.preStateRoot = b"x" * 32
    with pytest.raises(ValueError, match="pre-state roots disagree"):
        CostAuthorityEvidence.from_processed_deploy(mismatched)

    duplicate = valid_processed_deploy()
    duplicate.authorityFundingCertificate.demand.append(
        CostAuthorityResourceProto(key=SLOT, amount=1)
    )
    with pytest.raises(ValueError, match="strictly key-ordered"):
        CostAuthorityEvidence.from_processed_deploy(duplicate)


def test_cost_authority_evidence_rejects_unbound_or_excess_evidence() -> None:
    unbound = valid_processed_deploy()
    unbound.authorityCostWitness.certificateId = b"c" * 32
    with pytest.raises(ValueError, match="does not bind"):
        CostAuthorityEvidence.from_processed_deploy(unbound)

    excess = valid_processed_deploy()
    excess.authorityCostWitness.events[0].debit[0].amount = 3
    excess.authorityCostWitness.realized[0].amount = 3
    with pytest.raises(ValueError, match="exceeds certified demand"):
        CostAuthorityEvidence.from_processed_deploy(excess)

    rejected = valid_processed_deploy()
    rejected.admissionStatus = DEPLOY_ADMISSION_STATUS_REJECTED
    with pytest.raises(ValueError, match="rejected deploy"):
        CostAuthorityEvidence.from_processed_deploy(rejected)


def test_cost_authority_evidence_rejects_noncanonical_physical_draws() -> None:
    wrong_event = valid_processed_deploy()
    wrong_event.authorityCostWitness.physicalDraws[0].eventId = b"z" * 32
    with pytest.raises(ValueError, match="do not match"):
        CostAuthorityEvidence.from_processed_deploy(wrong_event)

    overdraw = valid_processed_deploy()
    overdraw.authorityCostWitness.physicalDraws[0].stackIds.append(STACK)
    with pytest.raises(ValueError, match="must be ordered"):
        CostAuthorityEvidence.from_processed_deploy(overdraw)


def test_cost_authority_evidence_requires_complete_physical_settlement() -> None:
    missing = valid_processed_deploy()
    del missing.authorityCostWitness.physicalDraws[:]
    with pytest.raises(ValueError, match="do not match authority events"):
        CostAuthorityEvidence.from_processed_deploy(missing)

    mismatched = valid_processed_deploy()
    mismatched.authorityCostWitness.settlement.add(key=SLOT, amount=1)
    with pytest.raises(ValueError, match="does not equal physical balance draws"):
        CostAuthorityEvidence.from_processed_deploy(mismatched)

    balance_paid = valid_processed_deploy()
    del balance_paid.authorityCostWitness.physicalDraws[0].stackIds[:]
    balance_paid.authorityCostWitness.physicalDraws[0].balances.add(
        key=SLOT,
        amount=1,
    )
    balance_paid.authorityCostWitness.settlement.add(key=SLOT, amount=1)
    evidence = CostAuthorityEvidence.from_processed_deploy(balance_paid)
    assert evidence.settlement == {SLOT: 1}


def test_cost_authority_evidence_rejects_stack_identity_collisions() -> None:
    collision = valid_processed_deploy()
    birth = collision.authorityCostWitness.bornStacks.add(
        stackId=STACK,
        produceHash=b"b" * 32,
    )
    birth.cells.add(ground=b"slot")
    collision.authorityCostWitness.events.add(
        eventId=hashlib.blake2b(
            b"f1r3node:cost-accounted-rho:stack-transfer-event:v1"
            + birth.produceHash
            + (0).to_bytes(8, "little"),
            digest_size=32,
        ).digest(),
        authority=CostAuthority(),
    )
    collision.authorityCostWitness.physicalDraws.add(
        eventId=collision.authorityCostWitness.events[-1].eventId,
    )
    with pytest.raises(ValueError, match="collides with a reserved stack"):
        CostAuthorityEvidence.from_processed_deploy(collision)
