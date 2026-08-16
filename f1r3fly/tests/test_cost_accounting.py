import hashlib

import pytest

from f1r3fly.cost_accounting import (
    AUTHORITY_ACCOUNTING_PROTOCOL_VERSION, CAPABILITIES_REGISTRY_URI,
    CapabilityAPI, CapabilityRegistration, CostAuthorityEvidence,
    CostAuthorityFundingCertificate, ExchangeAPI, FundingSlotGrant,
    ground_authority_presentation,
)
from f1r3fly.pb.CasperMessage_pb2 import (
    DEPLOY_ADMISSION_STATUS_EXECUTED, DEPLOY_ADMISSION_STATUS_REJECTED,
    CostAuthorityEventProto, CostAuthorityFundingCertificateProto,
    CostAuthorityPhysicalEventDrawProto, CostAuthorityResourceProto,
    CostAuthorityStackReservationProto, CostAuthorityWitnessProto,
    ProcessedDeployProto,
)
from f1r3fly.pb.RhoTypes_pb2 import CostAuthority, CostRegion, CostSignature

SLOT = b"s" * 32
GATEWAY = b"g" * 32
STACK = b"k" * 32
EVENT = b"e" * 32
PRE = b"p" * 32
POST = b"o" * 32
PROGRAM = b"m" * 32
RESERVATION = b"r" * 32


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
                authority=CostAuthority(
                    regions=[
                        CostRegion(
                            instance_id=b"i" * 32,
                            signature=CostSignature(ground=b"slot"),
                        )
                    ]
                ),
            )
        ],
        realized=[CostAuthorityResourceProto(key=SLOT, amount=1)],
        physicalDraws=[
            CostAuthorityPhysicalEventDrawProto(
                eventId=EVENT,
                stackIds=[STACK],
            )
        ],
    )
    return ProcessedDeployProto(
        preStateHash=PRE,
        postStateHash=POST,
        admissionStatus=DEPLOY_ADMISSION_STATUS_EXECUTED,
        authorityFundingCertificate=certificate_proto,
        authorityCostWitness=witness,
    )


def test_funding_slot_grant_renders_native_lollipop_workflow() -> None:
    grant = FundingSlotGrant(
        trigger_channel='agent-"trigger',
        slot_address_channel="agent-slot-address",
        completion_channel="agent-complete",
        gateway_public_key=b"\x04" + b"g" * 64,
    )
    source = grant.install_term('@"agent-ran"!(request)')

    assert 'for (@request, deployerId <= @"agent-\\"trigger")' in source
    assert 'DeployerIdOps!("pubKeyBytes", *deployerId, *publicKeyCh)' in source
    assert '"04' + (b"g" * 64).hex() + '".hexToBytes()' in source
    assert "if (publicKey ==" in source
    assert "entry!(request)" in source
    assert "{% for (@request <- entry)" in source
    assert "%}[ entry -o slot ]" in source
    assert "entry :: ()" in source
    assert 'VaultAddress!("fromUnforgeable", *slot, *slotAddressCh)' in source
    assert '@"agent-slot-address"!!(slotAddress)' in source
    assert (
        grant.trigger_term("7")
        == """new deployerId(`rho:system:deployerId`) in {
  @"agent-\\"trigger"!(7, *deployerId)
}"""
    )


def test_funding_slot_grant_rejects_empty_source_and_channel() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        FundingSlotGrant("", "address", "done", b"\x04" + b"g" * 64).install_term("Nil")
    with pytest.raises(ValueError, match="must not be empty"):
        FundingSlotGrant(
            "trigger", "address", "done", b"\x04" + b"g" * 64
        ).install_term("  ")
    with pytest.raises(ValueError, match="65-byte uncompressed"):
        FundingSlotGrant("trigger", "address", "done", b"short")


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


def test_cost_authority_certificate_id_has_a_stable_golden_vector() -> None:
    processed = valid_processed_deploy()
    certificate = CostAuthorityFundingCertificate.from_proto(
        processed.authorityFundingCertificate
    )

    assert certificate.certificate_id().hex() == (
        "88ecd40f4d389fa44d6f5464bf4e704cc67f1a487894d1242bd303e0a6d02096"
    )


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
