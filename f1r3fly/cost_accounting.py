"""Typed client workflows for native cost-accounted Rholang."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Iterable

from ecdsa.errors import MalformedPointError

from .client import F1r3flyClient
from .crypto import PrivateKey, PublicKey
from .par import par_as_bool, par_as_bytes, par_as_int, par_as_tuple
from .pb.CasperMessage_pb2 import (
    COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
    COST_AUTHORITY_BYTE_EVENT_KIND_CONSUME_INTRODUCTION,
    COST_AUTHORITY_BYTE_EVENT_KIND_PRODUCE_INTRODUCTION,
    COST_AUTHORITY_DEMAND_KIND_EXACT,
    COST_AUTHORITY_DEMAND_KIND_FINITE_UPPER_BOUND,
    COST_AUTHORITY_DEMAND_KIND_UNPROVABLE, DEPLOY_ADMISSION_STATUS_EXECUTED,
    CostAuthorityBornStackProto, CostAuthorityByteEventProto,
    CostAuthorityEventProto, CostAuthorityFundingCertificateProto,
    CostAuthorityPhysicalEventDrawProto, CostAuthorityResourceProto,
    CostAuthorityStackReservationProto, CostAuthorityWitnessProto,
    ProcessedDeployProto,
)
from .pb.DeployServiceCommon_pb2 import DeployInfo
from .pb.RhoTypes_pb2 import CostAuthority, CostSignature, Par
from .polling import deploy_and_read
from .vault import VaultAPI

CAPABILITIES_REGISTRY_URI = (
    "rho:id:bm117qf6d3j7z8mhcgr86ezrwf1cgmjfjhyoiuyjxhiqpt6q3wrhj1"
)


def _rho_string(value: str) -> str:
    if not value:
        raise ValueError("Rholang channel names must not be empty")
    return json.dumps(value, ensure_ascii=True)


def _rho_bytes(value: bytes) -> str:
    return f'"{value.hex()}".hexToBytes()'


def _nonempty_source(value: str, label: str) -> str:
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value


def ground_authority_presentation(value: bytes) -> CostSignature:
    return CostSignature(ground=value)


@dataclasses.dataclass(frozen=True)
class FundingSlotGrant:
    trigger_channel: str
    slot_address_channel: str
    completion_channel: str
    gateway_public_key: bytes
    outer_address_channel: str | None = None

    def __post_init__(self) -> None:
        channels = (
            self.trigger_channel,
            self.slot_address_channel,
            self.completion_channel,
            self.resolved_outer_address_channel,
        )
        for channel in channels:
            _rho_string(channel)
        if len(set(channels)) != len(channels):
            raise ValueError("funding-slot public channels must be distinct")
        if len(self.gateway_public_key) != 65 or self.gateway_public_key[0] != 4:
            raise ValueError(
                "gateway_public_key must be a 65-byte uncompressed secp256k1 key"
            )
        try:
            gateway = PublicKey.from_bytes(self.gateway_public_key)
        except MalformedPointError as error:
            raise ValueError(
                "gateway_public_key must encode a valid secp256k1 point"
            ) from error
        if gateway.to_bytes() != self.gateway_public_key:
            raise ValueError(
                "gateway_public_key must use canonical uncompressed encoding"
            )

    @property
    def resolved_outer_address_channel(self) -> str:
        return self.outer_address_channel or f"{self.slot_address_channel}-outer"

    def install_term(self, continuation_body: str) -> str:
        trigger = _rho_string(self.trigger_channel)
        outer_address = _rho_string(self.resolved_outer_address_channel)
        slot_address = _rho_string(self.slot_address_channel)
        completion = _rho_string(self.completion_channel)
        body = _nonempty_source(continuation_body, "continuation_body")
        gateway_public_key = _rho_bytes(self.gateway_public_key)
        return f"""new entry, slot, entryAddressCh, slotAddressCh,
    VaultAddress(`rho:vault:address`),
    DeployerIdOps(`rho:system:deployerId:ops`) in {{
  for (@request, deployerId <= @{trigger}) {{
    new publicKeyCh in {{
      DeployerIdOps!("pubKeyBytes", *deployerId, *publicKeyCh) |
      for (@publicKey <- publicKeyCh) {{
        if (publicKey == {gateway_public_key}) {{
          {{% for (@accepted <- entry) {{
            {body} |
            @{completion}!(accepted)
          }} %}}[ entry -o slot ] |
          entry!(request)
        }}
      }}
    }}
  }} |
  entry :: () |
  VaultAddress!("fromUnforgeable", *entry, *entryAddressCh) |
  for (@entryAddress <- entryAddressCh) {{
    @{outer_address}!!(entryAddress)
  }} |
  VaultAddress!(\"fromUnforgeable\", *slot, *slotAddressCh) |
  for (@slotAddress <- slotAddressCh) {{
    @{slot_address}!!(slotAddress)
  }}
}}"""

    def trigger_term(self, request_source: str = "0") -> str:
        request = _nonempty_source(request_source, "request_source")
        return f"""new deployerId(`rho:system:deployerId`) in {{
  @{_rho_string(self.trigger_channel)}!({request}, *deployerId)
}}"""


class FundingSlotAPI:
    def __init__(
        self,
        client: F1r3flyClient,
        shard_id: str = "root",
    ) -> None:
        self.client = client
        self.shard_id = shard_id
        self.vault = VaultAPI(client, shard_id)

    def install(
        self,
        grant: FundingSlotGrant,
        continuation_body: str,
        key: PrivateKey,
    ) -> str:
        return self.client.deploy_with_vabn_filled(
            key,
            grant.install_term(continuation_body),
            shard_id=self.shard_id,
        )

    def slot_address(
        self,
        grant: FundingSlotGrant,
        block_hash: str = "",
    ) -> str:
        value = self.client.read_channel(
            grant.slot_address_channel,
            block_hash,
        )
        if not isinstance(value, str):
            raise TypeError("funding-slot address channel did not contain a string")
        return value

    def outer_address(
        self,
        grant: FundingSlotGrant,
        block_hash: str = "",
    ) -> str:
        value = self.client.read_channel(
            grant.resolved_outer_address_channel,
            block_hash,
        )
        if not isinstance(value, str):
            raise TypeError("outer funding address channel did not contain a string")
        return value

    def addresses(
        self,
        grant: FundingSlotGrant,
        block_hash: str = "",
    ) -> tuple[str, str]:
        return (
            self.outer_address(grant, block_hash),
            self.slot_address(grant, block_hash),
        )

    def fund(
        self,
        grant: FundingSlotGrant,
        source_vault: str,
        outer_amount: int,
        continuation_amount: int,
        key: PrivateKey,
        block_hash: str = "",
        resolved_addresses: tuple[str, str] | None = None,
    ) -> str:
        if outer_amount <= 0 or continuation_amount <= 0:
            raise ValueError("funding amounts must be positive")
        outer_address, slot_address = (
            resolved_addresses
            if resolved_addresses is not None
            else self.addresses(grant, block_hash)
        )
        return self.vault.transfer_batch_ensure(
            source_vault,
            (
                (outer_address, outer_amount),
                (slot_address, continuation_amount),
            ),
            key,
        )

    def fund_address(
        self,
        slot_address: str,
        source_vault: str,
        amount: int,
        key: PrivateKey,
    ) -> str:
        if not slot_address:
            raise ValueError("slot_address must not be empty")
        if amount <= 0:
            raise ValueError("funding amount must be positive")
        return self.vault.transfer_ensure(
            source_vault,
            slot_address,
            amount,
            key,
        )

    def trigger(
        self,
        grant: FundingSlotGrant,
        key: PrivateKey,
        request_source: str = "0",
    ) -> str:
        return self.client.deploy_with_vabn_filled(
            key,
            grant.trigger_term(request_source),
            shard_id=self.shard_id,
        )


@dataclasses.dataclass(frozen=True)
class ExchangeResult:
    client_carrier: int
    vault_carrier: int


class ExchangeAPI:
    def __init__(
        self,
        client: F1r3flyClient,
        shard_id: str = "root",
    ) -> None:
        self.client = client
        self.shard_id = shard_id

    @staticmethod
    def integer_carrier_swap_term(
        client_datum: int,
        vault_datum: int,
    ) -> str:
        return f"""new deployId(`rho:system:deployId`),
    rl(`rho:registry:lookup`), exchangeCh,
    clientCarrier, vaultCarrier, swapCh in {{
  rl!(`rho:lang:exchange`, *exchangeCh) |
  for (@(_, Exchange) <- exchangeCh) {{
    clientCarrier!({client_datum}) |
    vaultCarrier!({vault_datum}) |
    @Exchange!(\"swap\", *clientCarrier, *vaultCarrier, *swapCh) |
    for (@(true, _) <- swapCh) {{
      for (@clientAfter <- clientCarrier & @vaultAfter <- vaultCarrier) {{
        deployId!((clientAfter, vaultAfter))
      }}
    }}
  }}
}}"""

    def swap_integer_carriers(
        self,
        client_datum: int,
        vault_datum: int,
        key: PrivateKey,
        inclusion_timeout: int,
        finalization_timeout: int,
    ) -> ExchangeResult:
        pars, _, _ = deploy_and_read(
            self.client,
            self.integer_carrier_swap_term(client_datum, vault_datum),
            key,
            inclusion_timeout,
            finalization_timeout,
            shard_id=self.shard_id,
        )
        values = par_as_tuple(pars[0])
        if len(values) != 2:
            raise ValueError("exchange result must contain exactly two carriers")
        return ExchangeResult(par_as_int(values[0]), par_as_int(values[1]))


@dataclasses.dataclass(frozen=True)
class CapabilityRegistration:
    from_signature: bytes
    to_signature: bytes
    transformer_body: str
    uses_bound: int = 1

    def __post_init__(self) -> None:
        if not self.from_signature or not self.to_signature:
            raise ValueError("capability signatures must not be empty")
        if self.uses_bound < 0:
            raise ValueError("uses_bound must be non-negative")
        _nonempty_source(self.transformer_body, "transformer_body")


@dataclasses.dataclass(frozen=True)
class CapabilityRegistrationResult:
    success: bool
    handle: bytes


class CapabilityAPI:
    def __init__(
        self,
        client: F1r3flyClient,
        shard_id: str = "root",
    ) -> None:
        self.client = client
        self.shard_id = shard_id

    @staticmethod
    def register_term(registration: CapabilityRegistration) -> str:
        transformer = registration.transformer_body
        return f"""new deployId(`rho:system:deployId`),
    rl(`rho:registry:lookup`), capabilitiesCh,
    deployerId(`rho:system:deployerId`), transformer, registerCh in {{
  contract transformer(fromCh, toCh) = {{
    {transformer}
  }} |
  rl!(`rho:system:capabilities`, *capabilitiesCh) |
  for (@(_, Capabilities) <- capabilitiesCh) {{
    @Capabilities!(
      \"register\",
      {_rho_bytes(registration.from_signature)},
      {_rho_bytes(registration.to_signature)},
      bundle+{{*transformer}},
      {registration.uses_bound},
      *deployerId,
      *registerCh
    ) |
    for (@result <- registerCh) {{ deployId!(result) }}
  }}
}}"""

    @staticmethod
    def invoke_term(handle: bytes, input_source: str) -> str:
        if not handle:
            raise ValueError("capability handle must not be empty")
        value = _nonempty_source(input_source, "input_source")
        return f"""new deployId(`rho:system:deployId`),
    rl(`rho:registry:lookup`), capabilitiesCh, sourceCh, invokeCh in {{
  rl!(`rho:system:capabilities`, *capabilitiesCh) |
  for (@(_, Capabilities) <- capabilitiesCh) {{
    sourceCh!({value}) |
    @Capabilities!(\"invoke\", {_rho_bytes(handle)}, *sourceCh, *invokeCh) |
    for (@result <- invokeCh) {{ deployId!(result) }}
  }}
}}"""

    def register(
        self,
        registration: CapabilityRegistration,
        key: PrivateKey,
    ) -> str:
        return self.client.deploy_with_vabn_filled(
            key,
            self.register_term(registration),
            shard_id=self.shard_id,
        )

    def invoke(
        self,
        handle: bytes,
        input_source: str,
        key: PrivateKey,
    ) -> str:
        return self.client.deploy_with_vabn_filled(
            key,
            self.invoke_term(handle, input_source),
            shard_id=self.shard_id,
        )

    @staticmethod
    def registration_result(par: Par) -> CapabilityRegistrationResult:
        values = par_as_tuple(par)
        if len(values) != 2:
            raise ValueError("capability registration result must have arity two")
        success = par_as_bool(values[0])
        return CapabilityRegistrationResult(
            success=success,
            handle=par_as_bytes(values[1]) if success else b"",
        )


AUTHORITY_ACCOUNTING_PROTOCOL_VERSION = 8
BYTE_COST_SCHEDULE_VERSION = 1
BYTE_COST_SCHEDULE_DIGEST = bytes.fromhex(
    "20f7da72457c462469ffb9e9d476e203b1395cada72bf102d8af484c32a4840c"
)
_CERTIFICATE_DOMAIN = b"f1r3node:authority-funding-certificate:v8"
_STACK_TRANSFER_EVENT_DOMAIN = (
    b"f1r3node:cost-accounted-rho:stack-transfer-event:v1"
)
_MAX_U64 = (1 << 64) - 1
_BYTE_EVENT_KINDS = frozenset(
    (
        COST_AUTHORITY_BYTE_EVENT_KIND_PRODUCE_INTRODUCTION,
        COST_AUTHORITY_BYTE_EVENT_KIND_CONSUME_INTRODUCTION,
        COST_AUTHORITY_BYTE_EVENT_KIND_COMM,
    )
)


def _digest(value: bytes, label: str) -> bytes:
    result = bytes(value)
    if len(result) != 32:
        raise ValueError(f"{label} must be exactly 32 bytes")
    return result


def _resources(
    values: Iterable[CostAuthorityResourceProto],
    label: str,
) -> tuple[CostAuthorityResource, ...]:
    result = []
    previous = None
    for value in values:
        key = _digest(value.key, f"{label} key")
        if value.amount <= 0:
            raise ValueError(f"{label} cannot contain a zero amount")
        if previous is not None and previous >= key:
            raise ValueError(f"{label} must be strictly key-ordered")
        result.append(CostAuthorityResource(key, value.amount))
        previous = key
    return tuple(result)


def _resource_map(
    resources: tuple[CostAuthorityResource, ...],
) -> dict[bytes, int]:
    return {resource.key: resource.amount for resource in resources}


def _sum_resources(
    groups: Iterable[tuple[CostAuthorityResource, ...]],
) -> dict[bytes, int]:
    result: dict[bytes, int] = {}
    for group in groups:
        for resource in group:
            amount = result.get(resource.key, 0) + resource.amount
            if amount > _MAX_U64:
                raise ValueError("cost-authority resource sum overflows uint64")
            result[resource.key] = amount
    return result


def _dominates(
    available: tuple[CostAuthorityResource, ...],
    required: tuple[CostAuthorityResource, ...],
) -> bool:
    supply = _resource_map(available)
    return all(supply.get(value.key, 0) >= value.amount for value in required)


def _write_u64(output: bytearray, value: int) -> None:
    if value < 0 or value > _MAX_U64:
        raise ValueError("canonical uint64 value is out of range")
    output.extend(value.to_bytes(8, "little"))


def _write_resources(
    output: bytearray,
    resources: tuple[CostAuthorityResource, ...],
) -> None:
    _write_u64(output, len(resources))
    for resource in resources:
        output.extend(resource.key)
        _write_u64(output, resource.amount)


def _signature_bytes(signature: CostSignature, label: str) -> bytes:
    value = signature.WhichOneof("value")
    if value is None:
        raise ValueError(f"{label} is missing its signature")
    if value == "bound_level":
        raise ValueError(f"{label} contains an unresolved bound signature")
    if value == "unit" and not signature.unit:
        raise ValueError(f"{label} contains a non-canonical unit signature")
    if value == "compound":
        if len(signature.compound.elements) < 2:
            raise ValueError(f"{label} contains a malformed compound signature")
        for element in signature.compound.elements:
            _signature_bytes(element, label)
    return signature.SerializeToString(deterministic=True)


@dataclasses.dataclass(frozen=True)
class CostAuthorityResource:
    key: bytes
    amount: int


@dataclasses.dataclass(frozen=True)
class CostAuthorityStackReservation:
    stack_id: bytes
    pop_count: int

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityStackReservationProto,
    ) -> CostAuthorityStackReservation:
        if value.popCount <= 0:
            raise ValueError("authority stack reservation cannot be empty")
        return cls(
            _digest(value.stackId, "authority stack reservation identity"),
            value.popCount,
        )


@dataclasses.dataclass(frozen=True)
class CostAuthorityFundingCertificate:
    protocol_version: int
    program_hash: bytes
    pre_state_root: bytes
    reservation_id: bytes
    demand_kind: int
    demand: tuple[CostAuthorityResource, ...]
    proof: bytes
    unprovable_reason: int
    allocation: tuple[CostAuthorityResource, ...]
    stack_reservations: tuple[CostAuthorityStackReservation, ...]
    fee_allocation: tuple[CostAuthorityResource, ...]
    fee_recipient: bytes
    byte_cost_schedule_version: int
    byte_cost_schedule_digest: bytes
    byte_cost_bound: int
    byte_allocation: tuple[CostAuthorityResource, ...]

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityFundingCertificateProto,
    ) -> CostAuthorityFundingCertificate:
        if value.protocolVersion != AUTHORITY_ACCOUNTING_PROTOCOL_VERSION:
            raise ValueError("unsupported cost-authority protocol version")
        if value.byteCostScheduleVersion != BYTE_COST_SCHEDULE_VERSION:
            raise ValueError("unsupported byte-cost schedule version")
        byte_cost_schedule_digest = _digest(
            value.byteCostScheduleDigest,
            "byte-cost schedule digest",
        )
        if byte_cost_schedule_digest != BYTE_COST_SCHEDULE_DIGEST:
            raise ValueError("unsupported byte-cost schedule digest")
        demand_kind = value.demandKind
        demand = _resources(value.demand, "cost-authority demand")
        proof = bytes(value.proof)
        reason = value.unprovableReason
        if demand_kind == COST_AUTHORITY_DEMAND_KIND_EXACT:
            if proof or reason != 0:
                raise ValueError("exact authority demand has non-exact metadata")
        elif demand_kind == COST_AUTHORITY_DEMAND_KIND_FINITE_UPPER_BOUND:
            if not proof or reason != 0:
                raise ValueError(
                    "finite authority bound must carry only a non-empty proof"
                )
        elif demand_kind == COST_AUTHORITY_DEMAND_KIND_UNPROVABLE:
            if demand or proof or reason not in range(4):
                raise ValueError("unprovable authority demand is malformed")
        else:
            raise ValueError("cost-authority demand kind is not recognized")

        reservations = tuple(
            CostAuthorityStackReservation.from_proto(reservation)
            for reservation in value.stackReservations
        )
        if any(
            left.stack_id >= right.stack_id
            for left, right in zip(reservations, reservations[1:])
        ):
            raise ValueError(
                "authority stack reservations must be strictly identity-ordered"
            )
        allocation = _resources(
            value.allocation,
            "cost-authority allocation",
        )
        fee_allocation = _resources(
            value.feeAllocation,
            "cost-authority fee allocation",
        )
        byte_allocation = _resources(
            value.byteAllocation,
            "byte-cost allocation",
        )
        _sum_resources((allocation, byte_allocation, fee_allocation))
        return cls(
            protocol_version=value.protocolVersion,
            program_hash=_digest(value.programHash, "authority program hash"),
            pre_state_root=_digest(
                value.preStateRoot,
                "authority certificate pre-state root",
            ),
            reservation_id=_digest(
                value.reservationId,
                "authority reservation identity",
            ),
            demand_kind=demand_kind,
            demand=demand,
            proof=proof,
            unprovable_reason=reason,
            allocation=allocation,
            stack_reservations=reservations,
            fee_allocation=fee_allocation,
            fee_recipient=bytes(value.feeRecipient),
            byte_cost_schedule_version=value.byteCostScheduleVersion,
            byte_cost_schedule_digest=byte_cost_schedule_digest,
            byte_cost_bound=value.byteCostBound,
            byte_allocation=byte_allocation,
        )

    def certificate_id(self) -> bytes:
        output = bytearray(_CERTIFICATE_DOMAIN)
        output.extend(self.protocol_version.to_bytes(4, "little"))
        output.extend(self.program_hash)
        output.extend(self.pre_state_root)
        output.extend(self.reservation_id)
        output.append(self.demand_kind)
        if self.demand_kind == COST_AUTHORITY_DEMAND_KIND_EXACT:
            _write_resources(output, self.demand)
        elif self.demand_kind == COST_AUTHORITY_DEMAND_KIND_FINITE_UPPER_BOUND:
            _write_resources(output, self.demand)
            _write_u64(output, len(self.proof))
            output.extend(self.proof)
        else:
            output.append(self.unprovable_reason)
        _write_resources(output, self.allocation)
        _write_u64(output, len(self.stack_reservations))
        for reservation in self.stack_reservations:
            output.extend(reservation.stack_id)
            _write_u64(output, reservation.pop_count)
        _write_resources(output, self.fee_allocation)
        _write_u64(output, len(self.fee_recipient))
        output.extend(self.fee_recipient)
        output.extend(self.byte_cost_schedule_version.to_bytes(4, "little"))
        output.extend(self.byte_cost_schedule_digest)
        _write_u64(output, self.byte_cost_bound)
        _write_resources(output, self.byte_allocation)
        return hashlib.blake2b(output, digest_size=32).digest()


@dataclasses.dataclass(frozen=True)
class CostAuthorityRegion:
    instance_id: bytes
    signature: bytes


def _authority_regions(
    authority: CostAuthority,
) -> tuple[CostAuthorityRegion, ...]:
    regions = []
    previous = None
    for region in authority.regions:
        instance_id = _digest(region.instance_id, "authority region identity")
        if previous is not None and previous >= instance_id:
            raise ValueError("authority regions must be strictly identity-ordered")
        if not region.HasField("signature"):
            raise ValueError("authority region is missing its signature")
        regions.append(
            CostAuthorityRegion(
                instance_id,
                _signature_bytes(region.signature, "authority region"),
            )
        )
        previous = instance_id
    return tuple(regions)


@dataclasses.dataclass(frozen=True)
class CostAuthorityEvent:
    event_id: bytes
    debit: tuple[CostAuthorityResource, ...]
    authority: tuple[CostAuthorityRegion, ...]

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityEventProto,
    ) -> CostAuthorityEvent:
        if not value.HasField("authority"):
            raise ValueError("authority event is missing its wrapper authority")
        return cls(
            event_id=_digest(value.eventId, "authority event identity"),
            debit=_resources(value.debit, "authority event debit"),
            authority=_authority_regions(value.authority),
        )


@dataclasses.dataclass(frozen=True)
class CostAuthorityByteEvent:
    event_id: bytes
    kind: int
    authority: tuple[CostAuthorityRegion, ...]
    authority_encoding: bytes
    amount: int

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityByteEventProto,
    ) -> CostAuthorityByteEvent:
        if value.kind not in _BYTE_EVENT_KINDS:
            raise ValueError("byte-accounting event kind is not recognized")
        if not value.HasField("authority"):
            raise ValueError(
                "authority byte event is missing its wrapper authority"
            )
        if value.amount <= 0:
            raise ValueError("authority byte-event amount must be positive")
        return cls(
            event_id=_digest(value.eventId, "authority byte-event identity"),
            kind=value.kind,
            authority=_authority_regions(value.authority),
            authority_encoding=value.authority.SerializeToString(
                deterministic=True
            ),
            amount=value.amount,
        )

    def canonical_key(self) -> bytes:
        output = bytearray(self.event_id)
        output.append(self.kind)
        _write_u64(output, len(self.authority_encoding))
        output.extend(self.authority_encoding)
        _write_u64(output, self.amount)
        return bytes(output)


@dataclasses.dataclass(frozen=True)
class CostAuthorityPhysicalEventDraw:
    event_id: bytes
    balances: tuple[CostAuthorityResource, ...]
    stack_ids: tuple[bytes, ...]

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityPhysicalEventDrawProto,
    ) -> CostAuthorityPhysicalEventDraw:
        stack_ids = tuple(
            _digest(stack_id, "authority stack identity")
            for stack_id in value.stackIds
        )
        if any(
            left >= right
            for left, right in zip(stack_ids, stack_ids[1:])
        ):
            raise ValueError("physical draw stack identities must be ordered")
        return cls(
            event_id=_digest(
                value.eventId,
                "authority physical draw event identity",
            ),
            balances=_resources(
                value.balances,
                "authority physical draw balances",
            ),
            stack_ids=stack_ids,
        )


@dataclasses.dataclass(frozen=True)
class CostAuthorityBornStack:
    stack_id: bytes
    produce_hash: bytes
    cells: tuple[bytes, ...]

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityBornStackProto,
    ) -> CostAuthorityBornStack:
        cells = tuple(
            _signature_bytes(cell, "authority born stack cell")
            for cell in value.cells
        )
        if not cells:
            raise ValueError("authority born stack cannot be empty")
        return cls(
            stack_id=_digest(value.stackId, "authority born stack identity"),
            produce_hash=_digest(
                value.produceHash,
                "authority born stack produce identity",
            ),
            cells=cells,
        )


@dataclasses.dataclass(frozen=True)
class CostAuthorityWitness:
    protocol_version: int
    certificate_id: bytes
    pre_state_root: bytes
    post_state_root: bytes
    events: tuple[CostAuthorityEvent, ...]
    realized: tuple[CostAuthorityResource, ...]
    settlement: tuple[CostAuthorityResource, ...]
    physical_draws: tuple[CostAuthorityPhysicalEventDraw, ...]
    born_stacks: tuple[CostAuthorityBornStack, ...]
    byte_cost_schedule_version: int
    byte_cost_schedule_digest: bytes
    byte_events: tuple[CostAuthorityByteEvent, ...]
    byte_cost: int
    byte_settlement: tuple[CostAuthorityResource, ...]

    @classmethod
    def from_proto(
        cls,
        value: CostAuthorityWitnessProto,
    ) -> CostAuthorityWitness:
        if value.protocolVersion != AUTHORITY_ACCOUNTING_PROTOCOL_VERSION:
            raise ValueError("unsupported cost-authority protocol version")
        if value.byteCostScheduleVersion != BYTE_COST_SCHEDULE_VERSION:
            raise ValueError("unsupported byte-cost witness schedule version")
        byte_cost_schedule_digest = _digest(
            value.byteCostScheduleDigest,
            "byte-cost witness schedule digest",
        )
        if byte_cost_schedule_digest != BYTE_COST_SCHEDULE_DIGEST:
            raise ValueError("unsupported byte-cost witness schedule digest")
        events = tuple(
            CostAuthorityEvent.from_proto(event) for event in value.events
        )
        if len({event.event_id for event in events}) != len(events):
            raise ValueError("authority event identities must be unique")
        realized = _resources(value.realized, "realized authority")
        if _sum_resources(event.debit for event in events) != _resource_map(
            realized
        ):
            raise ValueError("realized authority does not equal event debits")

        byte_events = tuple(
            CostAuthorityByteEvent.from_proto(event)
            for event in value.byteEvents
        )
        if any(
            left.canonical_key() > right.canonical_key()
            for left, right in zip(byte_events, byte_events[1:])
        ):
            raise ValueError("byte-accounting events must be canonically ordered")
        byte_identities: dict[bytes, tuple[int, bytes, int]] = {}
        byte_cost = 0
        for event in byte_events:
            identity = (event.kind, event.authority_encoding, event.amount)
            existing = byte_identities.get(event.event_id)
            if existing is not None and existing != identity:
                raise ValueError("byte-accounting event identity conflicts")
            byte_identities[event.event_id] = identity
            byte_cost += event.amount
            if byte_cost > _MAX_U64:
                raise ValueError("byte-accounting event sum overflows uint64")
        if byte_cost != value.byteCost:
            raise ValueError("byte cost does not equal byte-accounting events")

        draws = tuple(
            CostAuthorityPhysicalEventDraw.from_proto(draw)
            for draw in value.physicalDraws
        )
        if len(draws) != len(events) or any(
            event.event_id != draw.event_id
            for event, draw in zip(events, draws)
        ):
            raise ValueError("physical draws do not match authority events")

        settlement = _resources(
            value.settlement,
            "authority settlement",
        )
        if _sum_resources(draw.balances for draw in draws) != _resource_map(
            settlement
        ):
            raise ValueError(
                "authority settlement does not equal physical balance draws"
            )

        born_stacks = tuple(
            CostAuthorityBornStack.from_proto(birth)
            for birth in value.bornStacks
        )
        if any(
            left.stack_id >= right.stack_id
            for left, right in zip(born_stacks, born_stacks[1:])
        ):
            raise ValueError("born stack identities must be strictly ordered")
        event_ids = {event.event_id for event in events}
        for birth in born_stacks:
            for index in range(len(birth.cells)):
                event_id = hashlib.blake2b(
                    _STACK_TRANSFER_EVENT_DOMAIN
                    + birth.produce_hash
                    + index.to_bytes(8, "little"),
                    digest_size=32,
                ).digest()
                if event_id not in event_ids:
                    raise ValueError(
                        "born stack cell has no matching transfer event"
                    )
        return cls(
            protocol_version=value.protocolVersion,
            certificate_id=_digest(
                value.certificateId,
                "authority witness certificate identity",
            ),
            pre_state_root=_digest(
                value.preStateRoot,
                "authority witness pre-state root",
            ),
            post_state_root=_digest(
                value.postStateRoot,
                "authority witness post-state root",
            ),
            events=events,
            realized=realized,
            settlement=settlement,
            physical_draws=draws,
            born_stacks=born_stacks,
            byte_cost_schedule_version=value.byteCostScheduleVersion,
            byte_cost_schedule_digest=byte_cost_schedule_digest,
            byte_events=byte_events,
            byte_cost=value.byteCost,
            byte_settlement=_resources(
                value.byteSettlement,
                "byte-cost settlement",
            ),
        )


@dataclasses.dataclass(frozen=True)
class CostAuthorityEvidence:
    certificate: CostAuthorityFundingCertificate
    witness: CostAuthorityWitness

    @property
    def executed(self) -> bool:
        return True

    @property
    def demand(self) -> dict[bytes, int]:
        return _resource_map(self.certificate.demand)

    @property
    def allocation(self) -> dict[bytes, int]:
        return _resource_map(self.certificate.allocation)

    @property
    def fee_allocation(self) -> dict[bytes, int]:
        return _resource_map(self.certificate.fee_allocation)

    @property
    def realized(self) -> dict[bytes, int]:
        return _resource_map(self.witness.realized)

    @property
    def settlement(self) -> dict[bytes, int]:
        return _resource_map(self.witness.settlement)

    @property
    def byte_allocation(self) -> dict[bytes, int]:
        return _resource_map(self.certificate.byte_allocation)

    @property
    def byte_settlement(self) -> dict[bytes, int]:
        return _resource_map(self.witness.byte_settlement)

    @property
    def byte_cost(self) -> int:
        return self.witness.byte_cost

    @property
    def byte_cost_bound(self) -> int:
        return self.certificate.byte_cost_bound

    @property
    def stack_reservations(self) -> tuple[tuple[bytes, int], ...]:
        return tuple(
            (reservation.stack_id, reservation.pop_count)
            for reservation in self.certificate.stack_reservations
        )

    @property
    def pre_state_root(self) -> bytes:
        return self.certificate.pre_state_root

    @property
    def post_state_root(self) -> bytes:
        return self.witness.post_state_root

    @classmethod
    def from_processed_deploy(
        cls,
        deploy: ProcessedDeployProto | DeployInfo,
    ) -> CostAuthorityEvidence:
        if deploy.admissionStatus != DEPLOY_ADMISSION_STATUS_EXECUTED:
            raise ValueError("rejected deploy has no executable authority evidence")
        if not deploy.HasField("authorityFundingCertificate"):
            raise ValueError("processed deploy has no funding certificate")
        if not deploy.HasField("authorityCostWitness"):
            raise ValueError("processed deploy has no cost witness")
        certificate = CostAuthorityFundingCertificate.from_proto(
            deploy.authorityFundingCertificate
        )
        witness = CostAuthorityWitness.from_proto(deploy.authorityCostWitness)
        if certificate.protocol_version != witness.protocol_version:
            raise ValueError("certificate and witness protocol versions disagree")
        if (
            certificate.byte_cost_schedule_version
            != witness.byte_cost_schedule_version
            or certificate.byte_cost_schedule_digest
            != witness.byte_cost_schedule_digest
        ):
            raise ValueError("certificate and witness byte-cost schedules disagree")
        if witness.byte_cost > certificate.byte_cost_bound:
            raise ValueError("byte cost exceeds certified bound")
        if not _dominates(
            certificate.byte_allocation,
            witness.byte_settlement,
        ):
            raise ValueError("byte settlement exceeds certified allocation")
        if certificate.certificate_id() != witness.certificate_id:
            raise ValueError("witness does not bind the funding certificate")
        if certificate.pre_state_root != witness.pre_state_root:
            raise ValueError("certificate and witness pre-state roots disagree")
        if _digest(deploy.preStateHash, "processed deploy pre-state root") != (
            certificate.pre_state_root
        ):
            raise ValueError(
                "processed deploy and certificate pre-state roots disagree"
            )
        if _digest(deploy.postStateHash, "processed deploy post-state root") != (
            witness.post_state_root
        ):
            raise ValueError("processed deploy and witness post-state roots disagree")
        if certificate.demand_kind == COST_AUTHORITY_DEMAND_KIND_UNPROVABLE:
            raise ValueError("executed deploy cannot carry unprovable demand")
        if not _dominates(certificate.demand, witness.realized):
            raise ValueError("realized authority exceeds certified demand")
        if not _dominates(certificate.allocation, witness.settlement):
            raise ValueError("settlement exceeds certified allocation")

        pops: dict[bytes, int] = {}
        for draw in witness.physical_draws:
            for stack_id in draw.stack_ids:
                pops[stack_id] = pops.get(stack_id, 0) + 1
        reserved = {
            reservation.stack_id: reservation.pop_count
            for reservation in certificate.stack_reservations
        }
        born = {
            stack.stack_id: len(stack.cells) for stack in witness.born_stacks
        }
        if reserved.keys() & born.keys():
            raise ValueError(
                "born authority stack collides with a reserved stack identity"
            )
        if any(
            count > reserved.get(stack_id, 0) + born.get(stack_id, 0)
            for stack_id, count in pops.items()
        ):
            raise ValueError("physical stack draw exceeds certified availability")
        return cls(certificate=certificate, witness=witness)
