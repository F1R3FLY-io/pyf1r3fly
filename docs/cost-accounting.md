# Cost-accounted Rholang client workflows

This guide defines the Python application boundary for native cost-accounted
Rholang. It covers canonical deploy authority presentations, wallet-funded
located purses, gateway-authenticated linear continuations, the conserving
Exchange, bounded capabilities, terminal deploy status, and immutable views of
consensus evidence. It does not create a second fuel currency or reproduce the
node's consensus verifier in application code.

The governing semantics are the repository-local papers
`cost-accounted-rho.tex` and `continued-gslt-cost-v2.tex`. Their resource
interpretation builds on linear logic [1] and the reflective higher-order
calculus [2]. The node remains authoritative for normalization, execution,
replay, and settlement.

## Terms and trust boundaries

An **authority signature** identifies one cost-bearing surface. A **located
purse** is a surface whose signature comes from an unforgeable Rholang name. A
**slot address** is the public SystemVault address deterministically derived
from that name. Anyone may deposit to the public address, but only a process
holding the name as a first-class Rholang capability can locate, draw, or
delegate its purse. The serialized `GPrivate` identity is not a confidentiality
secret; source code has no bytes-to-`GPrivate` constructor, so observing
consensus evidence does not inject the capability into a process.

A **funding certificate** binds a protocol version, normalized program hash,
pre-state root, finite demand, authenticated allocation, stack reservations,
fee allocation, reservation identity, byte-cost schedule, byte-cost bound, and
byte allocation. A **cost witness** binds that certificate to the causal event
sequence, realized authority, physical draws, born stacks, byte events, byte
cost, both settlements, and the post-state root. Independent validators
recompute and verify both objects during replay.

For every authority lane `s`, admission and settlement preserve:

```math
B_{A,s}+B_{Q,s}+F_s \leq \Sigma_s
```

where $`\Sigma_s`$ is authenticated pre-state custody, $`B_{A,s}`$ is the
physical authority bound, $`B_{Q,s}`$ is the canonical RSpace byte bound, and
$`F_s`$ is the proposer-fee allocation. If $`\kappa_s`$ is the realized
physical authority draw and $`Q_s`$ the realized byte charge, the post-state
relation is:

```math
\Sigma'_s = \Sigma_s - \kappa_s - Q_s - F_s
```

Unused certified capacity never leaves custody, so there is no refund mint and
no transient consensus reservation cell.

## End-to-end workflow

![Sequence diagram showing an administrator installing a persistent unforgeable funding slot, a user depositing from canonical wallet custody, an authenticated gateway triggering the located lollipop continuation, validators rejecting before mutation when custody is insufficient or atomically executing and settling when sufficient, and the Python client structurally validating the returned consensus evidence.](diagrams/wallet-funded-slot-flow.svg)

(*Source: [`diagrams/wallet-funded-slot-flow.puml`](diagrams/wallet-funded-slot-flow.puml). Render with `plantuml -tsvg docs/diagrams/wallet-funded-slot-flow.puml`.*)

The workflow deliberately separates three authorities:

1. The administrator chooses the continuation and the gateway public key.
2. The user controls a wallet vault and chooses how much to deposit.
3. The gateway can activate only the grant handler that retains the private
   outer and continuation names; it does not gain the user's wallet key or
   general wallet withdrawal authority.

This is the application pattern for user-funded Embers agents. Each grant owns
its own unforgeable slot, so one user or agent cannot consume another grant's
custody.

## Creating and funding a grant

`FundingSlotGrant.install_term` creates unforgeable `entry` and `slot` names,
publishes both SystemVault deposit addresses, and persists an authenticated
activation scaffold. It does not install `entry -o slot` while both new purses
are empty: candidate-created supply cannot fund the deployment that created it.
After earlier finalized funding, the configured gateway causes the handler to
instantiate the lollipop and signal `entry` in one activation. The public
trigger accepts a request only when `rho:system:deployerId:ops` resolves the
submitting deployer to the configured uncompressed secp256k1 public key.

```python
from f1r3fly.client import F1r3flyClient
from f1r3fly.cost_accounting import FundingSlotAPI, FundingSlotGrant
from f1r3fly.crypto import PrivateKey

validator_client = F1r3flyClient("validator.example", 40401)
readonly_client = F1r3flyClient("readonly.example", 40401)
administrator_key = PrivateKey.generate()
gateway_key = PrivateKey.generate()
user_key = PrivateKey.generate()
source_key = PrivateKey.generate()
source_vault = source_key.get_public_key().get_vault_address()

grant = FundingSlotGrant(
    trigger_channel="embers:grant:7:trigger",
    slot_address_channel="embers:grant:7:address",
    outer_address_channel="embers:grant:7:outer-address",
    completion_channel="embers:grant:7:complete",
    gateway_public_key=gateway_key.get_public_key().to_bytes(),
)
validator_slots = FundingSlotAPI(validator_client, shard_id="root")
readonly_slots = FundingSlotAPI(readonly_client, shard_id="root")
install_id = validator_slots.install(
    grant,
    'new result in { result!("agent completed") }',
    administrator_key,
)
```

`source_key` represents an already funded wallet in this abbreviated example;
creating a key pair alone does not create REV.

Wait for `install_id` to reach terminal `Finalized` state before reading both
addresses. The wallet owner may then transfer existing native custody to both
purses atomically:

```python
outer_address, slot_address = readonly_slots.addresses(
    grant,
    finalized_install_hash,
)
fund_id = validator_slots.fund(
    grant=grant,
    source_vault="1111...user-vault-address...",
    outer_amount=25_000,
    continuation_amount=50_000,
    key=user_key,
    resolved_addresses=(outer_address, slot_address),
)
```

`fund` uses one native `SystemVault.transferBatch` through
`VaultAPI.transfer_batch_ensure`. The client rejects non-positive values,
values or totals outside the signed 64-bit vault domain, and duplicate
destinations before signing. The contract independently validates the complete
vector, proves the source covers the sum, and splits the source once before
creating or crediting either destination. A
rejected batch preserves both balances and destination-vault existence. The
operation moves existing custody; it cannot mint, duplicate, or publish either
unforgeable name as a first-class Rholang capability. Applications must confirm
the batch result and terminal deploy state before advertising the grant as
funded.

Wallets and grant purses are persistent destinations. The user may refill the
same wallet from another authenticated vault and top up the same outer and
continuation purses before the one-shot lollipop is activated:

```python
from f1r3fly.vault import VaultAPI

vaults = VaultAPI(validator_client, shard_id="root")
wallet_refill_id = vaults.transfer_ensure(
    source_vault,
    user_key.get_public_key().get_vault_address(),
    25_000,
    source_key,
)
slot_top_up_id = validator_slots.fund(
    grant,
    user_key.get_public_key().get_vault_address(),
    outer_amount=5_000,
    continuation_amount=10_000,
    key=user_key,
    resolved_addresses=(outer_address, slot_address),
)
```

A refill moves existing REV; it does not replace the destination or mint value.
A read-only client resolves the pair at the finalized installation root, and
the validator client receives that immutable pair through `resolved_addresses`
when it submits the state-changing batch. Validators do not accept exploratory
queries.
A transfer finalized concurrently with another admission cannot expand that
admission's already committed certificate. `FundingSlotGrant` installs a
one-shot activation scaffold: the authorized call creates and consumes one
lollipop instance. Depositing after it fires does not recreate that instance
and can strand funds unless the contract installed a reusable or recovery
branch beforehand. Deposit history never grants withdrawal authority; the
retained unforgeable capabilities do.

The gateway triggers the retained continuation with its own deploy signature:

```python
trigger_id = validator_slots.trigger(
    grant,
    gateway_key,
    request_source='("run-42", 7)',
)
```

An unauthorized deploy cannot instantiate or signal the private lollipop. It
pays any cost of its own failed attempt from its envelope authority and cannot
debit either grant purse. If authenticated outer or continuation custody is
insufficient, admission rejects before the continuation or any matched datum is
consumed.

## Authority presentations in deploy signatures

`F1r3flyClient.deploy` and `deploy_with_vabn_filled` accept
`authority_presentations`. The list is cloned, deterministically serialized,
strictly ordered, deduplicated, and included in the signed deploy preimage.
Unresolved bound levels, false units, empty signatures, and malformed compound
signatures are rejected before signing.

```python
from f1r3fly.cost_accounting import ground_authority_presentation

deploy_id = validator_client.deploy_with_vabn_filled(
    user_key,
    '@"job"!(42)',
    shard_id="root",
    authority_presentations=[ground_authority_presentation(b"payer")],
)
```

An authority presentation is evidence supplied to normalization; it is not a
balance and does not itself create custody. Embedded quoted or named
presentations must already be in the node's canonical normalized form. The node
performs the final canonicality and pre-state checks.

## Terminal deploy status

Track a deploy with `deploy_finalization_status`, not only the block hash that
first included it. Multi-parent merge may finalize a block while rejecting one
of its deploy effects. `wait_for_deploy_finalized` treats `Finalized`, `Failed`,
and `Expired` as distinct terminal states and reports the rejection count.

```python
from f1r3fly.polling import wait_for_deploy_finalized

status = wait_for_deploy_finalized(client, trigger_id, timeout=120)
```

Applications must not execute an off-chain side effect merely because
`find_deploy` returned an inclusion block. Wait for the canonical deploy state.

## Reading cost-authority evidence

The public gRPC `DeployInfo` returned by `F1r3flyClient.show_block()` carries the
protocol-v8 funding certificate, cost witness, adjacent state roots, and
admission status. `CostAuthorityEvidence.from_processed_deploy` accepts that
message as well as the node's internal `ProcessedDeployProto` shape and
constructs immutable typed views. It rejects:

- unsupported protocol versions or non-32-byte consensus identities;
- unordered, duplicate, or zero resource entries;
- malformed demand-kind metadata;
- a witness whose certificate identifier does not equal the independently
  recomputed domain-separated certificate hash;
- an unsupported or mismatched byte-cost schedule version or digest;
- pre-state or post-state root disagreement;
- duplicate events or realized totals different from the event fold;
- zero, unknown, unordered, conflicting, overflowing, or incorrectly totaled
  byte events;
- physical draws that do not correspond one-for-one with causal events;
- stack draws beyond certified and causally born availability; and
- realized, byte, or settled authority beyond its certified bound and
  allocation.

```python
from f1r3fly.cost_accounting import CostAuthorityEvidence
from f1r3fly.deploy import find_deploy_in_block

block = client.show_block(finalized_trigger_block_hash)
deploy_info = find_deploy_in_block(block, trigger_id)
evidence = CostAuthorityEvidence.from_processed_deploy(deploy_info)
print(evidence.certificate.protocol_version)
print(evidence.realized)
print(evidence.settlement)
print(evidence.byte_cost)
print(evidence.byte_settlement)
print(evidence.witness.events)
print(evidence.witness.byte_events)
```

`realized` is the logical authority charged by causal events. `settlement` is
the physical balance debit after stack cells and compound signatures have been
resolved, so applications must not assume that the two maps have identical
keys or values. The client does require the settlement map to equal the sum of
the presented physical balance draws.

`byte_cost` is the checked sum of the canonical produce-introduction,
consume-introduction, and COMM byte events. `byte_settlement` is the
authority-local vault debit selected for those events. The client requires the
schedule identity to match protocol v8, the events to be canonically ordered,
the cost to remain within the certified maximum, and settlement to remain
within the separately certified byte allocation. The byte schedule and
allocation are part of the certificate identifier, so changing any one of
them invalidates the witness binding.

The Python check is a fail-fast application integrity check, not an alternative
consensus implementation. In particular, the node remains authoritative for
canonical Rholang sorting, signature-lane reflection, proof checking, physical
allocation, and state-root replay.

## Exchange and capability registry

`ExchangeAPI` invokes the blessed `rho:lang:exchange` contract. It swaps two
already-existing carrier values only when both inputs are present. It neither
credits a native authority purse nor provides one-sided minting.

`CapabilityAPI` registers and invokes bounded transformers in
`rho:system:capabilities`. A registration contains source and destination
signatures, a bundled transformer, and a non-negative use bound. Capability
connectives such as lollipop and bang are type/authorization operators; they are
not silently accepted as direct funding signatures.

## Schema regeneration and compatibility

The checked-in `protobuf/*.proto` files are byte-identical to the authoritative
node schemas. Regenerate Python modules and type stubs with:

```bash
./update-generated
python -m mypy f1r3fly
isort --check-only f1r3fly
python -m pytest f1r3fly/tests
```

`update-generated` is deterministic and portable across GNU and BSD `sed`.
Changing any consensus field, field number, canonical preimage, authority
protocol version, or hash domain requires matching node and client golden
vectors plus an integration-suite pin update.

## Security checklist

- Keep administrator, user, and gateway private keys separate.
- Publish only the two deposit addresses at the application layer; transfer
  either first-class unforgeable capability solely through the contract path
  that must draw it.
- Treat continuation source as administrator-controlled code. Do not interpolate
  untrusted source text into `continuation_body` or `request_source`.
- Confirm vault-transfer results and canonical deploy terminal states.
- Reject evidence on any structural or root mismatch; never coerce malformed
  protobufs into partial application state.
- Never infer authority from a public address, deploy inclusion, or a candidate
  stack created by the same deploy.

## References

1. J.-Y. Girard, “Linear Logic,” *Theoretical Computer Science* 50 (1987),
   1–101. [doi:10.1016/0304-3975(87)90045-4](https://doi.org/10.1016/0304-3975(87)90045-4).
2. L. G. Meredith and M. Radestock, “A Reflective Higher-order Calculus,”
   *Electronic Notes in Theoretical Computer Science* 141(5) (2005), 49–67.
   [doi:10.1016/j.entcs.2005.05.016](https://doi.org/10.1016/j.entcs.2005.05.016).
