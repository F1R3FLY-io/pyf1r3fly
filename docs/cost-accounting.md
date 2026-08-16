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
holding the unforgeable name can locate, draw, or delegate its purse.

A **funding certificate** binds a protocol version, normalized program hash,
pre-state root, finite demand, authenticated allocation, stack reservations,
fee allocation, and reservation identity. A **cost witness** binds that
certificate to the causal event sequence, realized authority, physical draws,
born stacks, settlement, and post-state root. Independent validators recompute
and verify both objects during replay.

For every authority lane `s`, admission and settlement preserve:

```math
\kappa_s \leq B_s \leq \Sigma_s
```

where $`\Sigma_s`$ is authenticated pre-state custody, $`B_s`$ is the certified
finite maximum, and $`\kappa_s`$ is realized cost. The post-state relation is:

```math
\Sigma'_s = \Sigma_s - \kappa_s - \operatorname{fee}_s
```

The unused portion $`B_s-\kappa_s`$ never leaves custody, so there is no refund
mint and no transient consensus reservation cell.

## End-to-end workflow

![Sequence diagram showing an administrator installing a persistent unforgeable funding slot, a user depositing from canonical wallet custody, an authenticated gateway triggering the located lollipop continuation, validators rejecting before mutation when custody is insufficient or atomically executing and settling when sufficient, and the Python client structurally validating the returned consensus evidence.](diagrams/wallet-funded-slot-flow.svg)

(*Source: [`diagrams/wallet-funded-slot-flow.puml`](diagrams/wallet-funded-slot-flow.puml). Render with `plantuml -tsvg docs/diagrams/wallet-funded-slot-flow.puml`.*)

The workflow deliberately separates three authorities:

1. The administrator chooses the continuation and the gateway public key.
2. The user controls a wallet vault and chooses how much to deposit.
3. The gateway can trigger only the retained slot capability; it does not gain
   the user's wallet key or general wallet withdrawal authority.

This is the application pattern for user-funded Embers agents. Each grant owns
its own unforgeable slot, so one user or agent cannot consume another grant's
custody.

## Creating and funding a grant

`FundingSlotGrant.install_term` creates an unforgeable `slot`, derives its
SystemVault address, and persists a linearly funded continuation with the Rholang
lollipop form `entry -o slot`. The public trigger accepts a request only when
`rho:system:deployerId:ops` resolves the submitting deployer to the configured
uncompressed secp256k1 public key.

```python
from f1r3fly.client import F1r3flyClient
from f1r3fly.cost_accounting import FundingSlotAPI, FundingSlotGrant
from f1r3fly.crypto import PrivateKey

client = F1r3flyClient("validator.example", 40401)
administrator_key = PrivateKey.generate()
gateway_key = PrivateKey.generate()
user_key = PrivateKey.generate()

grant = FundingSlotGrant(
    trigger_channel="embers:grant:7:trigger",
    slot_address_channel="embers:grant:7:address",
    completion_channel="embers:grant:7:complete",
    gateway_public_key=gateway_key.get_public_key().to_bytes(),
)
slots = FundingSlotAPI(client, shard_id="root")
install_id = slots.install(
    grant,
    'new result in { result!("agent completed") }',
    administrator_key,
)
```

Wait for `install_id` to reach terminal `Finalized` state before reading the
address. The wallet owner may then transfer existing native custody to that
address:

```python
slot_address = slots.slot_address(grant)
fund_id = slots.fund_address(
    slot_address=slot_address,
    source_vault="1111...user-vault-address...",
    amount=50_000,
    key=user_key,
)
```

`fund_address` uses `SystemVault.transfer` through `VaultAPI.transfer_ensure`.
The operation debits one canonical vault and credits another; it cannot mint,
duplicate, or expose the unforgeable slot. Applications must confirm the
transfer result and terminal deploy state before advertising the grant as
funded.

The gateway triggers the retained continuation with its own deploy signature:

```python
trigger_id = slots.trigger(
    grant,
    gateway_key,
    request_source='("run-42", 7)',
)
```

An unauthorized deploy cannot enter the private `entry` channel. It pays any
cost of its own failed attempt from its envelope authority and cannot debit the
slot. If authenticated slot custody is insufficient, admission rejects before
the continuation or any matched datum is consumed.

## Authority presentations in deploy signatures

`F1r3flyClient.deploy` and `deploy_with_vabn_filled` accept
`authority_presentations`. The list is cloned, deterministically serialized,
strictly ordered, deduplicated, and included in the signed deploy preimage.
Unresolved bound levels, false units, empty signatures, and malformed compound
signatures are rejected before signing.

```python
from f1r3fly.cost_accounting import ground_authority_presentation

deploy_id = client.deploy_with_vabn_filled(
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

`CostAuthorityEvidence.from_processed_deploy` constructs immutable typed views
of all funding-certificate and witness fields. It rejects:

- unsupported protocol versions or non-32-byte consensus identities;
- unordered, duplicate, or zero resource entries;
- malformed demand-kind metadata;
- a witness whose certificate identifier does not equal the independently
  recomputed domain-separated certificate hash;
- pre-state or post-state root disagreement;
- duplicate events or realized totals different from the event fold;
- physical draws that do not correspond one-for-one with causal events;
- stack draws beyond certified and causally born availability; and
- realized or settled authority beyond its certified bound.

```python
from f1r3fly.cost_accounting import CostAuthorityEvidence

evidence = CostAuthorityEvidence.from_processed_deploy(processed_deploy)
print(evidence.certificate.protocol_version)
print(evidence.realized)
print(evidence.settlement)
print(evidence.witness.events)
```

`realized` is the logical authority charged by causal events. `settlement` is
the physical balance debit after stack cells and compound signatures have been
resolved, so applications must not assume that the two maps have identical
keys or values. The client does require the settlement map to equal the sum of
the presented physical balance draws.

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
- Publish only the slot address; disclose the unforgeable capability solely to
  the contract and gateway path that must draw it.
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
