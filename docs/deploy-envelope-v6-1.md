# Deploy envelope v6.1

Protocol v6.1 separates a deploy's consensus identity from any individual
signature. A **deploy ID** is the 32-byte Blake2b-256 commitment to the deploy
intent, authorization policy, and signer-presence bitmap. A **principal** is a
signature-scheme identifier paired with a canonical public key. A **witness**
is a selected principal's signature over the scheme-bound deploy ID.

This separation matters for threshold authorization: every validator derives
the same deploy ID even though a policy can list unselected members and can
carry multiple signatures. APIs, tuple-space deploy names, finalization
queries, and occurrence storage use the deploy ID. A witness signature is
authorization evidence and is never a deploy identifier.

## Single-signer workflow

`create_deploy_data` constructs the complete envelope. The shard ID must be
nonempty, the valid-after block and timestamp must be nonnegative, and the
language is exactly `rholang`.

```python
from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey
from f1r3fly.util import create_deploy_data

key = PrivateKey.from_hex("01" * 32)
deploy = create_deploy_data(
    key,
    'new deployId(`rho:system:deployId`) in { deployId!(42) }',
    valid_after_block_no=12,
    timestamp_millis=1_800_000_000_000,
    shard_id="root",
)

deploy_id = deploy.deployId.hex()
with F1r3flyClient("localhost", 40401) as client:
    assert client.send_deploy(deploy) == deploy_id
```

The client sends the exact `DeployDataProto`. It does not reconstruct a legacy
`deployer`/`sig` payload, and `send_deploy` returns `deploy.deployId`.

## Threshold workflow

`DeploySigner.private_key` selects that policy member. A member without a
private key remains in the policy but has no witness. Members are sorted by
their canonical principal encoding before the bitmap and signatures are
created.

```python
from f1r3fly.crypto import PrivateKey
from f1r3fly.pb.CasperMessage_pb2 import DeployDataProto
from f1r3fly.util import DeploySigner, authorize_deploy_data

keys = [PrivateKey.from_hex(f"{value:02x}" * 32) for value in (1, 2, 3)]
signers = [
    DeploySigner.selected(keys[0]),
    DeploySigner(keys[1].get_public_key()),
    DeploySigner.selected(keys[2]),
]
intent = DeployDataProto(
    term='new return in { return!("v6.1") }',
    language="rholang",
    timestamp=1_800_000_000_000,
    validAfterBlockNumber=42,
    shardId="root",
    expirationTimestamp=1_800_000_600_000,
)
deploy = authorize_deploy_data(intent, signers, threshold=2)
```

The constructor rejects empty policies, duplicate principals, mismatched
private/public keys, invalid thresholds, and witness selections that do not
satisfy the policy. An omitted threshold encodes `AllOf`; an explicit value
smaller than the member count encodes `Threshold`.

## Offline signing

The command-line workflow remains two-stage. `sign-deploy` emits both the
witness signature and deploy ID. `submit-deploy` reconstructs the same
single-member policy, verifies the supplied signature locally, and submits the
complete v6.1 envelope. Every intent field must be identical in both commands.

```text
canonicalize intent and principals
derive presence bitmap
commit deploy ID from intent, policy, and bitmap
derive scheme-bound signing hash from deploy ID
sign the 32-byte hash with canonical low-S ECDSA
attach the indexed witness and submit
```

The signed intent binds the term, timestamp, valid-after block, shard,
expiration, language selector, and ordered authority presentations. Changing
any field invalidates the witness.

## Wire and cryptographic rules

| Rule | Client behavior |
|---|---|
| Authorization format | Writes `formatVersion = 0x00060001` |
| Active scheme | Uncompressed SEC1 secp256k1, scheme ID `1` |
| Signature | Canonical DER ECDSA with low `S` |
| Policy order | Strict ascending canonical principal bytes |
| Bitmap order | Least-significant bit first by policy member index |
| Deploy ID | Blake2b-256 commitment, exactly 32 bytes |
| Legacy fields | Empty in protocol-v6.1 envelopes |

The immutable cross-language vector is
[`test-vectors/deploy-envelope-v6.1.json`](../test-vectors/deploy-envelope-v6.1.json).
The Python test reconstructs its two-of-three envelope and requires exact
agreement on the intent bytes, deterministic signatures, presence bitmap,
deploy ID, and serialized protobuf.

## Compatibility and failure behavior

Protocol-v6 nodes reject legacy authorization fields, mixed legacy/v6
envelopes, noncanonical signatures, inactive schemes, and mismatched deploy
IDs. Therefore a protocol-v6 client must not fall back to signature identity or
retry with a legacy payload. Nodes running an earlier protocol require their
matching earlier client; cross-protocol inference from byte length is not
permitted.
