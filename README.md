# pyf1r3fly

Python client library for F1R3fly Node RPC.

## Install

pyf1r3fly is a Python 3 library for interfacing with F1R3fly Node gRPC API. You can
install it for current user by running:

	pip3 install -U pyf1r3fly

See `pyproject.toml` for information about 3rd party library dependencies.

## Modules

| Module | Description |
|--------|-------------|
| `client.py` | `F1r3flyClient` -- gRPC client for deploy, block query, data query, deploy finalization status, status, bond status, file upload/download |
| `crypto.py` | `PrivateKey`, `PublicKey` -- SECP256k1 key handling, vault address derivation |
| `par.py` | `par_as_string`, `par_as_int`, `par_as_map`, etc. -- type-safe extraction from Rholang Par protobuf messages |
| `polling.py` | `poll_until`, `deploy_and_read`, `wait_for_finalized`, `wait_for_deploy_finalized`, `deploy_with_fallback` -- polling and deploy workflow utilities |
| `deploy.py` | `check_deploy_succeeded`, `check_deploy_errored`, `find_deploy_in_block` -- deploy result inspection |
| `contracts.py` | `registry_lookup`, `registry_query` -- read-only queries against on-chain contracts via exploratory deploy |
| `vault.py` | `VaultAPI(client, shard_id='root')` -- token transfers and balance queries. Methods: `get_balance` (exploratory deploy, readonly only on Rust node), `deploy_get_balance` (real deploy via `DEPLOY_GET_BALANCE_RHO_TPL`, works on validators), `transfer`, `transfer_ensure`, `read_transfer_result`. All deploy methods use the constructor's `shard_id`. |
| `system_contracts.py` | `query_token_metadata` -- queries for genesis-deployed system contracts |
| `pos.py` | `PosAPI` -- Proof-of-Stake operations: `bond`, `withdraw`, `reveal_random`, `pos_vault_transfer`; reads `get_bonds`, `get_rewards`, `get_withdrawers`, `get_coop_vault`, `get_epoch_length` |
| `websocket.py` | `connect_ws`, `wait_for_events`, `validate_block_event` -- WebSocket `/ws/events` client with event type constants and connection retry |
| `util.py` | `create_deploy_data`, `sign_deploy_data` -- deploy proto construction and signing |

## Quick Start

```python
from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey
from f1r3fly.polling import deploy_and_read
from f1r3fly.par import par_as_int

key = PrivateKey.from_hex("your_private_key_hex")

with F1r3flyClient("localhost", 40401) as client:
    # Deploy and read result in one call
    pars, block_hash, block_number = deploy_and_read(
        client,
        'new deployId(`rho:system:deployId`) in { deployId!(42) }',
        key,
        inclusion_timeout=30,
        finalization_timeout=60,
    )
    value = par_as_int(pars[0])  # 42
```

## Block hash profiles

`gen_block_hash_from_block` and `gen_deploys_hash_from_block` in `f1r3fly.crypto`
accept a keyword-only `scheme`. Existing calls default to `legacy`, preserving
the old flat Blake2b algorithm. For Rust nodes, select the profile of the
actual node build explicitly:

| Scheme | Node hash recipe |
|---|---|
| `legacy` | Historical flat header/body hash; original Python behavior |
| `merkle-v1` | Rust Merkle recipe before node PR #790 |
| `merkle-v1-justifications` | Rust Merkle recipe including #790's justification root |

```python
from f1r3fly.crypto import gen_block_hash_from_block

digest = gen_block_hash_from_block(block, scheme='merkle-v1-justifications')
assert digest == block.blockHash
```

These helpers take a canonical, node-produced `BlockMessageProto`, not the
HTTP/gRPC `BlockInfo` response. Both Merkle profiles use scheme byte 1 and can
have the same `header.version`; neither that field nor the supplied block hash
is used to guess a profile or fall back to another algorithm. A computed hash
does not validate consensus rules or signatures. Merkle hashing rejects unknown
protobuf fields so newer, unsupported schemas cannot silently produce a digest.

The Merkle deploys root includes user deploys followed by system deploys, in
commit order; it uses zero bytes for the empty root and promotes odd tails
unchanged. Bonds, rejected deploys and applied-from-scope signatures are sorted
as in the node; justifications retain their supplied order. The new node
requires strictly ascending justification validators separately during validation.

The implementation and hash-reachable protobuf additions follow
[node #790 at `277e83a2c48e`](https://github.com/F1R3FLY-io/f1r3node-rust/blob/277e83a2c48e2c3bc884588cc3e756744f170eff/casper/src/rust/util/proto_util.rs).
They include merge metadata, rejection provenance, slash activation epochs and
deploy expiration/parameters. This is a scoped schema update for hashing;
deploy signing and submission behavior are unchanged. Tests pin the Rust empty
block golden hashes before and after #790. Network/state migration remains a
separate node deployment concern.

## Examples

1. [generate private key and public key](https://github.com/F1R3FLY-io/pyf1r3fly/blob/main/examples/keys_example.py)
2. [sign a deploy with the private key](https://github.com/F1R3FLY-io/pyf1r3fly/blob/main/examples/sign_verify_examples.py)
3. [use grpc api to interact with f1r3node](https://github.com/F1R3FLY-io/pyf1r3fly/blob/main/examples/grpc_api_example.py)
4. [Vault Api to do transfer and check balance](https://github.com/F1R3FLY-io/pyf1r3fly/blob/main/examples/vault_example.py)

## Development

### Local Development Setup

1. Clone the repository and navigate to the project directory

2. Create and activate a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. Install the package with development dependencies:
    ```bash
    pip install -e ".[dev]"
    ```

### Running Examples

After installing the package, you can run the examples:

```bash
python examples/keys_example.py
python examples/grpc_api_example.py
python examples/vault_example.py
```

Note: Some examples require a running F1R3fly Node instance to connect to.

### Updating Protocol Buffers

To update protocol buffers from upstream run:

    ./update-protobufs
    ./update-generated

This first command will fetch latest F1R3fly `*.proto` files from `main` branch
into `./protobuf` directory. The second command will generate gRPC Python code
corresponding to the protcol buffers into `f1r3fly.pb` package (`./f1r3fly/pb`).

### Running Tests and Linting

	python -m pytest f1r3fly/tests
    python -m mypy f1r3fly
    isort --recursive --check-only f1r3fly
