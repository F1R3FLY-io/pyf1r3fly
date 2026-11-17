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

## Deploy admission failures

For node [#789](https://github.com/F1R3FLY-io/f1r3node-rust/pull/789),
`send_deploy` raises `DeployAdmissionRefused` only for an explicit gRPC service
error with one of these leading codes. Its `code` distinguishes the reasons,
`deploy_id` retains the signed ID and `retryable` says whether the same signed
deploy can be sent again. Other service errors keep `F1r3flyClientException`.

| Code | `retryable` |
|---|---|
| `deploy_pool_busy`, `deploy_admission_busy` | Yes |
| `invalid_deploy_window` | Yes, once the chain reaches the validity height |
| `deploy_too_large`, `deploy_expired` | No, build a new deploy |

A service error saying `Deploy already known` for this signature means the
node already holds the deploy, so `send_deploy` returns its ID.

A [deploy queue (#990)](https://github.com/F1R3FLY-io/f1r3node-rust/pull/990)
can serve a validator's client ports. It answers the node's own refusals as
above. Its capacity and availability refusals are gRPC statuses, which
`send_deploy` also raises as `DeployAdmissionRefused`, with `retry_after` in
seconds from the `retry-after` metadata. The queue did not hold the deploy.

| gRPC status | `code` | `congestion` | `retryable` |
|---|---|---|---|
| `RESOURCE_EXHAUSTED` | `deploy_queue_full` | `validator`, `validator_idle`, `shard`, `storage` | No: after `retry_after`, sign a new deploy with a current `valid_after_block_number`; for `validator_idle`, send it to another validator |
| `RESOURCE_EXHAUSTED` | `deploy_queue_full` | `deployer_share` | Yes |
| `UNAVAILABLE` | `node_unavailable`, `deploy_queue_stopped` | none | Yes |

A queue reports `DEPLOY_STATE_REJECTED` (value 5) for a deploy the node refused
when the queue released it; `wait_for_deploy_finalized` raises `DeployError`
for it instead of waiting until its timeout.

RPC failures (including deadlines and unavailability) raise
`DeploySubmissionUnknown`, with the original `deploy_id` and chained RPC error.
They do not establish that the deploy was rejected. Reconcile that ID before
creating a new payment. No automatic retry or new signature is added; callers
that retry should retain the original signed proto and pass it to `send_deploy`.
