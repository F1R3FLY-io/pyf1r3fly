# pyrchain

Interface to RChain RNode RPC.

## Install 

Pyrchain is Python 3 library for interfacing with RChain RNode gRPC API. The
library is distributed via PyPI (https://pypi.org/project/pyrchain/). You can
install it for current user by running:

	pip3 install -U pyrchain

See `pyproject.toml` for information about 3rd party library dependencies.


## Examples

The features below are provided in pyrchain.

1. [generate private key and public key](./examples/keys_example.py)
2. [sign a deploy with the private key](./examples/sign_verify_examples.py)
3. [use grpc api to interact with rnode](./examples/grpc_api_example.py)
4. [Vault Api of rchain to do transfer and check balance](./examples/vault_example.py)     

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

Note: Some examples require a running RNode instance to connect to.

### Updating Protocol Buffers

To update protocol buffers from upstream run:

    ./update-protobufs
    ./update-generated

The first command fetches latest RChain `*.proto` files from `dev` branch
into `./protobuf` directory. The second command generates gRPC Python code
into `rchain.pb` package (`./rchain/pb`).

### Running Tests and Linting

    python -m pytest rchain/tests
    python -m mypy rchain
    isort --recursive --check-only rchain
