# pyf1r3fly

Interface to F1R3fly Node RPC.

## Install 

pyf1r3fly is a Python 3 library for interfacing with F1R3fly Node gRPC API. You can
install it for current user by running:

	pip3 install -U pyf1r3fly

See `pyproject.toml` for information about 3rd party library dependencies.


## Examples

The features below are provided in pyf1r3fly.

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

Note: Some examples require a running RNode instance to connect to.

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
