import json
from typing import Any, Dict, List, Optional

import click

from f1r3fly.client import RClient
from f1r3fly.crypto import PrivateKey, PublicKey, generate_rev_addr_from_eth
from f1r3fly.pb.CasperMessage_pb2 import DeployDataProto, DeployParameter, RholangValue
from f1r3fly.util import create_deploy_data


def parse_parameters(params_json: Optional[str]) -> Optional[List[DeployParameter]]:
    """Parse JSON string into list of DeployParameter objects.

    JSON format: [{"name": "paramName", "value": <value>, "type": "string|int|bool|bytes"}]

    For bytes type, value should be hex-encoded string.

    Args:
        params_json: JSON string containing parameter definitions.

    Returns:
        List of DeployParameter objects, or None if no params provided.
    """
    if not params_json:
        return None

    params_list: List[Dict[str, Any]] = json.loads(params_json)
    deploy_params: List[DeployParameter] = []

    for param in params_list:
        name = param['name']
        value = param['value']
        value_type = param.get('type', 'string')  # default to string

        rho_value = RholangValue()
        if value_type == 'bool':
            rho_value.bool_value = bool(value)
        elif value_type == 'int':
            rho_value.int_value = int(value)
        elif value_type == 'string':
            rho_value.string_value = str(value)
        elif value_type == 'bytes':
            # Expect hex-encoded string for bytes
            rho_value.bytes_value = bytes.fromhex(value)
        else:
            raise ValueError(f"Unknown parameter type: {value_type}")

        deploy_params.append(DeployParameter(name=name, value=rho_value))

    return deploy_params if deploy_params else None


@click.group()
@click.option('--json-output', default=False, is_flag=True)
@click.pass_context
def cli(ctx: click.core.Context, json_output: bool) -> None:
    ctx.ensure_object(dict)
    ctx.obj['json_output'] = json_output

@cli.command()
@click.pass_context
@click.option('--input-type', type=click.Choice(['eth', 'public', 'private'], case_sensitive=False),
              help='the kind of the input you are going to provide.')
@click.option('--input', help='the concrete content of your input type')
def get_rev_addr(ctx: click.core.Context, input_type: str, input: str) -> None:
    if input_type == 'eth':
        if input.startswith("0x"):
            input = input[2:]
        addr = generate_rev_addr_from_eth(input)
    elif input_type == 'public':
        pub = PublicKey.from_hex(input)
        addr = pub.get_rev_address()
    elif input_type == 'private':
        private = PrivateKey.from_hex(input)
        addr  = private.get_public_key().get_rev_address()
    else:
        raise NotImplementedError("Not supported type {}".format(input_type))

    if ctx.obj['json_output']:
        click.echo(json.dumps({"revAddress": addr}))
    else:
        click.echo("Rev Address is : {}".format(addr))


@cli.command()
@click.pass_context
@click.option('--private-key', help='the private key hex string is used to sign')
@click.option('--term', help='the rholang term')
@click.option('--phlo-price', type=int, help='phlo price')
@click.option('--phlo-limit', type=int, help='phlo limit')
@click.option('--valid-after-block-number', type=int,
              help='valid after block number, usually used the latest block number')
@click.option('--timestamp', type=int, help='timestamp, unit millisecond')
@click.option('--sig-algorithm', type=click.Choice(['secp256k1']),
              help='signature algorithm. Currently only support secp256k1')
@click.option('--parameters', default=None,
              help='JSON string of deploy parameters: [{"name": "...", "value": ..., "type": "string|int|bool|bytes"}]')
def sign_deploy(ctx: click.core.Context, private_key: str, term: str, phlo_price: int, phlo_limit: int, valid_after_block_number: int,
                timestamp: int, sig_algorithm: str, parameters: Optional[str]) -> None:
    pri = PrivateKey.from_hex(private_key)
    deploy_params = parse_parameters(parameters)
    signed_deploy = create_deploy_data(
        pri, term, phlo_price, phlo_limit, valid_after_block_number, timestamp, deploy_params
    )
    deploy_id = signed_deploy.sig.hex()

    if ctx.obj['json_output']:
        click.echo(json.dumps({"signature": deploy_id}))
    else:
        click.echo("The deploy signature is : {}".format(deploy_id))


@cli.command()
@click.pass_context
@click.option('--deployer', help='the public key hex string is used to sign')
@click.option('--term', help='the rholang term')
@click.option('--phlo-price', type=int, help='phlo price')
@click.option('--phlo-limit', type=int, help='phlo limit')
@click.option('--valid-after-block-number', type=int,
              help='valid after block number, usually used the latest block number')
@click.option('--timestamp', type=int, help='timestamp, unit millisecond')
@click.option('--sig-algorithm', type=click.Choice(['secp256k1']),
              help='signature algorithm. Currently only support secp256k1')  # not used actually
@click.option('--sig', help='the signature of the deploy')
@click.option('--host', help='validator host the deploy is going to send to')
@click.option('--port', type=int, help='validator grpc port the deploy is going to send to')
@click.option('--parameters', default=None,
              help='JSON string of deploy parameters: [{"name": "...", "value": ..., "type": "string|int|bool|bytes"}]')
def submit_deploy(ctx: click.core.Context, deployer: str, term: str, phlo_price: int, phlo_limit: int, valid_after_block_number: int,
                  timestamp: int, sig_algorithm: str, sig: str, host: str,
                  port: int, parameters: Optional[str]) -> None:
    deploy_params = parse_parameters(parameters)
    deploy = DeployDataProto(
        deployer=bytes.fromhex(deployer),
        term=term,
        phloPrice=phlo_price,
        phloLimit=phlo_limit,
        validAfterBlockNumber=valid_after_block_number,
        timestamp=timestamp,
        sigAlgorithm='secp256k1',
        sig=bytes.fromhex(sig)
    )
    if deploy_params:
        for param in deploy_params:
            deploy.parameters.append(param)
    with RClient(host, port) as client:
        ret = client.send_deploy(deploy)

    if ctx.obj["json_output"]:
        click.echo(json.dumps({"deployID": ret}))
    else:
        click.echo("Send {} deploy succeeded".format(sig))

if __name__ == '__main__':
    cli()
