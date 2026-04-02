import json

import click

from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey, PublicKey, generate_vault_addr_from_eth
from f1r3fly.pb.CasperMessage_pb2 import DeployDataProto
from f1r3fly.util import create_deploy_data


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
def get_vault_addr(ctx: click.core.Context, input_type: str, input: str) -> None:
    if input_type == 'eth':
        if input.startswith("0x"):
            input = input[2:]
        addr = generate_vault_addr_from_eth(input)
    elif input_type == 'public':
        pub = PublicKey.from_hex(input)
        addr = pub.get_vault_address()
    elif input_type == 'private':
        private = PrivateKey.from_hex(input)
        addr  = private.get_public_key().get_vault_address()
    else:
        raise NotImplementedError("Not supported type {}".format(input_type))

    if ctx.obj['json_output']:
        click.echo(json.dumps({"vaultAddress": addr}))
    else:
        click.echo("Vault Address is : {}".format(addr))


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
def sign_deploy(ctx: click.core.Context, private_key: str, term: str, phlo_price: int, phlo_limit: int, valid_after_block_number: int,
                timestamp: int, sig_algorithm: str) -> None:
    pri = PrivateKey.from_hex(private_key)
    signed_deploy = create_deploy_data(
        pri, term, phlo_price, phlo_limit, valid_after_block_number, timestamp
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
def submit_deploy(ctx: click.core.Context, deployer: str, term: str, phlo_price: int, phlo_limit: int, valid_after_block_number: int,
                  timestamp: int, sig_algorithm: str, sig: str, host: str,
                  port: int) -> None:
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
    with F1r3flyClient(host, port) as client:
        ret = client.send_deploy(deploy)

    if ctx.obj["json_output"]:
        click.echo(json.dumps({"deployID": ret}))
    else:
        click.echo("Send {} deploy succeeded".format(sig))

@cli.command()
@click.option('--host', default='localhost', help='Node host')
@click.option('--ports', default='40401,40411,40421,40431,40451',
              help='Comma-separated gRPC ports to check')
@click.option('--names', default='bootstrap,validator1,validator2,validator3,readonly',
              help='Comma-separated node names (parallel to --ports)')
@click.pass_context
def status(ctx: click.core.Context, host: str, ports: str, names: str) -> None:
    """Check LFB and sync status of all nodes."""
    port_list = [int(p) for p in ports.split(',')]
    name_list = names.split(',')
    results = []
    for name, port in zip(name_list, port_list):
        try:
            with F1r3flyClient(host, port) as client:
                lfb = client.last_finalized_block()
                lfb_num = lfb.blockInfo.blockNumber
                lfb_hash = lfb.blockInfo.blockHash[:16]
                results.append({"node": name, "port": port, "lfb": lfb_num, "lfb_hash": lfb_hash, "status": "ok"})
        except Exception as e:
            results.append({"node": name, "port": port, "lfb": -1, "lfb_hash": "", "status": str(e)[:80]})

    if ctx.obj['json_output']:
        click.echo(json.dumps(results, indent=2))
    else:
        lfb_values = [r["lfb"] for r in results if r["status"] == "ok"]
        synced = len(set(lfb_values)) <= 1 and len(lfb_values) == len(results)
        for r in results:
            if r["status"] == "ok":
                click.echo(f"  {r['node']:12s} LFB=#{r['lfb']} ({r['lfb_hash']})")
            else:
                click.echo(f"  {r['node']:12s} ERROR: {r['status']}")
        if synced:
            click.echo(f"All {len(results)} nodes synced at LFB #{lfb_values[0]}")
        elif lfb_values:
            click.echo(f"Nodes out of sync: LFB range #{min(lfb_values)}-#{max(lfb_values)}")


if __name__ == '__main__':
    cli()
