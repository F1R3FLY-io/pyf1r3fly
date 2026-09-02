import time

import grpc

from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey
from f1r3fly.util import create_deploy_data
from f1r3fly.vault import TRANSFER_RHO_TPL, render_contract_template

a = PrivateKey.generate()
b = PrivateKey.generate()

# get the latest block number
with F1r3flyClient('localhost', 40401) as client:
    # get the latest 10 block in the f1r3node
    latest_blocks = client.show_blocks(depth=1)
    latest_block = latest_blocks[0]
    latest_block_num = latest_block.blockNumber

# sign the transfer deploy
from_addr = a.get_public_key().get_vault_address()
to_addr = b.get_public_key().get_vault_address()
amount = 10000
contract = render_contract_template(
    TRANSFER_RHO_TPL, {
        'from': from_addr,
        'to': to_addr,
        'amount': str(amount)
    }
)
timestamp_mill = int(time.time() * 1000)
# this would create the protobuf needs to be signed and sign the protobuf and return protobuf back
deploy = create_deploy_data(
    a, contract, latest_block_num, timestamp_mill, "root",
)
# deploy.deployId is the identifier used to fetch the containing block


with F1r3flyClient('localhost', 40401) as client:
    # send the signed deploy to the network
    resp = client.send_deploy(deploy)
