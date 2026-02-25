from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey
from f1r3fly.pb.RhoTypes_pb2 import GPrivate, GUnforgeable, Par
from f1r3fly.vault import VaultAPI

TESTNET_SERVER = []  # TODO: populate when f1r3fly testnet servers are available
TESTNET_READONLY = []  # TODO: populate when f1r3fly testnet read-only servers are available

MAINET_SERVER = []  # TODO: populate when f1r3fly mainnet servers are available

READONLY_SERVER = []  # TODO: populate when f1r3fly read-only servers are available

with F1r3flyClient(TESTNET_READONLY[0], 40401) as client:
    from f1r3fly.param import testnet_param

    # these param are fixed when the network starts on the genesis
    # the param will never change except hard-fork
    # but different network has different param based on the genesis block
    client.install_param(testnet_param)
    block_hash ='8012e93f480d561045f1046d74f8cb7c31a96206e49dbdf15b22a636e18a4693'
    testnet_transactions = client.get_transaction(block_hash)

with F1r3flyClient(READONLY_SERVER[0], 40401) as client:
    from f1r3fly.param import mainnet_param

    # these param are fixed when the network starts on the genesis
    # the param will never change except hard-fork
    # but different network has different param based on the genesis block
    client.install_param(mainnet_param)
    block_hash ='fe5ceeec3cc5e3d909ef1a688ce2a6c416a474870b13bb9ed96252043593ba5d'

    # only after install_param, the client can get the judge if
    # the transaction happened in the deploy otherwise, it would throw ValueError
    mainnet_transactions = client.get_transaction(block_hash)
