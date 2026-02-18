from f1r3fly.client import F1r3flyClient
from f1r3fly.crypto import PrivateKey
from f1r3fly.vault import VaultAPI

MAINNET_SERVER = []  # TODO: populate when f1r3fly mainnet servers are available
READONLY_SERVER = []  # TODO: populate when f1r3fly read-only servers are available

alice = PrivateKey.from_hex('61e594124ca6af84a5468d98b34a4f3431ef39c54c6cf07fe6fbf8b079ef64f6')
bob = PrivateKey.generate()

exploratory_term = 'new return in{return!("a")}'

with F1r3flyClient(READONLY_SERVER[0], 40401) as client:
    vault = VaultAPI(client)
    # get the balance of a vault
    # get balance can only perform in the read-only node
    bob_balance = vault.get_balance(bob.get_public_key().get_vault_address())

with F1r3flyClient(MAINNET_SERVER[0], 40401) as  client:
    # because transfer need a valid deploy
    # the transfer need the private to perform signing
    vault = VaultAPI(client)
    deployId = vault.transfer(alice.get_public_key().get_vault_address(), bob.get_public_key().get_vault_address(), 100000, alice)
