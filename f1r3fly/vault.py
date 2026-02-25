import string
import time
from typing import Mapping

from .client import F1r3flyClient
from .crypto import PrivateKey

CREATE_VAULT_RHO_TPL = """
new rl(`rho:registry:lookup`), SystemVaultCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreateVault", "$addr", Nil)
  }
}
"""

GET_BALANCE_RHO_TPL = """
new return, rl(`rho:registry:lookup`), SystemVaultCh, vaultCh, balanceCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreate", "$addr", *vaultCh) |
    for (@(true, vault) <- vaultCh) {
      @vault!("balance", *balanceCh) |
      for (@balance <- balanceCh) {
        return!(balance)
      }
    }
  }
}
"""

TRANSFER_RHO_TPL = """
new rl(`rho:registry:lookup`), SystemVaultCh, vaultCh, authKeyCh, deployerId(`rho:system:deployerId`), stdout(`rho:io:stdout`), resultCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreate", "$from", *vaultCh) |
    @SystemVault!("deployerAuthKey", *deployerId, *authKeyCh) |
    for (@(true, vault) <- vaultCh; key <- authKeyCh) {
      @vault!("transfer", "$to", $amount, *key, *resultCh) |
      for (_ <- resultCh) { Nil }
    }
  }
}
"""

TRANSFER_ENSURE_TO_RHO_TPL = """
new rl(`rho:registry:lookup`), SystemVaultCh, vaultCh, toVaultCh, deployerId(`rho:system:deployerId`), authKeyCh, resultCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreate", "$from", *vaultCh) |
    @SystemVault!("findOrCreate", "$to", *toVaultCh) |
    @SystemVault!("deployerAuthKey", *deployerId, *authKeyCh) |
    for (@(true, vault) <- vaultCh; key <- authKeyCh; @(true, toVault) <- toVaultCh) {
      @vault!("transfer", "$to", $amount, *key, *resultCh) |
      for (_ <- resultCh) { Nil }
    }
  }
}
"""

# these are predefined param
TRANSFER_PHLO_LIMIT = 1000000
TRANSFER_PHLO_PRICE = 1


def render_contract_template(template: str, substitutions: Mapping[str, str]) -> str:
    return string.Template(template).substitute(substitutions)


class VaultAPI:

    def __init__(self, client: F1r3flyClient):
        self.client = client

    def get_balance(self, vault_addr: str, block_hash: str='') -> int:
        contract = render_contract_template(
            GET_BALANCE_RHO_TPL,
            {'addr': vault_addr},
        )
        result = self.client.exploratory_deploy(contract, block_hash)
        return int(result[0].exprs[0].g_int)

    def transfer(self, from_addr: str, to_addr: str, amount: int, key: PrivateKey, phlo_price:int=TRANSFER_PHLO_PRICE,
                 phlo_limit:int=TRANSFER_PHLO_LIMIT) -> str:
        """
        Transfer from `from_addr` to `to_addr` in the chain. Just make sure the `to_addr` is created
        in the chain. Otherwise, the transfer would hang until the `to_addr` is created.
        """
        contract = render_contract_template(
            TRANSFER_RHO_TPL, {
                'from': from_addr,
                'to': to_addr,
                'amount': str(amount)
            }
        )
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(key, contract, phlo_price, phlo_limit,
                                                   timestamp_mill)

    def transfer_ensure(self, from_addr: str, to_addr: str, amount: int, key: PrivateKey,
                        phlo_price:int=TRANSFER_PHLO_PRICE,
                        phlo_limit:int=TRANSFER_PHLO_LIMIT) -> str:
        """
        The difference between `transfer_ensure` and `transfer` is that , if the to_addr is not created in the
        chain, the `transfer` would hang until the to_addr successfully created in the change and the `transfer_ensure`
        can be sure that if the `to_addr` is not existed in the chain the process would created the vault in the chain
        and make the transfer successfully.
        """
        contract = render_contract_template(
            TRANSFER_ENSURE_TO_RHO_TPL, {
                'from': from_addr,
                'to': to_addr,
                'amount': str(amount)
            }
        )
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(key, contract, phlo_price, phlo_limit,
                                                   timestamp_mill)

    def create_vault(self, addr: str, key: PrivateKey, phlo_price:int=TRANSFER_PHLO_PRICE, phlo_limit:int=TRANSFER_PHLO_LIMIT) -> str:
        contract = render_contract_template(
            CREATE_VAULT_RHO_TPL, {
                'addr': addr
            }
        )
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(key, contract, phlo_price, phlo_limit,
                                                   timestamp_millis=timestamp_mill)
