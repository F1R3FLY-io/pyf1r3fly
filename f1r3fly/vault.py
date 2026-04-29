import dataclasses
import string
import time
from typing import Mapping

from .client import F1r3flyClient
from .crypto import PrivateKey
from .par import par_as_bool, par_as_string, par_as_tuple

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

DEPLOY_GET_BALANCE_RHO_TPL = """
new deployId(`rho:system:deployId`), rl(`rho:registry:lookup`), SystemVaultCh, vaultCh, balanceCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreate", "$addr", *vaultCh) |
    for (@(true, vault) <- vaultCh) {
      @vault!("balance", *balanceCh) |
      for (@balance <- balanceCh) {
        deployId!(balance)
      }
    }
  }
}
"""

TRANSFER_RHO_TPL = """
new deployId(`rho:system:deployId`), rl(`rho:registry:lookup`), SystemVaultCh, vaultCh, authKeyCh, deployerId(`rho:system:deployerId`), resultCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreate", "$from", *vaultCh) |
    @SystemVault!("deployerAuthKey", *deployerId, *authKeyCh) |
    for (@(true, vault) <- vaultCh; key <- authKeyCh) {
      @vault!("transfer", "$to", $amount, *key, *resultCh) |
      for (@result <- resultCh) {
        deployId!(result)
      }
    }
  }
}
"""

TRANSFER_ENSURE_RHO_TPL = """
new deployId(`rho:system:deployId`), rl(`rho:registry:lookup`), SystemVaultCh, vaultCh, toVaultCh, deployerId(`rho:system:deployerId`), authKeyCh, resultCh in {
  rl!(`rho:vault:system`, *SystemVaultCh) |
  for (@(_, SystemVault) <- SystemVaultCh) {
    @SystemVault!("findOrCreate", "$from", *vaultCh) |
    @SystemVault!("findOrCreate", "$to", *toVaultCh) |
    @SystemVault!("deployerAuthKey", *deployerId, *authKeyCh) |
    for (@(true, vault) <- vaultCh; key <- authKeyCh; @(true, toVault) <- toVaultCh) {
      @vault!("transfer", "$to", $amount, *key, *resultCh) |
      for (@result <- resultCh) {
        deployId!(result)
      }
    }
  }
}
"""

TRANSFER_PHLO_LIMIT = 1000000
TRANSFER_PHLO_PRICE = 1


@dataclasses.dataclass
class TransferResult:
    """Result of a vault transfer operation."""
    deploy_id: str
    success: bool
    reason: str


def render_contract_template(template: str, substitutions: Mapping[str, str]) -> str:
    return string.Template(template).substitute(substitutions)


class VaultAPI:

    def __init__(self, client: F1r3flyClient, shard_id: str = "root"):
        self.client = client
        self.shard_id = shard_id

    def get_balance(self, vault_addr: str, block_hash: str = '') -> int:
        """Query vault balance via exploratory deploy (readonly nodes only)."""
        contract = render_contract_template(
            GET_BALANCE_RHO_TPL,
            {'addr': vault_addr},
        )
        result = self.client.exploratory_deploy(contract, block_hash)
        return int(result[0].exprs[0].g_int)

    def deploy_get_balance(
        self,
        vault_addr: str,
        private_key: PrivateKey,
        inclusion_timeout: int,
        finalization_timeout: int,
        phlo_price: int = TRANSFER_PHLO_PRICE,
        phlo_limit: int = TRANSFER_PHLO_LIMIT,
    ) -> int:
        """Query vault balance via a real deploy (works on any node).

        Creates a block and consumes phlo. Use this on validator nodes
        where exploratory deploy is not available.
        """
        from .polling import deploy_and_read
        from .par import par_as_int

        contract = render_contract_template(
            DEPLOY_GET_BALANCE_RHO_TPL,
            {'addr': vault_addr},
        )
        pars, _, _ = deploy_and_read(
            self.client, contract, private_key,
            inclusion_timeout, finalization_timeout,
            phlo_limit=phlo_limit, phlo_price=phlo_price,
            shard_id=self.shard_id,
        )
        return par_as_int(pars[0])

    def transfer(self, from_addr: str, to_addr: str, amount: int, key: PrivateKey,
                 phlo_price: int = TRANSFER_PHLO_PRICE,
                 phlo_limit: int = TRANSFER_PHLO_LIMIT) -> str:
        """Transfer tokens from one vault to another. Returns the deploy ID.

        The recipient vault must already exist. If it may not exist, use
        ``transfer_ensure`` instead.

        The transfer result (success/failure reason) is written to the
        deployId channel. After block inclusion, read it with
        ``read_transfer_result(deploy_id, block_hash)``.
        """
        contract = render_contract_template(
            TRANSFER_RHO_TPL, {
                'from': from_addr,
                'to': to_addr,
                'amount': str(amount),
            }
        )
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def transfer_ensure(self, from_addr: str, to_addr: str, amount: int, key: PrivateKey,
                        phlo_price: int = TRANSFER_PHLO_PRICE,
                        phlo_limit: int = TRANSFER_PHLO_LIMIT) -> str:
        """Transfer tokens, creating the recipient vault if needed. Returns the deploy ID.

        The transfer result is written to the deployId channel. After
        block inclusion, read it with ``read_transfer_result(deploy_id, block_hash)``.
        """
        contract = render_contract_template(
            TRANSFER_ENSURE_RHO_TPL, {
                'from': from_addr,
                'to': to_addr,
                'amount': str(amount),
            }
        )
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def read_transfer_result(self, deploy_id: str, block_hash: str = "") -> TransferResult:
        """Read the transfer result from the deployId channel.

        Call after the deploy has been included in a block.
        Returns ``TransferResult(deploy_id, success, reason)``.
        """
        data = self.client.get_data_at_deploy_id(deploy_id, block_hash=block_hash)
        if data is None or not hasattr(data, 'par') or len(data.par) == 0:
            return TransferResult(deploy_id=deploy_id, success=False, reason="no data")
        par = data.par[0]
        try:
            elements = par_as_tuple(par)
            success = par_as_bool(elements[0])
            reason = par_as_string(elements[1]) if not success else ""
            return TransferResult(deploy_id=deploy_id, success=success, reason=reason)
        except (ValueError, IndexError):
            return TransferResult(deploy_id=deploy_id, success=False, reason=f"unexpected data: {par}")

    def create_vault(self, addr: str, key: PrivateKey,
                     phlo_price: int = TRANSFER_PHLO_PRICE,
                     phlo_limit: int = TRANSFER_PHLO_LIMIT) -> str:
        contract = render_contract_template(
            CREATE_VAULT_RHO_TPL,
            {'addr': addr},
        )
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )
