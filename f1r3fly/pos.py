"""Proof-of-Stake (PoS) domain API.

Inline Rholang term construction for the PoS contract: bond / withdraw
deploys (whose ``(Boolean, message)`` contract return is captured on the
``rho:system:deployId`` channel so the result can be read back after block
inclusion), plus exploratory read terms for the PoS state maps that have no
HTTP endpoint (withdrawers, pendingWithdrawers) and the bonds / rewards maps.

Mirrors the :class:`~f1r3fly.vault.VaultAPI` pattern. Self-contained: terms
are built here, not loaded from external ``.rho`` files.

Read methods use exploratory deploy and therefore only work on a read-only
(observer) node — a validator rejects exploratory deploy unless it runs in
dev mode.
"""
import dataclasses
import string
import time
from typing import Dict, List, Mapping, Tuple

from .client import F1r3flyClient
from .crypto import PrivateKey
from .par import par_as_bool, par_as_int, par_as_map, par_as_string, par_as_tuple

BOND_RHO_TPL = """
new retCh, PoSCh, rl(`rho:registry:lookup`), deployerId(`rho:system:deployerId`), deployId(`rho:system:deployId`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!("bond", *deployerId, $amount, *retCh) |
    for (@result <- retCh) {
      deployId!(result)
    }
  }
}
"""

WITHDRAW_RHO_TPL = """
new retCh, PoSCh, rl(`rho:registry:lookup`), deployerId(`rho:system:deployerId`), deployId(`rho:system:deployId`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!("withdraw", *deployerId, *retCh) |
    for (@result <- retCh) {
      deployId!(result)
    }
  }
}
"""

# commitRandomImage(@deployerId, @hash, ackCh): stores the keccak256 image for the
# deployer; ack is bare ``true`` on success or ``(false, msg)`` on failure.
COMMIT_RANDOM_IMAGE_RHO_TPL = """
new retCh, PoSCh, rl(`rho:registry:lookup`), deployerId(`rho:system:deployerId`), deployId(`rho:system:deployId`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!("commitRandomImage", *deployerId, "$image".hexToBytes(), *retCh) |
    for (@result <- retCh) {
      deployId!(result)
    }
  }
}
"""

# revealRandom(@deployerId, @random, ackCh): the contract keccak256-hashes the
# revealed random and compares to the committed image; ack is bare ``true`` on a
# match or ``(false, msg)`` on mismatch / no-commit.
REVEAL_RANDOM_RHO_TPL = """
new retCh, PoSCh, rl(`rho:registry:lookup`), deployerId(`rho:system:deployerId`), deployId(`rho:system:deployId`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!("revealRandom", *deployerId, "$random".hexToBytes(), *retCh) |
    for (@result <- retCh) {
      deployId!(result)
    }
  }
}
"""

# posVaultTransfer(@targetAddress, @amount, @deployerID, @transferRet): human-facing
# posVault transfer, gated on the deployer being the PoS contract key — any other
# deployer gets ``(false, "You have not permission to transfer.")``.
POS_VAULT_TRANSFER_RHO_TPL = """
new retCh, PoSCh, rl(`rho:registry:lookup`), deployerId(`rho:system:deployerId`), deployId(`rho:system:deployId`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!("posVaultTransfer", "$target", $amount, *deployerId, *retCh) |
    for (@result <- retCh) {
      deployId!(result)
    }
  }
}
"""

# Auth-token-gated system methods (chargeDeploy/refundDeploy/closeBlock) invoked from a
# user deploy with a bogus token (Nil). The PoS bundle is a single write-enabled bundle,
# so a user CAN call these; the token check fails first -> "(false, Invalid system auth
# token)" with no state change. ``$call`` is the full argument list incl ``*retCh``.
AUTH_BAD_TOKEN_RHO_TPL = """
new retCh, PoSCh, rl(`rho:registry:lookup`), deployerId(`rho:system:deployerId`), deployId(`rho:system:deployId`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!($call) |
    for (@result <- retCh) {
      deployId!(result)
    }
  }
}
"""

# Per-method argument lists for AUTH_BAD_TOKEN_RHO_TPL; the token slot is always ``Nil``.
_AUTH_GATED_BAD_TOKEN_CALLS = {
    "chargeDeploy": '"chargeDeploy", *deployerId, 100, Nil, *retCh',
    "refundDeploy": '"refundDeploy", 100, Nil, *retCh',
    "closeBlock": '"closeBlock", Nil, *retCh',
}

# Exploratory read terms. ``$method`` is one of the zero-argument PoS read
# methods (getBonds, getActiveValidators, getWithdrawers,
# getPendingWithdrawer, getRewards). The result is left on ``return`` for the
# exploratory harvester.
READ_RHO_TPL = """
new return, PoSCh, rl(`rho:registry:lookup`) in {
  rl!(`rho:system:pos`, *PoSCh) |
  for (@(_, PoS) <- PoSCh) {
    @PoS!("$method", *return)
  }
}
"""

BOND_PHLO_LIMIT = 100_000_000
BOND_PHLO_PRICE = 1


@dataclasses.dataclass
class PosResult:
    """Result of a PoS bond/withdraw contract call.

    ``success`` is the Boolean the contract returned on its retCh; ``reason``
    is the accompanying message on failure (empty on success). A rejected
    bond/withdraw still produces a SUCCESSFUL deploy — the rejection is the
    contract returning ``(false, reason)``, not a deploy error.
    """
    deploy_id: str
    success: bool
    reason: str


def render_contract_template(template: str, substitutions: Mapping[str, str]) -> str:
    return string.Template(template).substitute(substitutions)


class PosAPI:

    def __init__(self, client: F1r3flyClient, shard_id: str = "root"):
        self.client = client
        self.shard_id = shard_id

    # ── Mutating deploys (signed by the acting validator's key) ──────────

    def bond(self, key: PrivateKey, amount: int,
             phlo_price: int = BOND_PHLO_PRICE,
             phlo_limit: int = BOND_PHLO_LIMIT) -> str:
        """Bond ``amount`` for the validator identified by ``key``. Returns the
        deploy ID.

        The deployer (``key``) is the validator being bonded — PoS resolves
        the bonding validator from ``rho:system:deployerId``. The contract
        ``(Boolean, message)`` return is written to the deployId channel;
        read it after inclusion with :meth:`read_result`.
        """
        contract = render_contract_template(BOND_RHO_TPL, {"amount": str(amount)})
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def withdraw(self, key: PrivateKey,
                 phlo_price: int = BOND_PHLO_PRICE,
                 phlo_limit: int = BOND_PHLO_LIMIT) -> str:
        """Withdraw (unbond) the validator identified by ``key``. Returns the
        deploy ID.

        The contract ``(Boolean, message)`` return is written to the deployId
        channel; read it after inclusion with :meth:`read_result`.
        """
        contract = WITHDRAW_RHO_TPL
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def commit_random_image(self, key: PrivateKey, image_hex: str,
                            phlo_price: int = BOND_PHLO_PRICE,
                            phlo_limit: int = BOND_PHLO_LIMIT) -> str:
        """Commit the keccak256 ``image_hex`` (hex, no ``0x``) for the validator
        identified by ``key``. Read the ack with :meth:`read_result`."""
        contract = render_contract_template(COMMIT_RANDOM_IMAGE_RHO_TPL, {"image": image_hex})
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def reveal_random(self, key: PrivateKey, random_hex: str,
                      phlo_price: int = BOND_PHLO_PRICE,
                      phlo_limit: int = BOND_PHLO_LIMIT) -> str:
        """Reveal the ``random_hex`` (hex, no ``0x``) preimage for the validator
        identified by ``key``. The contract keccak256-hashes it and compares to the
        committed image. Read the ack with :meth:`read_result`."""
        contract = render_contract_template(REVEAL_RANDOM_RHO_TPL, {"random": random_hex})
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def pos_vault_transfer(self, key: PrivateKey, target_address: str, amount: int,
                           phlo_price: int = BOND_PHLO_PRICE,
                           phlo_limit: int = BOND_PHLO_LIMIT) -> str:
        """Request a posVault transfer of ``amount`` to ``target_address``, signed by
        ``key``. Only the PoS contract key is authorized; any other deployer gets
        ``(false, "You have not permission to transfer.")``. Read with :meth:`read_result`."""
        contract = render_contract_template(
            POS_VAULT_TRANSFER_RHO_TPL, {"target": target_address, "amount": str(amount)})
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def call_auth_gated_invalid_token(self, key: PrivateKey, method: str,
                                      phlo_price: int = BOND_PHLO_PRICE,
                                      phlo_limit: int = BOND_PHLO_LIMIT) -> str:
        """Invoke an auth-token-gated system method (``chargeDeploy`` / ``refundDeploy`` /
        ``closeBlock``) with a bogus token (Nil), driving the "Invalid system auth token"
        reject branch with no state change. Read the ``(false, msg)`` ack with
        :meth:`read_result`. Returns the deploy ID."""
        if method not in _AUTH_GATED_BAD_TOKEN_CALLS:
            raise ValueError(f"unsupported auth-gated method {method!r}")
        contract = render_contract_template(
            AUTH_BAD_TOKEN_RHO_TPL, {"call": _AUTH_GATED_BAD_TOKEN_CALLS[method]})
        timestamp_mill = int(time.time() * 1000)
        return self.client.deploy_with_vabn_filled(
            key, contract, phlo_price, phlo_limit, timestamp_mill, self.shard_id,
        )

    def read_result(self, deploy_id: str, block_hash: str = "") -> PosResult:
        """Read a PoS contract ack from the deployId channel. Call after the deploy
        has been included in a block.

        Handles both ack shapes: bond/withdraw/posVaultTransfer return a
        ``(Boolean, message)`` tuple; commitRandomImage/revealRandom return a bare
        ``true`` on success (and ``(false, message)`` on failure).
        """
        data = self.client.get_data_at_deploy_id(deploy_id, block_hash=block_hash)
        if data is None or not hasattr(data, "par") or len(data.par) == 0:
            return PosResult(deploy_id=deploy_id, success=False, reason="no data")
        par = data.par[0]
        try:
            elements = par_as_tuple(par)
            success = par_as_bool(elements[0])
            reason = par_as_string(elements[1]) if not success else ""
            return PosResult(deploy_id=deploy_id, success=success, reason=reason)
        except (ValueError, IndexError):
            # Bare-bool ack (commitRandomImage / revealRandom success path).
            try:
                success = par_as_bool(par)
                return PosResult(deploy_id=deploy_id, success=success,
                                 reason="" if success else "false")
            except ValueError:
                return PosResult(deploy_id=deploy_id, success=False,
                                 reason=f"unexpected data: {par}")

    # ── Exploratory reads (read-only / observer node only) ───────────────

    def get_bonds(self, block_hash: str = "") -> Dict[str, int]:
        """``allBonds`` map: ``{validator_pubkey_hex: stake}``."""
        return self._read_int_map("getBonds", block_hash)

    def get_rewards(self, block_hash: str = "") -> Dict[str, int]:
        """Accrued reward per ever-bonded validator: ``{pubkey_hex: reward}``."""
        return self._read_int_map("getRewards", block_hash)

    def get_pending_withdrawer(self, block_hash: str = "") -> Dict[str, int]:
        """``pendingWithdrawers`` map: ``{pubkey_hex: quarantineUntilBlock}``.

        A validator that has issued a withdraw in the current epoch but has
        not yet been moved out at the epoch boundary.
        """
        return self._read_int_map("getPendingWithdrawer", block_hash)

    def get_withdrawers(self, block_hash: str = "") -> Dict[str, Tuple[int, int]]:
        """``withdrawers`` map: ``{pubkey_hex: (bond_plus_reward, quarantineUntil)}``.

        A validator moved out of the active set at an epoch boundary, awaiting
        quarantine payout.
        """
        raw = self._read_map("getWithdrawers", block_hash)
        result: Dict[str, Tuple[int, int]] = {}
        for pk_hex, pair in raw.items():
            # par_as_map decodes a tuple value to a list of raw Par elements.
            result[pk_hex] = (par_as_int(pair[0]), par_as_int(pair[1]))
        return result

    def get_coop_vault(self, block_hash: str = "") -> List:
        """``getCoopVault``: the cooperative multi-sig vault descriptor tuple
        (raw Par elements). Sanity read — assert arity, not a deep decode
        (vault elements are unforgeable names)."""
        return self._read_tuple("getCoopVault", block_hash)

    def get_initial_pos_vault(self, block_hash: str = "") -> List:
        """``getInitialPosVault``: ``(vault_address, vault)`` tuple (raw Par
        elements). Sanity read."""
        return self._read_tuple("getInitialPosVault", block_hash)

    def get_epoch_length(self, block_hash: str = "") -> int:
        """``getEpochLength``: the genesis ``epochLength`` parameter."""
        return self._read_int("getEpochLength", block_hash)

    def get_quarantine_length(self, block_hash: str = "") -> int:
        """``getQuarantineLength``: the genesis ``quarantineLength`` parameter."""
        return self._read_int("getQuarantineLength", block_hash)

    def _read_map(self, method: str, block_hash: str) -> Dict:
        contract = render_contract_template(READ_RHO_TPL, {"method": method})
        result = self.client.exploratory_deploy(contract, block_hash)
        return par_as_map(result[0])

    def _read_int_map(self, method: str, block_hash: str) -> Dict[str, int]:
        return {k: int(v) for k, v in self._read_map(method, block_hash).items()}

    def _read_tuple(self, method: str, block_hash: str) -> List:
        contract = render_contract_template(READ_RHO_TPL, {"method": method})
        result = self.client.exploratory_deploy(contract, block_hash)
        return par_as_tuple(result[0])

    def _read_int(self, method: str, block_hash: str) -> int:
        contract = render_contract_template(READ_RHO_TPL, {"method": method})
        result = self.client.exploratory_deploy(contract, block_hash)
        return par_as_int(result[0])
