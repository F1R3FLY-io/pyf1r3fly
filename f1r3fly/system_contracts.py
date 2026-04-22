"""Query helpers for F1R3FLY system contracts.

System contracts are deployed at genesis and registered at well-known
URIs. This module provides typed query methods for each contract.

Two query modes:
  - **Exploratory deploy** (``query_token_metadata``, etc.) — instant,
    read-only, no block created. Restricted to readonly nodes.
  - **Real deploy** (``deploy_query_token_metadata``) — creates a block,
    consumes phlo. Works on any node including validators.

Currently supported:
  - ``rho:system:tokenMetadata`` — native token name, symbol, decimals
"""
from __future__ import annotations

import dataclasses

from .client import F1r3flyClient
from .par import par_as_int, par_as_string, par_as_tuple


@dataclasses.dataclass
class TokenMetadata:
    """Native token metadata from the on-chain contract."""
    name: str
    symbol: str
    decimals: int


_QUERY_ALL = """
new ret, rl(`rho:registry:lookup`), tmCh in {
  rl!(`rho:system:tokenMetadata`, *tmCh) |
  for (@(_, TokenMetadata) <- tmCh) {
    @TokenMetadata!("all", *ret)
  }
}
"""

_QUERY_NAME = """
new ret, rl(`rho:registry:lookup`), tmCh in {
  rl!(`rho:system:tokenMetadata`, *tmCh) |
  for (@(_, TokenMetadata) <- tmCh) {
    @TokenMetadata!("name", *ret)
  }
}
"""

_QUERY_SYMBOL = """
new ret, rl(`rho:registry:lookup`), tmCh in {
  rl!(`rho:system:tokenMetadata`, *tmCh) |
  for (@(_, TokenMetadata) <- tmCh) {
    @TokenMetadata!("symbol", *ret)
  }
}
"""

_QUERY_DECIMALS = """
new ret, rl(`rho:registry:lookup`), tmCh in {
  rl!(`rho:system:tokenMetadata`, *tmCh) |
  for (@(_, TokenMetadata) <- tmCh) {
    @TokenMetadata!("decimals", *ret)
  }
}
"""


# Deploy-based template — writes result to deployId channel.
# Used on validator nodes where exploratory deploy is not available.
_DEPLOY_QUERY_ALL = """
new deployId(`rho:system:deployId`),
    rl(`rho:registry:lookup`), tmCh in {
  rl!(`rho:system:tokenMetadata`, *tmCh) |
  for (@(_, TokenMetadata) <- tmCh) {
    new ret in {
      @TokenMetadata!("all", *ret) |
      for (@result <- ret) {
        deployId!(result)
      }
    }
  }
}
"""


def deploy_query_token_metadata(
    client: F1r3flyClient,
    private_key,
    inclusion_timeout: int,
    finalization_timeout: int,
    phlo_limit: int = 100_000,
    phlo_price: int = 1,
    shard_id: str = "root",
) -> TokenMetadata:
    """Query all native token metadata via a real deploy.

    Creates a block and consumes phlo. Use this on validator nodes
    where exploratory deploy is not available.

    Args:
        client: F1r3flyClient instance (connected to a validator).
        private_key: PrivateKey for signing the deploy.
        inclusion_timeout: Seconds to wait for block inclusion.
        finalization_timeout: Seconds to wait for finalization.

    Returns:
        TokenMetadata with name, symbol, decimals.
    """
    from .polling import deploy_and_read

    pars, _, _ = deploy_and_read(
        client, _DEPLOY_QUERY_ALL, private_key,
        inclusion_timeout, finalization_timeout,
        phlo_limit=phlo_limit, phlo_price=phlo_price,
        shard_id=shard_id,
    )
    elements = par_as_tuple(pars[0])
    if len(elements) != 3:
        raise RuntimeError(
            f"Expected (name, symbol, decimals) tuple, got {len(elements)} elements"
        )
    return TokenMetadata(
        name=par_as_string(elements[0]),
        symbol=par_as_string(elements[1]),
        decimals=par_as_int(elements[2]),
    )


def query_token_metadata(
    client: F1r3flyClient, *, block_hash: str = ""
) -> TokenMetadata:
    """Query all native token metadata fields via exploratory deploy.

    Calls ``TokenMetadata!("all", *ret)`` which returns a
    ``(name, symbol, decimals)`` tuple.
    """
    results = client.exploratory_deploy(_QUERY_ALL, block_hash)
    if not results:
        raise RuntimeError(
            "TokenMetadata('all', ...) returned no results; the contract "
            "may not have been deployed at genesis"
        )
    elements = par_as_tuple(results[0])
    if len(elements) != 3:
        raise RuntimeError(
            f"Expected (name, symbol, decimals) tuple, got {len(elements)} elements"
        )
    return TokenMetadata(
        name=par_as_string(elements[0]),
        symbol=par_as_string(elements[1]),
        decimals=par_as_int(elements[2]),
    )


def query_token_name(
    client: F1r3flyClient, *, block_hash: str = ""
) -> str:
    """Query just the native token name."""
    results = client.exploratory_deploy(_QUERY_NAME, block_hash)
    if not results:
        raise RuntimeError("TokenMetadata('name', ...) returned no results")
    return par_as_string(results[0])


def query_token_symbol(
    client: F1r3flyClient, *, block_hash: str = ""
) -> str:
    """Query just the native token symbol."""
    results = client.exploratory_deploy(_QUERY_SYMBOL, block_hash)
    if not results:
        raise RuntimeError("TokenMetadata('symbol', ...) returned no results")
    return par_as_string(results[0])


def query_token_decimals(
    client: F1r3flyClient, *, block_hash: str = ""
) -> int:
    """Query just the native token decimals."""
    results = client.exploratory_deploy(_QUERY_DECIMALS, block_hash)
    if not results:
        raise RuntimeError("TokenMetadata('decimals', ...) returned no results")
    return par_as_int(results[0])
