"""Rholang contract query helpers via exploratory deploy.

Provides read-only queries against on-chain data without creating blocks.
Uses exploratory deploy for instant, free reads. Must be called on a
read-only node (validators don't support exploratory deploy).

Two helpers:

- ``registry_lookup`` — look up a raw value stored in the registry.
  Works for any value registered via ``insertArbitrary``.

- ``registry_query`` — look up a contract in the registry and call a
  method on it. Only works for contracts that respond synchronously
  within the exploratory deploy's execution window (e.g. system
  contracts like ``TokenMetadata``). Does NOT work for contracts
  that read from persistent state channels (e.g. bridge contract's
  ``getNonce`` reads from ``nonceCh``).

Example::

    from f1r3fly.client import F1r3flyClient
    from f1r3fly.contracts import registry_lookup
    from f1r3fly.par import par_as_string

    with F1r3flyClient("localhost", 40401) as client:
        # Read a value stored via insertArbitrary
        pars = registry_lookup(client, "rho:id:abc123")
        value = par_as_string(pars[0])
"""
from __future__ import annotations





def registry_lookup(
    client,
    uri: str,
    block_hash: str = "",
) -> list:
    """Look up a value in the registry via exploratory deploy.

    Returns the Par values stored at the given URI. No block is created.

    Args:
        client: F1r3flyClient instance.
        uri: Registry URI (e.g. ``rho:id:abc123``).
        block_hash: Optional block hash to query against.

    Returns:
        List of Par values from the registry entry.

    Raises:
        RuntimeError: If the lookup returns no results.
    """
    term = f"""
new ret, lookup(`rho:registry:lookup`) in {{
  lookup!(`{uri}`, *ret)
}}
"""
    results = client.exploratory_deploy(term, block_hash)
    if not results:
        raise RuntimeError(
            f"Registry lookup for {uri} returned no results. "
            f"The URI may not exist."
        )
    return results


def registry_query(
    client,
    uri: str,
    method: str,
    param: str = "Nil",
    block_hash: str = "",
) -> list:
    """Query a registry-registered contract via exploratory deploy.

    Looks up ``uri`` in the registry, calls ``method`` with ``param``,
    and returns the Par results. No block is created — this is a
    read-only operation.

    Args:
        client: F1r3flyClient instance.
        uri: Registry URI (e.g. ``rho:id:abc123``).
        method: Contract method name (e.g. ``"getNonce"``).
        param: Rholang expression for the method parameter (default ``"Nil"``).
        block_hash: Optional block hash to query against. Empty string
            means latest state.

    Returns:
        List of Par values from the contract response.

    Raises:
        RuntimeError: If the exploratory deploy returns no results.
    """
    term = f"""
new ret, lookup(`rho:registry:lookup`), ch in {{
  lookup!(`{uri}`, *ch) |
  for (c <- ch) {{
    c!("{method}", {param}, *ret)
  }}
}}
"""
    results = client.exploratory_deploy(term, block_hash)
    if not results:
        raise RuntimeError(
            f"Registry query {uri} -> {method}({param}) returned no results. "
            f"The contract may not be deployed or the method may not respond."
        )
    return results
