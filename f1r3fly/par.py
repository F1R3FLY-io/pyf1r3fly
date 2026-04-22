"""Par extraction utilities for Rholang protobuf values.

Provides type-safe extraction of values from Par protobuf messages
returned by exploratory deploys and data-at-name queries.

Example::

    results = client.exploratory_deploy('new x in { x!(42) }')
    value = par_as_int(results[0])  # 42
"""
from __future__ import annotations

from typing import Any, Dict, List


def par_as_string(par) -> str:
    """Extract a Rholang string from a Par protobuf message."""
    for expr in par.exprs:
        if expr.HasField("g_string"):
            return expr.g_string
    raise ValueError(f"Expected a string-bearing Par, got {par}")


def par_as_int(par) -> int:
    """Extract a Rholang integer from a Par protobuf message."""
    for expr in par.exprs:
        if expr.HasField("g_int"):
            return int(expr.g_int)
    raise ValueError(f"Expected an int-bearing Par, got {par}")


def par_as_bool(par) -> bool:
    """Extract a Rholang boolean from a Par protobuf message."""
    for expr in par.exprs:
        if expr.HasField("g_bool"):
            return bool(expr.g_bool)
    raise ValueError(f"Expected a bool-bearing Par, got {par}")


def par_as_tuple(par) -> List:
    """Extract the elements of a Rholang tuple from a Par.

    Returns a list of Par elements from the tuple body.
    """
    for expr in par.exprs:
        if expr.HasField("e_tuple_body"):
            return list(expr.e_tuple_body.ps)
    raise ValueError(f"Expected a tuple-bearing Par, got {par}")


def par_as_list(par) -> List:
    """Extract elements from a Rholang list Par.

    Returns a list of Par elements.
    """
    for expr in par.exprs:
        if expr.HasField("e_list_body"):
            return list(expr.e_list_body.ps)
    raise ValueError(f"Expected a list-bearing Par, got {par}")


def par_as_map(par) -> Dict[Any, Any]:
    """Extract a Rholang map from a Par protobuf message.

    Returns a Python dict with keys and values extracted via ``par_value``.
    """
    for expr in par.exprs:
        if expr.HasField("e_map_body"):
            result = {}
            for kv in expr.e_map_body.kvs:
                result[par_value(kv.key)] = par_value(kv.value)
            return result
    raise ValueError(f"Expected a map-bearing Par, got {par}")


def par_as_uri(par) -> str:
    """Extract a URI string from a Par protobuf message.

    Handles both ``Expr.g_uri`` (inline URI in expression) and
    ``Par.uris`` (top-level URI repeated field).
    """
    for expr in par.exprs:
        if expr.HasField("g_uri"):
            return expr.g_uri
    for uri in par.uris:
        return uri.value
    raise ValueError(f"Expected a URI-bearing Par, got {par}")


def par_value(par) -> Any:
    """Extract the first available value from a Par, auto-detecting the type.

    Tries string, int, bool, tuple, list, URI in order. Returns the
    first match. Raises ValueError if no known type is found.
    """
    for expr in par.exprs:
        if expr.HasField("g_string"):
            return expr.g_string
        if expr.HasField("g_int"):
            return int(expr.g_int)
        if expr.HasField("g_bool"):
            return bool(expr.g_bool)
        if expr.HasField("e_tuple_body"):
            return list(expr.e_tuple_body.ps)
        if expr.HasField("e_list_body"):
            return list(expr.e_list_body.ps)
        if expr.HasField("e_map_body"):
            return {par_value(kv.key): par_value(kv.value) for kv in expr.e_map_body.kvs}
        if expr.HasField("g_uri"):
            return expr.g_uri
    for uri in par.uris:
        return uri.value
    raise ValueError(f"Cannot extract value from Par: {par}")
