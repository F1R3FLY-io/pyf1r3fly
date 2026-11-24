"""Token amounts as the node encodes them.

The node holds every individual amount in ``[0, 2**256 - 1]``. On the peer and
deploy wire an amount is exactly 32 big-endian bytes; in API responses it is a
canonical decimal string (digits only, no leading zero).
"""

MAX = 2**256 - 1
WIRE_BYTES = 32


def _check(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f'token amount must be an int, got {type(value).__name__}')
    if not 0 <= value <= MAX:
        raise ValueError('token amount must be between 0 and 2**256 - 1')
    return value


def encode(value: int) -> bytes:
    """32-byte big-endian wire form of ``value``."""
    return _check(value).to_bytes(WIRE_BYTES, 'big')


def decode(data: bytes) -> int:
    """Amount from its 32-byte big-endian wire form."""
    if len(data) != WIRE_BYTES:
        raise ValueError('token amount encoding must contain exactly 32 bytes')
    return int.from_bytes(data, 'big')


def parse(text: str) -> int:
    """Amount from its canonical decimal form."""
    return _check(parse_total(text))


def parse_total(text: str) -> int:
    """Sum of amounts, such as a total stake, which can exceed ``MAX``."""
    if not isinstance(text, str):
        raise TypeError(f'token amount must be a decimal string, got {type(text).__name__}')
    if not text or not text.isascii() or not text.isdigit() or (len(text) > 1 and text[0] == '0'):
        raise ValueError(f'token amount must be a canonical unsigned decimal integer: {text!r}')
    return int(text)
