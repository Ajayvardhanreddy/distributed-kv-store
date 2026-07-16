"""
Version token — Phase 4.

The token is the opaque string returned to clients in GET responses and
accepted in PUT / DELETE if_match fields.  Clients must treat it as opaque
and not parse or compare the internal representation.

Phase 5 replaces the internals (int → vector clock) without changing the
public API:  encode_token / decode_token / version_matches signatures stay
identical; only their bodies change.

CASConflictError is raised by the router when a version check fails.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Token encoding
# ---------------------------------------------------------------------------

def encode_token(version: int) -> str:
    """Encode an integer version counter as an opaque client token."""
    return str(version)


def decode_token(token: str) -> int:
    """
    Decode a client token back to a version counter.

    Raises ValueError for tokens that cannot be decoded.
    """
    try:
        return int(token)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid version token: {token!r}")


def version_matches(current_version: int, if_match: str) -> bool:
    """
    Return True if current_version is represented by if_match token.

    Returns False (not raises) on malformed tokens — the caller converts
    this to a 409 response.
    """
    try:
        return current_version == decode_token(if_match)
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------

class CASConflictError(Exception):
    """
    Raised by the router when a conditional write fails.

    Carries the current version token so the caller can include it in
    the 409 response body.
    """

    def __init__(self, current_version: int) -> None:
        self.current_version = current_version
        self.current_token = encode_token(current_version)
        super().__init__(f"CAS mismatch: current version is {current_version}")
