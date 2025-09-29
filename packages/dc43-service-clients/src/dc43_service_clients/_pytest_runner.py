"""Lightweight wrapper that exposes the ``pytest`` CLI for editable installs.

This module lets continuous integration workflows rely on the ``pytest``
console script even when editable installs of :mod:`dc43_service_clients`
are used.  By delegating to :func:`pytest.console_main` we ensure that the
dependency-provided entry point is available in environments where the
standard ``pytest`` script might not be generated.
"""

from __future__ import annotations

from pytest import console_main


def main() -> int:
    """Execute ``pytest``'s console entry point and return its exit code."""

    return console_main()

