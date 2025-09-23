"""Compatibility shim for the filesystem-backed stub DQ client."""

from .stubs.fs import StubDQClient

__all__ = ["StubDQClient"]
