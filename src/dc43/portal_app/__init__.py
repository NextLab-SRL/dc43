"""Portal application exposing contract and dataset views."""

from .server import app, create_app

__all__ = ["app", "create_app"]
