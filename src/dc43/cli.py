from __future__ import annotations

"""Small CLI utilities for publishing and fetching contracts from FS stores."""

import argparse
import json
import sys
from typing import Optional

from .storage.fs import FSContractStore
from .odcs import ensure_version, contract_identity, to_model, as_odcs_dict


def _cmd_publish(args):
    """Publish an ODCS JSON file to an FS-based registry."""
    store = FSContractStore(args.base)
    if args.file == "-":
        data = sys.stdin.read()
    else:
        with open(args.file, "r", encoding="utf-8") as f:
            data = f.read()
    import json
    doc = json.loads(data)
    model = to_model(doc)
    ensure_version(model)
    store.put(model)
    cid, ver = contract_identity(model)
    print(f"Published {cid}:{ver} -> {args.base}")


def _cmd_get(args):
    """Fetch and print an ODCS JSON contract from the FS registry."""
    store = FSContractStore(args.base)
    c = store.get(args.contract_id, args.version)
    import json
    print(json.dumps(as_odcs_dict(c), indent=2, sort_keys=True))


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint used by the package console script ``dc43``."""
    p = argparse.ArgumentParser(prog="dc43", description="Data contracts CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    pub = sub.add_parser("publish", help="Publish a contract JSON to FS store")
    pub.add_argument("--base", required=True, help="Base folder path (e.g. /dbfs/mnt/contracts)")
    pub.add_argument("--file", required=True, help="Contract JSON file (or - for stdin)")
    pub.set_defaults(func=_cmd_publish)

    getp = sub.add_parser("get", help="Fetch a contract from FS store")
    getp.add_argument("--base", required=True)
    getp.add_argument("--contract-id", required=True)
    getp.add_argument("--version", required=True)
    getp.set_defaults(func=_cmd_get)

    # codegen command removed to simplify: rely on official ODCS package

    args = p.parse_args(argv)
    return args.func(args) or 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
