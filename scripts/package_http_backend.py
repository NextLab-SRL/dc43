"""Helper CLI for building and publishing dc43 service images.

The script wraps a couple of `docker` invocations so operators can rely on a
single command regardless of the target registry. It assumes the Docker CLI is
installed and authenticated against the registry when `--push` is requested.

Usage examples:

```bash
# HTTP backend image
python scripts/package_http_backend.py --image myregistry.azurecr.io/dc43/governance:1.0.0

# Contracts app image
python scripts/package_http_backend.py --target contracts-app --image myregistry.azurecr.io/dc43/contracts-app:latest --push
```
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]

TARGETS: dict[str, Path] = {
    "http-backend": ROOT / "deploy" / "http-backend" / "Dockerfile",
    "contracts-app": ROOT / "deploy" / "contracts-app" / "Dockerfile",
}


def run(cmd: Sequence[str]) -> None:
    """Run *cmd* and echo it before execution."""

    print("$", " ".join(shlex.quote(part) for part in cmd))
    subprocess.run(cmd, check=True)


def build(image: str, dockerfile: Path, context: Path, platform: str | None) -> None:
    """Build the requested image using *dockerfile*."""

    command = [
        "docker",
        "build",
        "-t",
        image,
        "-f",
        str(dockerfile),
    ]
    if platform:
        command.extend(["--platform", platform])
    command.append(str(context))
    run(command)


def push(image: str) -> None:
    """Push the previously built image to its registry."""

    run(["docker", "push", image])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=sorted(TARGETS),
        default="http-backend",
        help="Which service image to build.",
    )
    parser.add_argument(
        "--image",
        required=True,
        help="Fully qualified image reference including registry and tag.",
    )
    parser.add_argument(
        "--context",
        type=Path,
        default=ROOT,
        help="Build context passed to docker build (defaults to repository root).",
    )
    parser.add_argument(
        "--platform",
        help="Optional target platform passed to docker build (for example linux/amd64).",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Push the image to the remote registry after a successful build.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build(
        args.image,
        dockerfile=TARGETS[args.target],
        context=args.context,
        platform=args.platform,
    )
    if args.push:
        push(args.image)


if __name__ == "__main__":
    main()
