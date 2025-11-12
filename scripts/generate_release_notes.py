"""Generate release notes between the previous and current release tags.

This helper keeps the GitHub release descriptions focused on the commits that
landed since the last release tag instead of accumulating the entire project
history on every publication.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Iterable


def _run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def _normalise_remote(url: str) -> str:
    if url.endswith(".git"):
        url = url[:-4]
    if url.startswith("git@github.com:"):
        return url.replace("git@github.com:", "https://github.com/")
    return url


def _iter_commits(rev_range: str) -> Iterable[tuple[str, str]]:
    output = _run_git("log", "--pretty=format:%h\t%s", rev_range)
    if not output:
        return []
    commits = []
    for line in output.splitlines():
        if "\t" in line:
            sha, subject = line.split("\t", 1)
        else:
            sha, subject = line[:7], line[7:]
        commits.append((sha, subject.strip()))
    return commits


def _find_previous_tag(tag: str, pattern: str) -> str | None:
    tags_output = _run_git("tag", "--list", pattern, "--sort=v:refname")
    tags = [entry.strip() for entry in tags_output.splitlines() if entry.strip()]
    try:
        index = tags.index(tag)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise SystemExit(f"Tag '{tag}' not found in pattern '{pattern}'.") from exc

    if index == 0:
        return None
    return tags[index - 1]


def generate_notes(tag: str, pattern: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    previous = _find_previous_tag(tag, pattern)
    rev_range = f"{previous}..{tag}" if previous else tag
    commits = list(_iter_commits(rev_range))

    remote = _normalise_remote(_run_git("remote", "get-url", "origin"))

    lines = ["## What's Changed"]
    if commits:
        for sha, subject in commits:
            lines.append(f"- {subject} ({sha})")
    else:
        lines.append("- No code changes since the previous release.")

    if previous:
        lines.append("")
        lines.append("## Full Changelog")
        lines.append(f"- {remote}/compare/{previous}...{tag}")

    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Release tag to describe.")
    parser.add_argument(
        "--pattern",
        required=True,
        help="Pattern used to find matching tags (e.g. 'dc43-v*').",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path where the release notes should be written.",
    )

    args = parser.parse_args()
    output_path = Path(args.output)
    generate_notes(args.tag, args.pattern, output_path)


if __name__ == "__main__":
    main()
