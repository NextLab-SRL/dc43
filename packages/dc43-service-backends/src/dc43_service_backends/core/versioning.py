from __future__ import annotations

"""Simple Semantic Version helper for contract version comparisons."""

from dataclasses import dataclass
from typing import Optional, Tuple
import re


SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$")


@dataclass(frozen=True)
class SemVer:
    """Tiny SemVer parser/utility used for version checks in IO wrappers."""
    major: int
    minor: int
    patch: int
    prerelease: Optional[str] = None
    build: Optional[str] = None

    def __str__(self) -> str:
        base = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            base += f"-{self.prerelease}"
        if self.build:
            base += f"+{self.build}"
        return base

    @staticmethod
    def parse(s: str) -> "SemVer":
        """Parse a ``MAJOR.MINOR.PATCH[-prerelease][+build]`` string."""
        m = SEMVER_RE.match(s)
        if not m:
            raise ValueError(f"Invalid semver: {s}")
        major, minor, patch, prerelease, build = m.groups()
        return SemVer(int(major), int(minor), int(patch), prerelease, build)

    def bump(self, level: str) -> "SemVer":
        """Return a new instance bumped at ``major``/``minor``/``patch`` level."""
        if level == "major":
            return SemVer(self.major + 1, 0, 0)
        if level == "minor":
            return SemVer(self.major, self.minor + 1, 0)
        if level == "patch":
            return SemVer(self.major, self.minor, self.patch + 1)
        raise ValueError("level must be one of: major, minor, patch")

_VERSION_KEY_PATTERN = re.compile(
    r"^\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:\.(\d+))?(.*)\s*$"
)
_VERSION_KEY_STAGE = re.compile(
    r"^(?P<label>dev|draft|a|alpha|b|beta|rc|post)(?P<number>\d*)",
    re.IGNORECASE,
)
_VERSION_KEY_SUFFIX = re.compile(
    r"^(?P<label>[a-z]+)(?:[\W_]*(?P<number>\d+))?",
    re.IGNORECASE,
)
_VERSION_KEY_ORDER = {
    "dev": 0,
    "draft": 0,
    "a": 1,
    "alpha": 1,
    "b": 2,
    "beta": 2,
    "rc": 3,
    "": 4,
    "post": 5,
}


def version_key(version: str) -> Tuple[int, int, int, int, int, int]:
    """Return a sortable key for dotted versions with optional suffixes."""

    match = _VERSION_KEY_PATTERN.match(version or "")
    if not match:
        return (0, 0, 0, 0, -1, 0)

    major, minor, patch, build, suffix = match.groups()
    digits = [major, minor, patch, build]
    components = [int(part) if part and part.isdigit() else 0 for part in digits]

    suffix = (suffix or "").lstrip(".-_").lower()
    label = ""
    number = 0
    if suffix:
        stage = _VERSION_KEY_STAGE.match(suffix)
        if stage:
            label = stage.group("label").lower()
            number_str = stage.group("number") or "0"
            number = int(number_str) if number_str.isdigit() else 0
        else:
            fallback = _VERSION_KEY_SUFFIX.match(suffix)
            if fallback:
                label = fallback.group("label").lower()
                number_str = fallback.group("number")
                if number_str and number_str.isdigit():
                    number = int(number_str)
            else:
                label = suffix
    rank = _VERSION_KEY_ORDER.get(label, -1)
    return (*components, rank, number)


__all__ = ["SemVer", "version_key"]
