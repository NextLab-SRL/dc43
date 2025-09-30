"""Small semantic-version helper used by the contracts app."""

from __future__ import annotations

from dataclasses import dataclass
import re

SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$")


@dataclass(frozen=True)
class SemVer:
    """Tiny SemVer parser/utility used for version checks in IO wrappers."""

    major: int
    minor: int
    patch: int
    prerelease: str | None = None
    build: str | None = None

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

        match = SEMVER_RE.match(s)
        if not match:
            raise ValueError(f"Invalid semver: {s}")
        major, minor, patch, prerelease, build = match.groups()
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


__all__ = ["SemVer"]
