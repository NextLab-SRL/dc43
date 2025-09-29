# Release and Versioning Guide

This repository ships four Python distributions:

- `dc43-service-clients`
- `dc43-service-backends`
- `dc43-integrations`
- `dc43` (meta package aggregating the other three)

Each package keeps its own version in its `pyproject.toml`. Use the following workflow to manage
releases without forcing synchronized bumps across the whole stack.

## Tagging strategy

1. **Use annotated tags per package.** Prefix tags with the distribution name to avoid collisions
   and to keep the Git history navigable, e.g. `dc43-service-clients-v0.8.0`.
2. **Tag after merging the release commit.** Update the package version (and changelog if
   applicable), merge to `main`, then tag that merge commit with the appropriate package-prefixed
   tag. This keeps `main` as the canonical source of released artifacts while still letting you
   rewind to any package version with `git checkout dc43-service-clients-v0.8.0`.
3. **Keep the meta package independent.** Only create a `dc43-vX.Y.Z` tag when the aggregate
   package actually changes (e.g. dependency pin updates or CLI tweaks). The three component
   packages can evolve on their own cadence.

This mirrors mono-repo projects such as Kubernetes and the AWS SDKs where a single repository hosts
many release trains.

## GitHub Actions automation

The [`release.yml`](../.github/workflows/release.yml) workflow already watches for each package's
prefix:

```yaml
on:
  push:
    tags:
      - "dc43-v*"
      - "dc43-service-clients-v*"
      - "dc43-service-backends-v*"
      - "dc43-integrations-v*"
```

When a tag fires the workflow, the first step resolves which package directory owns the tag and
produces helper outputs (`package_dir`, `pyproject`, `dist_path`). The subsequent steps then:

1. Install the repo's test dependencies and run the full test suite.
2. Validate that the pushed tag matches the package version declared in the resolved `pyproject.toml`.
3. Change into that package directory and run `python -m build`, producing `dist/` artifacts only for
   the tagged distribution.
4. Publish those artifacts to PyPI and attach them to the GitHub Release.

Because the build and publish stages point at the derived `dist_path`, the workflow only pushes the
package that owns the tag. You do **not** need to retag the other distributions—just bump the version
for the package you changed, push a matching tag, and the automation handles the rest. If you push
multiple package-prefixed tags in the same `git push` command, GitHub Actions starts an independent
workflow run for each tag so you can release several packages from a single commit.

### Multi-package release CLI

To make tagging easier—especially when several packages need a release—you can use the helper
script at [`scripts/release.py`](../scripts/release.py). The CLI inspects the repository to work out
which packages changed since their most recent release tag, verifies that the version in each
`pyproject.toml` is new, checks whether the version is already on PyPI, and then (optionally)
creates and pushes the corresponding tags.

Common workflows:

- **Dry run:** list all packages, highlighting those that have changed and the tag that would be
  created.

  ```bash
  python scripts/release.py
  ```

- **Release everything that changed:** create annotated tags for all packages that need a release
  and push them to `origin` to trigger GitHub Actions.

  ```bash
  python scripts/release.py --apply --push
  ```

- **Release a subset:** specify the packages explicitly. You can also target a specific commit if
  you need to cut a release from an older point in history (the script still validates that the
  versions are new compared with PyPI).

  ```bash
  python scripts/release.py --packages dc43-service-clients dc43-integrations --commit <sha> --apply
  ```

The script prints the plan before taking any action and aborts if the working tree is dirty or if a
tag/version already exists on PyPI. This ensures that every tagged commit has the full set of
expected releases.

## Release checklist

For a package bump (example: `dc43-service-backends`):

1. Update the version in `packages/dc43-service-backends/pyproject.toml`.
2. Update changelog notes (either a shared CHANGELOG or one per package).
3. Commit the changes with a message such as `chore(backends): release 0.9.0`.
4. Merge to `main` via PR.
5. Tag the merge commit: `git tag -a dc43-service-backends-v0.9.0 && git push origin dc43-service-backends-v0.9.0`.
6. Let GitHub Actions publish the new wheel/sdist to PyPI.

Only repeat the steps for other packages when they change. The meta package can be bumped less
frequently, e.g. when you need a curated bundle of the latest component versions.

## Handling concurrent work

When two packages evolve simultaneously:

- Cut separate release commits so that the version bumps stay isolated.
- Merge them in any order; each tag points to the same `main` history but a different release
  commit.
- If you need to patch an older release, branch from the corresponding tag (`git checkout -b
  backport/dc43-service-clients-0.7.x dc43-service-clients-v0.7.3`) and cherry-pick fixes.

Avoid git submodules—they complicate dependency management and require independent repos for each
package. Keeping everything in this mono-repo plus prefixed tags and conditional workflows gives you
per-package releases without sacrificing shared development.
