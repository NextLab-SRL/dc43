# Release and Versioning Guide

This repository ships four Python distributions:

- `dc43-service-clients`
- `dc43-service-backends`
- `dc43-integrations`
- `dc43` (meta package aggregating the other three)

Each package keeps its own version in a plain `VERSION` file alongside the corresponding
`pyproject.toml`. Use the following workflow to manage releases without forcing synchronized bumps
across the whole stack.

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

The [`release.yml`](../.github/workflows/release.yml) workflow now triggers when a commit lands on
`main` with `[release]` anywhere in its message (manual runs via **Run workflow** remain available
through `workflow_dispatch`). Once invoked, the workflow gathers every package tag that points at the
commit and elects a **primary** run—the first tag in dependency order (`dc43`, then
`dc43-service-clients`, `dc43-service-backends`, `dc43-integrations`, and finally
`dc43-contracts-app`). Only that primary run executes the publication pipeline; other runs exit
immediately after reporting which tag will perform the release.

Within the primary run a single `release` job stages and publishes each package in two passes:

1. **Build pass.** Install the editable sources, run the relevant tests, validate the tag against the
   package's `VERSION` file, and deposit wheels/sdists into `release-artifacts/<package>`.
2. **Publish pass.** Once every requested package has built successfully, push artifacts to PyPI and
   attach them to GitHub Releases in the same dependency order.

Running all builds before any publishes guarantees we either release all packages or none for a given
commit, eliminating the ad-hoc `wait_for_internal_deps.py` helper. Pushing multiple tags from a single
commit is still supported—the primary tag's run handles every package end-to-end, so you get one long
pipeline instead of five independent queues.

### Multi-package release CLI

To make tagging easier—especially when several packages need a release—you can use the helper
script at [`scripts/release.py`](../scripts/release.py). The CLI inspects the repository to work out
which packages changed since their most recent release tag, verifies that the version in each
`VERSION` file is new, checks whether the version is already on PyPI, and then (optionally)
creates and pushes the corresponding tags.

Common workflows:

- **Dry run:** list all packages, highlighting those that have changed and the tag that would be
  created.

  ```bash
  python scripts/release.py
  ```

- **Release everything that changed:** create annotated tags for all packages that need a release,
  and push the branch plus tags to `origin` (`git push origin main --tags`). The CLI checks that the
  target commit message already contains `[release]` so GitHub Actions will execute. Use
  `--allow-missing-release-marker` when intentionally tagging an older commit without the marker.

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

1. Update the version in `packages/dc43-service-backends/VERSION`.
2. Update changelog notes (either a shared CHANGELOG or one per package).
3. Commit the changes with a message such as `chore(backends): release 0.9.0 [release]` so that the
   automation runs (the release helper script will verify the marker is present).
4. Merge to `main` via PR.
5. Tag the merge commit: `git tag -a dc43-service-backends-v0.9.0`.
6. Push the branch and tags together: `git push origin main --tags`.
7. Let GitHub Actions publish the new wheel/sdist to PyPI.

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
