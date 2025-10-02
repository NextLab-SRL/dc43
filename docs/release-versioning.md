# Release and Versioning Guide

This repository ships four Python distributions:

- `dc43-service-clients`
- `dc43-service-backends`
- `dc43-integrations`
- `dc43` (meta package aggregating the other three)

Each package keeps its own version in a plain `VERSION` file alongside the corresponding
`pyproject.toml`. Use the following workflow to manage releases without forcing synchronized bumps
across the whole stack.

## Branching model

- Do day-to-day development on the `dev` branch. Cut feature branches from `dev` and land them via
  pull requests.
- Protect `main`. Only merge pull requests from `dev` (or dedicated release branches) after they
  pass review. Every merge to `main` is a potential release.
- Let GitHub Actions publish artifacts. Once code lands on `main`, the release workflow decides
  whether any packages need to ship and handles tagging, PyPI uploads, and GitHub Releases without
  extra manual intervention.

## Tagging strategy

1. **Use annotated tags per package.** Prefix tags with the distribution name to avoid collisions
   and to keep the Git history navigable, e.g. `dc43-service-clients-v0.8.0`.
2. **Let CI create the tags.** Update the package version (and changelog if applicable), merge to
   `main`, and allow the release workflow to create and push the annotated tags once publishing
   succeeds. When running the helper script locally you can still create tags yourself, but the
   automation covers the common case.
3. **Keep the meta package independent.** Only create a `dc43-vX.Y.Z` tag when the aggregate
   package actually changes (e.g. dependency pin updates or CLI tweaks). The three component
   packages can evolve on their own cadence.

This mirrors mono-repo projects such as Kubernetes and the AWS SDKs where a single repository hosts
many release trains.

## GitHub Actions automation

### Pull request validation

Every pull request into `dev` or `main` runs the [`ci.yml`](../.github/workflows/ci.yml) workflow. The
pipeline exercises the test suite for each distributable package (`dc43`, `dc43-service-clients`,
`dc43-service-backends`, `dc43-integrations`, and `dc43-contracts-app`) using the same install
commands the release pipeline uses. Keeping the checks green is what allows the protected `main`
branch to accept the merge and eventually cut a release.

### Automated releases

The [`release.yml`](../.github/workflows/release.yml) workflow now runs automatically for every push
to `main` (including merges from `dev`). Manual runs remain available via **Run workflow**. The
pipeline works in three stages:

1. **Plan.** The `determine` job checks out the repository and executes
   [`scripts/release.py`](../scripts/release.py) in dry-run mode. The script compares each package's
   `VERSION` file with the latest annotated tag and the state of PyPI. If the version already exists
   (tag or PyPI), the job fails with a warning so we can bump the version before retrying. Otherwise
   it records a JSON release plan, publishes a summary to the workflow run, and passes the tag names
   to the `release` job.
2. **Build & publish.** The `release` job runs only when at least one package requires publishing. It
   installs dependencies, executes the package-specific test suites, builds wheels/sdists, and uploads
   them to PyPI. Steps are gated per package, so unchanged distributions incur no work. Because the
   same test commands already ran in the PR validation workflow, the release pipeline acts as a final
   verification before packaging and publishing.
3. **Tag & announce.** After all PyPI uploads succeed the workflow reruns `scripts/release.py` with
   `--apply --push` to create and push annotated tags for the release commit. Finally the job creates
   GitHub Releases with the freshly built artifacts.

Because planning happens before any build or publish step, we never release partially—either every
package in the plan succeeds or the workflow fails without leaving dangling tags. The uploaded
`plan.json` artifact captures the exact decision for auditing or debugging.

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
  target commit message already contains `[release]` to keep release commits easy to spot, and when
  targeting `HEAD` it can amend the commit for you if the marker is missing. Use
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
tag/version already exists on PyPI. Use `--json-output <path>` to emit the plan as structured data—the
GitHub Actions workflow relies on this flag. When the automation runs in CI it passes
`--allow-missing-release-marker` so merges from `dev` do not need a special commit message, but we keep
the marker requirement locally to make intentional releases obvious in the history.

## Release checklist

For a package bump (example: `dc43-service-backends`):

1. Update the version in `packages/dc43-service-backends/VERSION`.
2. Update changelog notes (either a shared CHANGELOG or one per package).
3. Commit the changes on `dev` with a descriptive message (adding `[release]` is optional but keeps
   history searchable when you run the helper script locally).
4. Open a PR from `dev` to `main` and wait for review.
5. Merge the PR. The release workflow will determine which packages changed, build them, upload to
   PyPI, create the annotated tags, and publish GitHub Releases automatically.

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
