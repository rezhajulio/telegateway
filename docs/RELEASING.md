# Releasing telegateway

Releases are cut from git tags. Pushing a `v*` tag runs the release CI
job, which publishes to crates.io and attaches release binaries to a
GitHub Release. There is no manual upload step.

## Commit messages

This repo follows [Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `docs:`, `ci:`, `test:`, `refactor:`, `chore:`.
Write the subject in imperative mood, keep it under 72 characters, and
put the why in the body when it isn't obvious from the subject. The
history was converted to this format in October 2026; keep it that way so a future changelog generator can read these prefixes.

## One-time setup

1. Create a crates.io API token: crates.io → your profile → Account
   Settings → API Tokens → New Token. The token needs publish access to
   the `telegateway` crate (or be unscoped for the first publish).
2. Add it as a repository secret: GitHub → telegateway → Settings →
   Secrets and variables → Actions → New repository secret:
   - Name: `CARGO_REGISTRY_TOKEN`
   - Value: the token from step 1.

Without this secret, the `cargo publish` step fails and the release
stops before the GitHub Release is created.

## Cutting a release

1. Bump `version` in `Cargo.toml` (follow semver; every release needs a
   new version; crates.io versions are immutable).
2. Update `CHANGELOG.md` if there is something worth noting. (There is
   no changelog file yet; releases so far are described by the GitHub
   Release notes.)
3. Commit and push to `main`. Wait for CI to go green.
4. Tag and push the tag:
   ```
   git tag v0.2.0
   git push origin v0.2.0
   ```
5. Watch the release workflow. On success you get:
   - `telegateway` `0.2.0` on crates.io (`cargo install telegateway`),
   - a GitHub Release `v0.2.0` with `telegateway-x86_64-unknown-linux-gnu.tar.gz`
     and `telegateway-aarch64-apple-darwin.tar.gz`.

The tag version and the `Cargo.toml` version should match; CI does not
enforce this, it publishes whatever `Cargo.toml` says.

## If something goes wrong

- `cargo publish` runs **before** the GitHub Release is created, so a
  publish failure leaves no half-made release. Fix the cause, bump the
  version if the failed version already reached crates.io, and tag again.
- A version that reached crates.io cannot be overwritten. If it is broken,
  release a fixed version, and optionally `cargo yank` the broken one
  (yanked versions stay downloadable for existing lockfiles but are not
  picked for new resolves).
- To publish without a GitHub Release (e.g. a version bump with no
  binaries), run `cargo publish` from a clean checkout after `cargo login`.
