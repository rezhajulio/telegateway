#!/usr/bin/env bash
# Print the CHANGELOG.md section for a release tag (e.g. v0.1.3) to stdout.
#
# The release CI job feeds this into softprops/action-gh-release body_path
# so the GitHub Release gets the same notes as the changelog section. The
# "## [x.y.z] - date" heading and trailing blank lines are stripped; the
# release page already shows the tag and date.
#
# Exits non-zero when the tag has no changelog section, so a release can
# never be cut with empty notes.
set -euo pipefail

tag="${1:?usage: release-notes.sh vX.Y.Z}"
version="${tag#v}"

if [[ ! -f CHANGELOG.md ]]; then
  echo "error: CHANGELOG.md not found" >&2
  exit 1
fi

# Keep a Changelog orders sections newest first, so the section runs from
# its "## [version]" heading to the next "## [" heading.
section=$(awk -v ver="$version" '
  $0 ~ "^## \\[" ver "\\] " { found = 1; next }
  found && /^## \[/         { exit }
  found                     { print }
' CHANGELOG.md)

if [[ -z "$section" ]]; then
  echo "error: no CHANGELOG.md section for [$version]; add one before tagging" >&2
  exit 1
fi

# Trim leading and trailing blank lines.
printf '%s\n' "$section" | sed -e '/./,$!d'
