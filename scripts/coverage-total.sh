#!/usr/bin/env bash
#
# Whole-suite coverage: what fraction of pkg/ the test suite actually exercises.
#
#   scripts/coverage-total.sh           # print the total and the per-package split
#   scripts/coverage-total.sh --badge   # ...and refresh the README badge
#
# This is NOT the gate — `make cover-check` is, and it deliberately measures
# something narrower. The gate credits each package only for what its OWN tests
# reach, which is the right signal for catching a regression: a drop names the
# package whose tests regressed, and integration tests walking the same code
# cannot prop the number back up. The cost of that precision is that it
# understates reality, because most of `wire` is exercised by message/session/
# relay tests rather than its own.
#
# This script reports the other view, via -coverpkg. Informational only: as a
# gate it would let someone delete a package's unit tests while integration
# tests kept the number green, which is the fea80fc failure mode wearing a
# different hat.
#
# Env:
#   COVER_PKGS  packages to measure and attribute across (default ./pkg/...)

set -euo pipefail

COVER_PKGS=${COVER_PKGS:-./pkg/...}
README=${README:-README.md}

badge=no
case "${1:-}" in
	--badge | -b) badge=yes ;;
	--help | -h)
		awk 'NR == 1 { next }
		     /^#/ { sub(/^# ?/, ""); print; next }
		     { exit }' "$0"
		exit 0
		;;
	"") ;;
	*)
		echo "unknown argument: $1 (try --help)" >&2
		exit 2
		;;
esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

if ! go test -count=1 -coverpkg="$COVER_PKGS" -coverprofile="$tmp/cross.out" $COVER_PKGS \
	>"$tmp/log" 2>&1; then
	echo "coverage run failed — fix the tests first:" >&2
	cat "$tmp/log" >&2
	exit 1
fi

total=$(go tool cover -func="$tmp/cross.out" | awk 'END { sub(/%$/, "", $NF); print $NF }')

# Per-package split. One profile block can appear once per test binary that
# linked it, so merge duplicates by block and keep the highest hit count —
# summing the raw lines instead would count a block once per binary and report
# a wildly deflated number.
echo "  package                                            covered"
awk 'NR > 1 {
	if (!($1 in mx) || $3 + 0 > mx[$1]) mx[$1] = $3 + 0
	st[$1] = $2
}
END {
	for (b in mx) {
		pkg = b; sub(":.*", "", pkg); sub("/[^/]*\\.go$", "", pkg)
		tot[pkg] += st[b]
		if (mx[b] > 0) cov[pkg] += st[b]
	}
	for (p in tot) printf "  %-50s %5.1f%%\n", p, (tot[p] ? 100 * cov[p] / tot[p] : 0)
}' "$tmp/cross.out" | sort -k2 -n

echo
echo "  whole-suite total: ${total}%"

[ "$badge" = no ] && exit 0

# Rounded to a whole percent on purpose. The suite's timing-dependent branches
# move the total ~0.1-0.3pp between runs on unchanged code, and a badge carrying
# a decimal would produce a README diff every time anyone regenerated it.
rounded=$(printf '%.0f' "$total")

case $rounded in
	9[0-9] | 100) color=brightgreen ;;
	8[0-9]) color=green ;;
	7[0-9]) color=yellowgreen ;;
	6[0-9]) color=yellow ;;
	5[0-9]) color=orange ;;
	*) color=red ;;
esac

if ! grep -q 'img.shields.io/badge/coverage-' "$README"; then
	echo "no coverage badge found in $README — add one first, this only refreshes it" >&2
	exit 1
fi

# Rewrite just the percentage and colour inside the existing badge URL.
new="https://img.shields.io/badge/coverage-${rounded}%25-${color}.svg"
sed -i.bak -E "s|https://img\.shields\.io/badge/coverage-[0-9.]+%25-[a-z]+\.svg|$new|" "$README"
rm -f "$README.bak"
echo "  badge updated: ${rounded}% (${color}), from a measured ${total}%"
