#!/usr/bin/env bash
#
# Whole-suite coverage across both Go modules, and the artifacts the README
# badge and the published report are built from.
#
#   scripts/coverage.sh              # per-package table + total
#   scripts/coverage.sh --site DIR   # ...and write the report site into DIR
#
# Measured with -coverpkg, so a package is credited for every line the suite
# exercises rather than only for what its own tests reach. The difference is
# large and uneven — most of `wire` is driven by message/session/relay tests,
# not its own — which is why this is the published figure.
#
# This is NOT a gate. Coverage stopped being enforced when the per-package
# floors were removed; the badge and the published report are the signal now,
# and they are read by a human rather than by CI.
#
# Both modules are measured. `go test ./pkg/...` cannot reach
# pkg/relay/discovery/etcd — it has its own go.mod — so it runs separately and
# its profile is concatenated onto the root one. Blocks are keyed by full import
# path, so the two cannot collide.
#
# Percentages are computed from the profile in awk rather than with
# `go tool cover -func`, deliberately: -func resolves every import path to a
# source file, and the root module does not require the etcd module, so it would
# fail on precisely the blocks we went out of our way to fold in. The awk also
# has to dedupe — one block appears once per test binary that linked it, and
# summing the raw lines would count its statements repeatedly and report a
# wildly deflated number.
#
# Env:
#   COVER_PKGS        packages to measure and attribute across (default ./pkg/...)
#   COVER_SUBMODULES  extra modules to fold in (default pkg/relay/discovery/etcd)

set -euo pipefail

COVER_PKGS=${COVER_PKGS:-./pkg/...}
COVER_SUBMODULES=${COVER_SUBMODULES:-pkg/relay/discovery/etcd}

site=""
case "${1:-}" in
	--help | -h)
		awk 'NR == 1 { next }
		     /^#/ { sub(/^# ?/, ""); print; next }
		     { exit }' "$0"
		exit 0
		;;
	--site)
		site=${2:-}
		if [ -z "$site" ]; then
			echo "--site needs a directory" >&2
			exit 2
		fi
		;;
	"") ;;
	*)
		echo "unknown argument: $1 (try --help)" >&2
		exit 2
		;;
esac

repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# shellcheck disable=SC2086 # COVER_PKGS is a deliberate multi-pattern list.
if ! go test -count=1 -coverpkg="$COVER_PKGS" -coverprofile="$tmp/root.out" $COVER_PKGS \
	>"$tmp/log" 2>&1; then
	echo "coverage run failed (root module) — fix the tests first:" >&2
	cat "$tmp/log" >&2
	exit 1
fi
cp "$tmp/root.out" "$tmp/merged.out"

for m in $COVER_SUBMODULES; do
	name=$(basename "$m")
	if ! (cd "$m" && go test -count=1 -coverpkg=./... -coverprofile="$tmp/$name.out" ./...) \
		>"$tmp/log" 2>&1; then
		echo "coverage run failed ($m) — fix the tests first:" >&2
		cat "$tmp/log" >&2
		exit 1
	fi
	tail -n +2 "$tmp/$name.out" >>"$tmp/merged.out"
done

awk 'NR > 1 {
	if (!($1 in mx) || $3 + 0 > mx[$1]) mx[$1] = $3 + 0
	st[$1] = $2
}
END {
	for (b in mx) {
		pkg = b; sub(":.*", "", pkg); sub("/[^/]*\\.go$", "", pkg)
		tot[pkg] += st[b]; T += st[b]
		if (mx[b] > 0) { cov[pkg] += st[b]; C += st[b] }
	}
	for (p in tot) printf "%s\t%.1f\n", p, (tot[p] ? 100 * cov[p] / tot[p] : 0)
	printf "TOTAL\t%.1f\n", (T ? 100 * C / T : 0)
}' "$tmp/merged.out" >"$tmp/raw.tsv"

total=$(awk -F'\t' '$1 == "TOTAL" { print $2 }' "$tmp/raw.tsv")
awk -F'\t' '$1 != "TOTAL"' "$tmp/raw.tsv" | sort -t"$(printf '\t')" -k2 -n >"$tmp/pkgs.tsv"

echo "  package                                            covered"
awk -F'\t' '{ printf "  %-50s %5.1f%%\n", $1, $2 }' "$tmp/pkgs.tsv"
echo
echo "  whole-suite total: ${total}%"

[ -n "$site" ] || exit 0

mkdir -p "$site"
site=$(cd "$site" && pwd)

# One HTML report per module: `go tool cover -html` needs to resolve each import
# path to a file, which only works from inside the module that owns it.
go tool cover -html="$tmp/root.out" -o "$site/root.html"
for m in $COVER_SUBMODULES; do
	name=$(basename "$m")
	(cd "$m" && go tool cover -html="$tmp/$name.out" -o "$site/$name.html")
done

color=$(awk -v p="$total" 'BEGIN {
	if (p >= 90) print "brightgreen"
	else if (p >= 80) print "green"
	else if (p >= 70) print "yellowgreen"
	else if (p >= 60) print "yellow"
	else if (p >= 50) print "orange"
	else print "red"
}')
printf '{"schemaVersion":1,"label":"coverage","message":"%s%%","color":"%s"}\n' \
	"$total" "$color" >"$site/coverage.json"

{
	cat <<-HTML
		<!doctype html>
		<meta charset="utf-8">
		<meta name="viewport" content="width=device-width, initial-scale=1">
		<title>moq-go coverage</title>
		<style>
		  :root { color-scheme: light dark; }
		  body { font: 16px/1.5 system-ui, sans-serif; margin: 2rem auto; max-width: 46rem; padding: 0 1rem; }
		  h1 { font-size: 1.3rem; }
		  .total { font-size: 2.5rem; font-weight: 600; }
		  table { border-collapse: collapse; width: 100%; margin: 1.5rem 0; }
		  th, td { text-align: left; padding: .35rem .5rem; border-bottom: 1px solid #8883; }
		  td.pct { text-align: right; font-variant-numeric: tabular-nums; }
		  code, td:first-child { font-family: ui-monospace, monospace; font-size: .9em; }
		  p.note { color: #8889; font-size: .9em; }
		</style>
		<h1>moq-go coverage</h1>
		<p class="total">${total}%</p>
		<p>Whole-suite statement coverage measured with <code>-coverpkg</code>, across
		both modules. Line-by-line reports:
		<a href="root.html">root module</a>, <a href="etcd.html">etcd submodule</a>.</p>
		<table>
		<tr><th>package</th><th class="pct">covered</th></tr>
	HTML
	awk -F'\t' '{ printf "<tr><td>%s</td><td class=\"pct\">%.1f%%</td></tr>\n", $1, $2 }' \
		"$tmp/pkgs.tsv"
	cat <<-HTML
		</table>
		<p class="note">Not a gate — nothing in CI fails on a drop. Regenerate locally with
		<code>make cover-site</code>.</p>
	HTML
} >"$site/index.html"

echo "  site written to ${site}"
