#!/usr/bin/env bash
#
# Enforce per-package statement-coverage floors.
#
#   scripts/check-coverage.sh                      # check against coverage-floors.txt
#   scripts/check-coverage.sh --update             # re-measure; floors may only rise
#   scripts/check-coverage.sh --update --allow-lower  # ...and may also fall
#
# Why floors and not a single global percentage: a global number lets coverage
# drain out of the relay while the wire codec's tests hold the average up. Why a
# tolerance and not an exact match: `session` and `relay` have timing-dependent
# teardown and error branches that don't run every time, so their measured
# coverage moves ~0.4% run to run on the same commit (verified across macOS
# arm64 and linux amd64/arm64). Floors are recorded at the bottom of the
# observed band and compared with TOLERANCE of slack on top; a real regression
# is far larger than that, because it is whole functions going unrun.
#
# Why --update refuses to lower: rewriting the floors is the obvious thing to
# reach for when the check fails, so if it silently accepted a lower number the
# gate would launder away every regression it caught. Lowering has to be asked
# for by name, and explained in the commit.
#
# Env:
#   COVER_PKGS          packages to measure        (default ./pkg/...)
#   COVERAGE_TOLERANCE  slack in percentage points (default 0.5)
#   COVERAGE_RUNS       samples taken by --update  (default 3)
#   FLOORS_FILE         floors file                (default coverage-floors.txt)

set -euo pipefail

COVER_PKGS=${COVER_PKGS:-./pkg/...}
COVERAGE_TOLERANCE=${COVERAGE_TOLERANCE:-0.5}
FLOORS_FILE=${FLOORS_FILE:-coverage-floors.txt}
# Separate Go modules, measured with their own `go test ./...` and folded into
# the same floors file. `go test ./pkg/...` from the root cannot reach them, so
# without this the one distributed DiscoveryStore backend — and the relay-etcd
# binary the multi-instance deployments run — is gated by nothing. Package
# names in the profile are full import paths, so one floors file stays
# unambiguous across modules.
COVER_SUBMODULES=${COVER_SUBMODULES:-pkg/relay/discovery/etcd}
# --update samples the suite this many times and keeps the per-package minimum,
# so a floor is the bottom of the observed band rather than whichever value one
# lucky run happened to produce. Checking only ever needs one run.
COVERAGE_RUNS=${COVERAGE_RUNS:-3}

mode=check
allow_lower=no
for arg in "$@"; do
	case "$arg" in
		--update | -u) mode=update ;;
		--allow-lower) allow_lower=yes ;;
		--help | -h)
			# The header comment block, minus the shebang: everything until the
			# first line that isn't a comment. Beats a hardcoded line range,
			# which silently truncates the moment the block grows.
			awk 'NR == 1 { next }
			     /^#/ { sub(/^# ?/, ""); print; next }
			     { exit }' "$0"
			exit 0
			;;
		*)
			echo "unknown argument: $arg (try --help)" >&2
			exit 2
			;;
	esac
done

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

runs=1
[ "$mode" = update ] && runs=$COVERAGE_RUNS

for n in $(seq 1 "$runs"); do
	[ "$runs" -gt 1 ] && echo "sampling coverage, run $n/$runs..." >&2
	# -count=1 because a cached test result reports the coverage of the cached
	# run, which may predate the change being checked.
	if ! go test -count=1 -cover $COVER_PKGS >"$tmp/raw" 2>"$tmp/err"; then
		echo "coverage run failed — fix the tests first:" >&2
		cat "$tmp/raw" "$tmp/err" >&2
		exit 1
	fi
	for m in $COVER_SUBMODULES; do
		if ! (cd "$m" && go test -count=1 -cover ./...) >"$tmp/sub" 2>"$tmp/err"; then
			echo "coverage run failed in $m — fix the tests first:" >&2
			cat "$tmp/sub" "$tmp/err" >&2
			exit 1
		fi
		cat "$tmp/sub" >>"$tmp/raw"
	done
	# `ok <pkg> <time> coverage: N% of statements` and the
	# testless-but-instrumented `<pkg> coverage: N% of statements` both land
	# here; `[no test files]` does not.
	awk '
		/coverage: [0-9.]+% of statements/ {
			pkg = ""; pct = ""
			for (i = 1; i <= NF; i++) {
				if ($i ~ /^github\.com\//) pkg = $i
				if ($i == "coverage:") { pct = $(i + 1); sub(/%$/, "", pct) }
			}
			if (pkg != "" && pct != "") print pkg, pct
		}
	' "$tmp/raw" >>"$tmp/samples"
done

# Keep the lowest sample per package.
awk '{ if (!($1 in min) || $2 + 0 < min[$1] + 0) min[$1] = $2 }
     END { for (p in min) print p, min[p] }' "$tmp/samples" | sort >"$tmp/measured"

if [ ! -s "$tmp/measured" ]; then
	echo "no coverage reported for $COVER_PKGS — nothing to check" >&2
	exit 1
fi

if [ "$mode" = update ]; then
	# The floors ratchet one way. Rewriting them is the obvious thing to try when
	# `cover-check` fails, and if that silently accepted the lower number the gate
	# would launder every regression it caught. Keep the existing floor wherever
	# it is higher, and make lowering an explicit, separate decision.
	if [ -f "$FLOORS_FILE" ] && [ "$allow_lower" = no ]; then
		grep -v -e '^#' -e '^[[:space:]]*$' "$FLOORS_FILE" | sort >"$tmp/floors"
		join -a2 -e MISSING -o '0,1.2,2.2' "$tmp/floors" "$tmp/measured" >"$tmp/merged"
		awk '
			{
				pkg = $1; floor = $2; measured = $3
				if (floor != "MISSING" && measured + 0 < floor + 0) {
					printf "  refusing to lower %-46s %s%% -> %s%%\n", pkg, floor, measured > "/dev/stderr"
					lowered++
					print pkg, floor          # keep the higher, existing floor
				} else {
					print pkg, measured
				}
			}
			END {
				if (lowered) {
					print "" > "/dev/stderr"
					print "Kept " lowered " floor(s) at their existing value. Coverage went DOWN there." > "/dev/stderr"
					print "Add the missing tests, or if the drop is legitimate (you deleted tested" > "/dev/stderr"
					print "code, or moved it to another package), rerun with:" > "/dev/stderr"
					print "    ./scripts/check-coverage.sh --update --allow-lower" > "/dev/stderr"
					print "and say in the commit message why the floor moved down." > "/dev/stderr"
				}
			}
		' "$tmp/merged" >"$tmp/ratcheted"
		mv "$tmp/ratcheted" "$tmp/measured"
	fi
	{
		echo "# Per-package statement-coverage floors, enforced by \`make cover-check\`."
		echo "#"
		echo "# Regenerate with \`make cover-floors\` — never hand-edit. Raising a floor is"
		echo "# a deliberate act: do it in the same commit as the tests that earned it, so"
		echo "# the diff shows what was covered and why."
		echo "#"
		echo "# Each floor is the lowest of ${COVERAGE_RUNS} sampled runs, and the check allows a"
		echo "# further ${COVERAGE_TOLERANCE}pp of slack. See scripts/check-coverage.sh."
		echo "#"
		echo "# <package> <floor %>"
		cat "$tmp/measured"
	} >"$FLOORS_FILE"
	echo "wrote $(wc -l <"$tmp/measured" | tr -d ' ') floors to $FLOORS_FILE"
	exit 0
fi

if [ ! -f "$FLOORS_FILE" ]; then
	echo "$FLOORS_FILE not found — create it with: make cover-floors" >&2
	exit 1
fi

grep -v -e '^#' -e '^[[:space:]]*$' "$FLOORS_FILE" | sort >"$tmp/floors"

join -a1 -a2 -e MISSING -o '0,1.2,2.2' "$tmp/floors" "$tmp/measured" |
	awk -v tol="$COVERAGE_TOLERANCE" '
	{
		pkg = $1; floor = $2; measured = $3
		if (measured == "MISSING") {
			# In the floors file but not measured: package renamed or deleted.
			printf "  gone     %-52s was %s%%\n", pkg, floor > "/dev/stderr"
			stale++
			next
		}
		if (floor == "MISSING") {
			printf "  NEW      %-52s %s%% (no floor recorded)\n", pkg, measured
			new++
			next
		}
		if (measured + tol < floor) {
			printf "  DROPPED  %-52s %s%% < %s%% floor\n", pkg, measured, floor
			failed++
			next
		}
		if (measured > floor + tol) {
			printf "  raised   %-52s %s%% > %s%% floor\n", pkg, measured, floor
			raised++
			next
		}
		printf "  ok       %-52s %s%%\n", pkg, measured
	}
	END {
		print ""
		if (stale)  print "note: " stale " package(s) in the floors file no longer report coverage; run `make cover-floors`."
		if (raised) print "note: " raised " package(s) now exceed their floor. Run `make cover-floors` to lock the gain in."
		if (new) {
			print "FAIL: " new " package(s) have no recorded floor."
			print "      Run `make cover-floors` and commit the result, so this package"
			print "      cannot quietly lose coverage later."
		}
		if (failed) {
			print "FAIL: " failed " package(s) dropped below their coverage floor."
			print "      Untested code is dead code: add the missing test, or delete the"
			print "      code it would have covered. `make cover-gaps COVER_PKGS=<pkg>`"
			print "      lists the functions no test reaches."
		}
		if (failed || new) exit 1
		print "All packages at or above their coverage floor (tolerance " tol "pp)."
	}
'
