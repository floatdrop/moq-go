#!/bin/sh
# matrix.sh — run the relay built from this repo against several third-party
# draft-18 MoQT test clients, over each transport they support, and print a
# pass/skip/fail matrix.
#
# Assumes the relay image (moq-relay:latest, overridable via RELAY_IMAGE) is
# already built and certs exist under interop/certs — the `make interop-matrix`
# target arranges both. Clients are pulled on demand by docker compose.
#
# Override the client set via the CLIENTS env var; each line is
#   <name>|<image>|<space-separated transports>[|advisory]
# where a transport is "quic" or "webtransport". A 4th "advisory" field marks a
# client whose failures are NOT attributed to the relay (e.g. the client diverges
# from the draft we track): its row is still shown, but it does not affect the
# exit status.
set -eu

COMPOSE="docker compose -f interop/docker-compose.yml"
RELAY_IMAGE="${RELAY_IMAGE:-moq-relay:latest}"

# moq-rs-draft-18 is advisory: its moq-transport library enforces a Maximum
# Request ID that draft-18 (the text we track) removed, so it reports "too many
# requests" before our relay does anything — a client/draft-version divergence,
# not a relay fault. See STATUS.md.
CLIENTS="${CLIENTS:-\
moq-dev-rs|ghcr.io/englishm/moq-interop-runner-moq-dev-rs-client:latest|quic webtransport
moq-dev-js|ghcr.io/englishm/moq-interop-runner-moq-dev-js-client:latest|webtransport
moq-rs-draft-18|ghcr.io/englishm/moq-interop-runner-moq-test-client-draft-18:latest|quic webtransport|advisory}"

strip_ansi() { sed -E 's/\x1b\[[0-9;]*m//g'; }

# Map a transport name to the RELAY_URL the client should dial.
relay_url() {
	case "$1" in
	quic) echo "moqt://relay:4443" ;;
	webtransport) echo "https://relay:4443/moq" ;;
	*) echo "moqt://relay:4443" ;;
	esac
}

printf '%-18s %-13s %5s %5s %5s %5s  %s\n' CLIENT TRANSPORT PASS SKIP FAIL TOTAL RESULT
printf '%-18s %-13s %5s %5s %5s %5s  %s\n' ------ --------- ---- ---- ---- ----- ------

overall=0

# Iterate clients. IFS split on newline only so the transport list (with
# spaces) stays in one field.
OLDIFS=$IFS
IFS='
'
for line in $CLIENTS; do
	IFS='|'
	# shellcheck disable=SC2086
	set -- $line
	IFS=$OLDIFS
	name=$1
	image=$2
	transports=$3
	advisory=${4:-}

	for t in $transports; do
		url=$(relay_url "$t")
		log=$(RELAY_IMAGE="$RELAY_IMAGE" CLIENT_IMAGE="$image" \
			MOQT_TRANSPORT="$t" RELAY_URL="$url" \
			$COMPOSE up --abort-on-container-exit 2>&1 || true)
		$COMPOSE down -v >/dev/null 2>&1 || true

		tap=$(printf '%s\n' "$log" | strip_ansi | sed -n 's/^test-client-1[ ]*| //p')
		oks=$(printf '%s\n' "$tap" | grep -cE '^ok [0-9]' || true)
		skip=$(printf '%s\n' "$tap" | grep -cE '# SKIP' || true)
		fail=$(printf '%s\n' "$tap" | grep -cE '^not ok [0-9]' || true)
		pass=$((oks - skip))
		total=$((pass + skip + fail))

		bad=0
		if [ "$total" -eq 0 ]; then
			result="ERROR (no TAP — client image or connection failed)"
			bad=1
		elif [ "$fail" -gt 0 ]; then
			result="FAIL"
			bad=1
		elif [ "$skip" -gt 0 ]; then
			result="OK (with skips)"
		else
			result="OK"
		fi
		if [ -n "$advisory" ]; then
			result="$result [advisory]"
		elif [ "$bad" -eq 1 ]; then
			overall=1
		fi

		printf '%-18s %-13s %5d %5d %5d %5d  %s\n' \
			"$name" "$t" "$pass" "$skip" "$fail" "$total" "$result"
	done
done

echo
echo "PASS = test exercised and passed   SKIP = client opted out (its API/limitation)"
echo "FAIL = test ran and failed         ERROR = client never produced TAP"
echo "[advisory] rows are shown for visibility but do not affect the exit status."
echo
echo "For the full registered matrix (all clients, classified vs the target draft),"
echo "use the runner:  cd ../moq-interop-runner && make interop-relay RELAY=moq-go"

exit $overall
