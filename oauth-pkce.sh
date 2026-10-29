#!/bin/sh
# Called by Lightroom on macOS. No credentials are passed on the command line.
set -eu
umask 077
set -C
exec 3> "$1"
complete=false
trap '[ "$complete" = true ] || rm -f -- "$1"' EXIT
verifier=$(/usr/bin/openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\r\n')
state=$(/usr/bin/openssl rand -base64 32 | tr '+/' '-_' | tr -d '=\r\n')
challenge=$(printf '%s' "$verifier" | /usr/bin/openssl dgst -sha256 -binary | /usr/bin/openssl base64 -A | tr '+/' '-_' | tr -d '=\r\n')
[ "${#verifier}" = 43 ] && [ "${#state}" = 43 ] && [ "${#challenge}" = 43 ]
printf '{"verifier":"%s","state":"%s","challenge":"%s"}' "$verifier" "$state" "$challenge" >&3
complete=true
