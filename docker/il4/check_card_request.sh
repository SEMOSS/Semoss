#!/bin/sh
# Does the running server actually ASK the browser for a CAC/PIV card?
#
#   docker/il4/check_card_request.sh <host[:port]>        (default port 8443)
#
# Opens a TLS 1.2 handshake and looks for the CertificateRequest's list of acceptable CA names. A connector that
# never requests a client certificate prints "No client certificate CA names sent", and CAC login cannot work
# behind it. This checks what the server really does; the unit tests only read server.xml. Needs openssl and a
# network path to the container that does not terminate TLS (a TCP/NLB passthrough; an ALB hides the answer).
set -eu
TARGET="${1:?usage: check_card_request.sh <host[:port]>}"
case "$TARGET" in *:*) ADDR="$TARGET" ;; *) ADDR="$TARGET:8443" ;; esac
OUT="$(echo | timeout 20 openssl s_client -tls1_2 -connect "$ADDR" -servername "${ADDR%%:*}" 2>&1 || true)"
if printf '%s' "$OUT" | grep -q 'Acceptable client certificate CA names'; then
  printf '%s\n' "$OUT" | grep -A3 'Acceptable client certificate CA names' | head -4
  echo "OK: the server requests a client certificate."
else
  echo "FAIL: the server does not request a client certificate (no CA list in the TLS handshake); CAC login cannot work." >&2
  exit 1
fi
