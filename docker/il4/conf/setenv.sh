#!/bin/sh
CLASSPATH="/opt/fips/*${CLASSPATH:+:$CLASSPATH}"
CATALINA_OPTS="${CATALINA_OPTS:-} -Djava.security.properties==/opt/fips/java.security"
CATALINA_OPTS="$CATALINA_OPTS -Dorg.bouncycastle.fips.approved_only=true"
CATALINA_OPTS="$CATALINA_OPTS -Dorg.bouncycastle.native.cpu_variant=java"
CATALINA_OPTS="$CATALINA_OPTS -Dorg.sqlite.lib.path=/opt/sqlite -Dorg.sqlite.lib.name=libsqlitejdbc.so"
CATALINA_OPTS="$CATALINA_OPTS -Djavax.net.ssl.trustStore=/opt/fips/cacerts.bcfks"
CATALINA_OPTS="$CATALINA_OPTS -Djavax.net.ssl.trustStoreType=BCFKS -Djavax.net.ssl.trustStoreProvider=BCFIPS"
CATALINA_OPTS="$CATALINA_OPTS -Djavax.net.ssl.trustStorePassword=changeit"
CATALINA_OPTS="$CATALINA_OPTS -Djava.awt.headless=true -Duser.home=/opt/semosshome"
CATALINA_OPTS="$CATALINA_OPTS --enable-native-access=ALL-UNNAMED"
# CAC/PIV is REQUIRED by default for IL4. The connector asks the browser for a
# client certificate (clientAuth="want") and validates whatever it presents
# against the DoD PKI trust store mounted at /run/secrets/cac-truststore.bcfks
# (server.xml reads the cac.piv.* properties below). It stays at "want", not
# "true", on purpose: a connector that insists on the certificate ends the TLS
# handshake, and the browser then shows its own error page with no way to tell
# the user what to do. Monolith's CACRequiredFilter (enabled by
# cac.piv.required) makes the requirement instead - a request with no
# validated certificate never reaches the login page; browsers are sent to a
# "CAC required" page with a link to try again. The entrypoint refuses to start
# without the trust store, so this cannot silently fall back to "any client".
#
#   CAC_PIV_REQUIRED=true   (default) require a CAC/PIV certificate
#   CAC_PIV_REQUIRED=false  do not require one (local development, or a site
#                           that authenticates users some other way). Set this
#                           explicitly: it re-opens the login page to anyone.
#   CAC_PIV_CLIENT_AUTH=true with CAC_PIV_REQUIRED=false requests a certificate
#                           without requiring one (CACFilter/PIVFilter then
#                           authenticate a user who has a card, others log in
#                           as before).
CAC_PIV_REQUIRED="${CAC_PIV_REQUIRED:-true}"
case "$CAC_PIV_REQUIRED" in
  true|false) ;;
  *) echo "CAC_PIV_REQUIRED must be true or false" >&2; return 1 2>/dev/null || exit 1 ;;
esac
if [ "$CAC_PIV_REQUIRED" = "true" ] || [ "${CAC_PIV_CLIENT_AUTH:-false}" = "true" ]; then
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.client.auth=want"
  # the DoD PKI roots and intermediates the connector accepts a card from; a
  # BCFKS keystore of public CA certificates, never the JVM's public-CA store
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.truststore.file=/run/secrets/cac-truststore.bcfks"
  CAC_PIV_TRUSTSTORE_PASSWORD=""
  if [ -r /run/secrets/cac-truststore.password ]; then
    CAC_PIV_TRUSTSTORE_PASSWORD="$(cat /run/secrets/cac-truststore.password)"
  fi
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.truststore.password=$CAC_PIV_TRUSTSTORE_PASSWORD"
else
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.client.auth=false"
  # no client certificate is requested, so the connector's trust store is never
  # used; point it at the JVM's own store, which always exists
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.truststore.file=/opt/fips/cacerts.bcfks"
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.truststore.password=changeit"
fi
CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.required=$CAC_PIV_REQUIRED"
# Login sessions. By default each lives in the memory of the one container that
# created it, so a deployment with a single container needs nothing. With several
# containers set SESSION_STORE=jdbc: the entrypoint (session_store.py) writes a
# Monolith-only context descriptor, with the database settings, into a private
# directory and this points Tomcat at it. Only the directory name is passed here;
# no database setting or secret goes through the JVM options, which catalina.sh
# re-parses with eval. See README "Sessions across several containers".
case "${SESSION_STORE:-memory}" in
  memory) ;;
  jdbc) CATALINA_OPTS="$CATALINA_OPTS -Dsession.xml.base=/tmp/session-store" ;;
  *) echo "SESSION_STORE must be memory or jdbc" >&2; return 1 2>/dev/null || exit 1 ;;
esac
export CLASSPATH CATALINA_OPTS
