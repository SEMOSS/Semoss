#!/bin/sh
CLASSPATH="/opt/accp/*${CLASSPATH:+:$CLASSPATH}"
CATALINA_OPTS="${CATALINA_OPTS:-} -Djava.security.properties==/opt/accp/java.security"
CATALINA_OPTS="$CATALINA_OPTS -Dcom.amazon.corretto.crypto.provider.useExternalLib=true"
CATALINA_OPTS="$CATALINA_OPTS -Djava.library.path=/opt/accp/lib"
CATALINA_OPTS="$CATALINA_OPTS -Dorg.bouncycastle.native.cpu_variant=java"
CATALINA_OPTS="$CATALINA_OPTS -Dorg.sqlite.lib.path=/opt/sqlite -Dorg.sqlite.lib.name=libsqlitejdbc.so"
CATALINA_OPTS="$CATALINA_OPTS -Djavax.net.ssl.trustStore=/opt/accp/cacerts.p12"
CATALINA_OPTS="$CATALINA_OPTS -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStoreProvider=SUN"
CATALINA_OPTS="$CATALINA_OPTS -Djavax.net.ssl.trustStorePassword=changeit"
CATALINA_OPTS="$CATALINA_OPTS -Djava.awt.headless=true -Duser.home=/opt/semosshome"
CATALINA_OPTS="$CATALINA_OPTS --enable-native-access=ALL-UNNAMED"
# CAC/PIV: request (not require) a client certificate on the HTTPS connector -
# see server.xml's clientAuth attribute and docker/il4/README.md's security
# audit logging / CAC-PIV section. Off by default: a request with no client
# certificate is unaffected either way, so this only matters once a site also
# adds a <TrustStore> to SSLHostConfig in server.xml for its own DoD PKI trust
# bundle - clientAuth alone, with no trust store to validate against, can't
# authenticate anyone.
if [ "${CAC_PIV_CLIENT_AUTH:-false}" = "true" ]; then
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.client.auth=want"
else
  CATALINA_OPTS="$CATALINA_OPTS -Dcac.piv.client.auth=false"
fi
export CLASSPATH CATALINA_OPTS
