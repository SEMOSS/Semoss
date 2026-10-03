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
export CLASSPATH CATALINA_OPTS
