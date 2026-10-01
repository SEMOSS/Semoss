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
export CLASSPATH CATALINA_OPTS
