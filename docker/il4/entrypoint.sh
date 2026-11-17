#!/bin/sh
set -eu
if [ "${JAVA_TOOL_OPTIONS+x}" ] || [ "${JDK_JAVA_OPTIONS+x}" ] || [ "${JAVA_OPTS+x}" ]; then
    echo "Do not inject global JVM options; use reviewed CATALINA_OPTS instead." >&2
    exit 1
fi
# The same provider options are used by the check and by the Tomcat JVM.
(
    . /opt/tomcat/bin/setenv.sh
    java $CATALINA_OPTS -cp '/opt/fips/*:/opt/fips-check' FipsCheck
    java $CATALINA_OPTS -cp '/opt/fips/*:/opt/fips-check:/opt/tomcat/webapps/Monolith/WEB-INF/lib/*' JdbcCheck
)
/opt/semoss-python/bin/python -I -c \
    'import sys, pandas, pyarrow, jsonpickle; assert sys.version_info[:2] == (3, 14); print("PASS: Python 3.14 core imports")'
if [ "${1:-}" = "--check" ]; then
    exit 0
fi
if [ "${1:-}" != "run" ] || [ "$#" -ne 1 ]; then
    echo "Usage: semoss-entrypoint [run|--check]" >&2
    exit 1
fi
for file in /run/secrets/server.bcfks /run/secrets/server.password; do
    if [ ! -r "$file" ] || [ ! -s "$file" ]; then
        echo "Required TLS secret is missing, empty, or unreadable: $file" >&2
        exit 1
    fi
done
# CAC/PIV is required unless a site explicitly sets CAC_PIV_REQUIRED=false (see
# conf/setenv.sh). The connector can only judge a card against the DoD PKI trust
# store a site mounts, so refuse to start without it rather than fall back to
# the JVM's public-CA store, which would accept any publicly issued certificate.
if [ "${CAC_PIV_REQUIRED:-true}" = "true" ]; then
    for file in /run/secrets/cac-truststore.bcfks /run/secrets/cac-truststore.password; do
        if [ ! -r "$file" ] || [ ! -s "$file" ]; then
            echo "CAC/PIV is required but its trust store secret is missing, empty, or unreadable: $file" >&2
            echo "Mount the DoD PKI CA bundle, or set CAC_PIV_REQUIRED=false to run without CAC/PIV." >&2
            exit 1
        fi
    done
    # the password reaches Tomcat through a space-separated JVM option, so keep it to a safe alphabet
    if [ -n "$(tr -d 'A-Za-z0-9._@%+=:,/-' < /run/secrets/cac-truststore.password | tr -d '\n')" ]; then
        echo "/run/secrets/cac-truststore.password may only contain letters, digits and . _ @ % + = : , / -" >&2
        exit 1
    fi
fi
# Shared login sessions (SESSION_STORE=jdbc): validate the settings, write the
# Monolith-only Tomcat descriptor to a private temporary directory, and make sure the
# session table exists. Any failure stops the container rather than letting it run
# with sessions that other containers cannot see. SESSION_STORE_* are read from the
# environment, never from the command line.
case "${SESSION_STORE:-memory}" in
    memory) ;;
    jdbc)
        /opt/semoss-python/bin/python -I /opt/python-check/session_store.py prepare /tmp/session-store
        (
            . /opt/tomcat/bin/setenv.sh
            java $CATALINA_OPTS -cp '/opt/fips/*:/opt/fips-check:/opt/tomcat/webapps/Monolith/WEB-INF/lib/*' SessionStoreInit
        )
        ;;
    *)
        echo "SESSION_STORE must be memory or jdbc" >&2
        exit 1
        ;;
esac
exec /opt/tomcat/bin/catalina.sh run
