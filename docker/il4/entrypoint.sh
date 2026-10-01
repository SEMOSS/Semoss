#!/bin/sh
set -eu
echo "WARNING: Experimental/nonvalidated ACCP runtime; SunJSSE and SunJCE PBKDF2 are not a validated FIPS path. No certificate coverage is claimed." >&2
if [ "$#" -ne 1 ] || { [ "$1" != "--check" ] && [ "$1" != "run" ]; }; then
    echo "Usage: semoss-entrypoint [run|--check]" >&2
    exit 1
fi
if [ "$1" = "run" ] && [ "${SEMOSS_ALLOW_NONVALIDATED_ACCP:-}" != "true" ]; then
    echo "Development-only startup requires explicit SEMOSS_ALLOW_NONVALIDATED_ACCP=true." >&2
    exit 1
fi
if [ "${JAVA_TOOL_OPTIONS+x}" ] || [ "${JDK_JAVA_OPTIONS+x}" ] || [ "${JAVA_OPTS+x}" ]; then
    echo "Do not inject global JVM options; use reviewed CATALINA_OPTS instead." >&2
    exit 1
fi
# The same provider options are used by the check and by the Tomcat JVM.
(
    . /opt/tomcat/bin/setenv.sh
    java $CATALINA_OPTS -cp '/opt/accp/*:/opt/accp-check' AccpCheck
    java $CATALINA_OPTS -cp '/opt/accp/*:/opt/accp-check:/opt/tomcat/webapps/Monolith/WEB-INF/lib/*' JdbcCheck
)
/opt/semoss-python/bin/python -I -c \
    'import sys, pandas, pyarrow, jsonpickle; assert sys.version_info[:2] == (3, 14); print("PASS: Python 3.14 core imports")'
if [ "${1:-}" = "--check" ]; then
    exit 0
fi
for file in /run/secrets/server.p12 /run/secrets/server.password; do
    if [ ! -r "$file" ] || [ ! -s "$file" ]; then
        echo "Required TLS secret is missing, empty, or unreadable: $file" >&2
        exit 1
    fi
done
exec /opt/tomcat/bin/catalina.sh run
