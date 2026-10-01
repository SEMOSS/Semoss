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
exec /opt/tomcat/bin/catalina.sh run
