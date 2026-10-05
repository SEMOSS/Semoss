#!/usr/bin/env bash
#
# Migrates one SEMOSS H2 database file from the 1.4.200 on-disk format
# (MVStore "format 1") to 2.2.220 ("format 3"). H2 2.x refuses to open a
# 1.4.200 .mv.db file directly ("Unsupported database file version or
# invalid file header") - this follows H2's own documented upgrade path:
# dump the old database to SQL with the OLD jar's Script tool, then replay
# that SQL into a brand-new file with the NEW jar's RunScript tool.
#
# Usage:
#   ./migrate-h2-database.sh <db-base-path> [old-h2-jar] [new-h2-jar]
#
#   <db-base-path>  Path to the database WITHOUT the .mv.db extension, e.g.
#                   /opt/semoss/db/MyEngine/database or
#                   /opt/semoss/db/MyEngine/audit_log_database
#   [old-h2-jar]    Path to h2-1.4.200.jar. Defaults to $OLD_H2_JAR, then to
#                   the standard Maven local-repo location; if neither exists
#                   and `mvn` is on PATH, it is fetched automatically.
#   [new-h2-jar]    Path to h2-2.2.220.jar. Same default/fetch behavior via
#                   $NEW_H2_JAR.
#
# What it does, in order (see README.md in this directory for the full
# write-up, including how to find every .mv.db file in a deployment):
#   1. Refuses to run if <db-base-path>.mv.db doesn't exist, or if a backup
#      from a previous attempt already exists at
#      <db-base-path>.mv.db.pre-2.2.220 (never overwrites a backup - if a
#      prior run failed partway, move that backup back into place by hand
#      first, or remove it once you've confirmed it's no longer needed).
#   2. Dumps the OLD database to <db-base-path>.migration.sql with
#      h2-1.4.200.jar's bundled org.h2.tools.Script.
#   3. Moves the original .mv.db (and .trace.db, if H2 left one behind) aside
#      to <db-base-path>.mv.db.pre-2.2.220 / .trace.db.pre-2.2.220 - this is
#      the rollback path if anything below fails.
#   4. Replays the .sql script into a brand-new file at the original path
#      with h2-2.2.220.jar's bundled org.h2.tools.RunScript.
#   5. Re-runs the per-table "-- N +/- SELECT COUNT(*) FROM SCHEMA.TABLE;"
#      assertions that H2's own Script tool embeds in the dump, against the
#      new database, so a silent partial RunScript failure can't pass as a
#      success.
#
# Nothing here deletes the .migration.sql dump or the .pre-2.2.220 backup -
# that's left to the operator once the migrated deployment has been
# confirmed good.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
	echo "Usage: $0 <db-base-path> [old-h2-jar] [new-h2-jar]" >&2
	exit 2
fi

DB_BASE_PATH="$1"
OLD_H2_JAR="${2:-${OLD_H2_JAR:-$HOME/.m2/repository/com/h2database/h2/1.4.200/h2-1.4.200.jar}}"
NEW_H2_JAR="${3:-${NEW_H2_JAR:-$HOME/.m2/repository/com/h2database/h2/2.2.220/h2-2.2.220.jar}}"

fetch_jar_if_missing() {
	local jar_path="$1" version="$2"
	if [[ -f "$jar_path" ]]; then
		return 0
	fi
	if ! command -v mvn >/dev/null 2>&1; then
		echo "Missing $jar_path and 'mvn' is not on PATH to fetch it - pass an explicit jar path." >&2
		exit 1
	fi
	echo "Fetching com.h2database:h2:$version into the local Maven repo..."
	# run from a directory with no pom.xml so this resolves against Maven
	# Central directly instead of picking up this project's own pom as context
	(cd "$(mktemp -d)" && mvn -q dependency:get -Dartifact=com.h2database:h2:"$version")
}

fetch_jar_if_missing "$OLD_H2_JAR" "1.4.200"
fetch_jar_if_missing "$NEW_H2_JAR" "2.2.220"

MV_DB="$DB_BASE_PATH.mv.db"
TRACE_DB="$DB_BASE_PATH.trace.db"
BACKUP_MV_DB="$DB_BASE_PATH.mv.db.pre-2.2.220"
BACKUP_TRACE_DB="$DB_BASE_PATH.trace.db.pre-2.2.220"
DUMP_SQL="$DB_BASE_PATH.migration.sql"

if [[ ! -f "$MV_DB" ]]; then
	echo "No such database file: $MV_DB" >&2
	exit 1
fi
if [[ -f "$BACKUP_MV_DB" ]]; then
	echo "A backup from a previous attempt already exists: $BACKUP_MV_DB" >&2
	echo "Move it back into place (or remove it) before re-running." >&2
	exit 1
fi

echo "==> Dumping $MV_DB (H2 1.4.200 format) to $DUMP_SQL"
# ACCESS_MODE_DATA=r opens the file read-only at the storage layer - without
# it, H2 can still write to the file as a side effect of its normal startup/
# recovery handling even on a connection attempt that ultimately fails,
# which would silently corrupt the original before it's ever backed up.
java -cp "$OLD_H2_JAR" org.h2.tools.Script \
	-url "jdbc:h2:nio:$DB_BASE_PATH;ACCESS_MODE_DATA=r" -user sa -password "" -script "$DUMP_SQL"

echo "==> Backing up the 1.4.200 file(s)"
mv "$MV_DB" "$BACKUP_MV_DB"
if [[ -f "$TRACE_DB" ]]; then
	mv "$TRACE_DB" "$BACKUP_TRACE_DB"
fi

echo "==> Replaying $DUMP_SQL into a fresh H2 2.2.220 database at $DB_BASE_PATH"
if ! java -cp "$NEW_H2_JAR" org.h2.tools.RunScript \
	-url "jdbc:h2:nio:$DB_BASE_PATH" -user sa -password "" -script "$DUMP_SQL"; then
	echo "RunScript failed - rolling back to the 1.4.200 file so the original is undisturbed." >&2
	rm -f "$MV_DB"
	mv "$BACKUP_MV_DB" "$MV_DB"
	if [[ -f "$BACKUP_TRACE_DB" ]]; then
		mv "$BACKUP_TRACE_DB" "$TRACE_DB"
	fi
	exit 1
fi

echo "==> Verifying row counts against the assertions H2 embedded in the dump"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSERTIONS_FILE="$(mktemp)"
grep -oE -- '-- [0-9]+ \+/- SELECT COUNT\(\*\) FROM [^;]+;' "$DUMP_SQL" \
	| sed -E 's/^-- ([0-9]+) \+\/- (SELECT COUNT\(\*\) FROM [^;]+);/\1|\2/' > "$ASSERTIONS_FILE"
if [[ ! -s "$ASSERTIONS_FILE" ]]; then
	echo "No row-count assertions found in the dump (an empty database?) - skipping count verification."
elif ! java -cp "$NEW_H2_JAR" "$SCRIPT_DIR/VerifyRowCounts.java" \
	"jdbc:h2:nio:$DB_BASE_PATH" "$ASSERTIONS_FILE"; then
	echo "Row-count verification failed - inspect the output above before trusting the migration." >&2
	echo "The original 1.4.200 file is still safe at $BACKUP_MV_DB" >&2
	exit 1
fi
rm -f "$ASSERTIONS_FILE"

echo "==> Done. $DB_BASE_PATH is now an H2 2.2.220 database."
echo "    Dump kept at:        $DUMP_SQL"
echo "    1.4.200 backup kept: $BACKUP_MV_DB"
echo "    Remove both once you've confirmed the migrated deployment is good."
