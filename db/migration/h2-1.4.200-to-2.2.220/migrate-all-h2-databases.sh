#!/usr/bin/env bash
#
# Finds every H2 database file under a SEMOSS base folder (the default
# per-project insights_database.mv.db, each engine's audit_log_database.mv.db
# when H2 is the configured DEFAULT_INSIGHTS_RDBMS, any user-created native H2
# data engine, and Quartz's job-store DB if it's H2-backed all end up here -
# they're all just .mv.db files regardless of role) and migrates each one
# with migrate-h2-database.sh. See README.md in this directory first.
#
# Usage:
#   ./migrate-all-h2-databases.sh <semoss-base-folder> [old-h2-jar] [new-h2-jar]
#
# Skips (with a note, not a failure) any .mv.db whose backup already exists,
# since migrate-h2-database.sh treats that as "already handled or in
# progress" rather than silently re-touching it. Exits non-zero if any
# individual migration failed; successful ones are not rolled back just
# because a later one in the batch failed.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
	echo "Usage: $0 <semoss-base-folder> [old-h2-jar] [new-h2-jar]" >&2
	exit 2
fi

BASE_FOLDER="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -d "$BASE_FOLDER" ]]; then
	echo "No such directory: $BASE_FOLDER" >&2
	exit 1
fi

mapfile -d '' -t MV_DB_FILES < <(find "$BASE_FOLDER" -name '*.mv.db' -print0)

if [[ ${#MV_DB_FILES[@]} -eq 0 ]]; then
	echo "No .mv.db files found under $BASE_FOLDER"
	exit 0
fi

echo "Found ${#MV_DB_FILES[@]} H2 database file(s) under $BASE_FOLDER:"
printf '  %s\n' "${MV_DB_FILES[@]}"
echo

FAILED=()
SKIPPED=()
SUCCEEDED=()

for mv_db in "${MV_DB_FILES[@]}"; do
	db_base_path="${mv_db%.mv.db}"
	echo "=============================================================="
	echo "Migrating: $db_base_path"
	echo "=============================================================="
	if [[ -f "$db_base_path.mv.db.pre-2.2.220" ]]; then
		echo "Skipping - a .pre-2.2.220 backup already exists (already migrated, or a prior attempt needs manual cleanup first)."
		SKIPPED+=("$db_base_path")
		continue
	fi
	if "$SCRIPT_DIR/migrate-h2-database.sh" "$db_base_path" "${2:-}" "${3:-}"; then
		SUCCEEDED+=("$db_base_path")
	else
		FAILED+=("$db_base_path")
	fi
	echo
done

echo "=============================================================="
echo "Summary: ${#SUCCEEDED[@]} succeeded, ${#SKIPPED[@]} skipped, ${#FAILED[@]} failed"
if [[ ${#SUCCEEDED[@]} -gt 0 ]]; then
	printf '  OK:      %s\n' "${SUCCEEDED[@]}"
fi
if [[ ${#SKIPPED[@]} -gt 0 ]]; then
	printf '  SKIPPED: %s\n' "${SKIPPED[@]}"
fi
if [[ ${#FAILED[@]} -gt 0 ]]; then
	printf '  FAILED:  %s\n' "${FAILED[@]}"
	exit 1
fi
