# H2 1.4.200 -> 2.4.240 data migration

SEMOSS's H2 dependency moved from 1.4.200 to 2.2.220 and is now at 2.4.240. The
first move closed 4 Dependabot alerts (CVE-2021-42392, CVE-2022-23221, CVE-2021-23463, CVE-2022-45868), all
specific to H2's web admin console and `JdbcSQLXML.getSource(DOMSource.class)`
- neither of which SEMOSS uses - so this is a hygiene/compliance fix, not a
response to an active exploit path.

**Which files need migrating.** Only 1.4.200-format files (MVStore format 1).
H2 2.4.240 opens a 2.2.220-format file (format 3) as is - checked with a
database that has identity columns, CHECK/FK constraints and CLOB/BLOB data -
so a deployment that already ran the 2.2.220 version of this tooling needs no
further migration. The scripts read the format from the file header and skip
2.x files (exit code 3, reported as `SKIPPED` by the bulk script).

The version bump alone is not enough to deploy, though: **H2 2.x cannot open a
1.4.200-format `.mv.db` file**. Any already-deployed SEMOSS instance has to
migrate its existing H2 files before (or as part of) upgrading, or every
engine backed by one will fail to open with:

```
Unsupported database file version or invalid file header in file "..."
Caused by: org.h2.mvstore.MVStoreException: The write format 1 is smaller than the supported format 3
```

This directory has the tooling for that migration. It implements H2's own
documented upgrade path: dump the old database to SQL with the *old* H2 jar's
`Script` tool, then replay that SQL into a brand-new file with the *new* H2
jar's `RunScript` tool.

## What needs migrating

Every H2 file SEMOSS creates is a plain `.mv.db` file somewhere under the
deployment's base folder, regardless of what it's used for - so rather than
tracking down each category by name, `migrate-all-h2-databases.sh` just finds
all of them:

- Each project's default insights store (`insights_database.mv.db`, built in
  `SmssUtilities`)
- Each engine's audit log (`audit_log_database.mv.db`, built in
  `AuditDatabase`) - only present if H2 is the configured
  `DEFAULT_INSIGHTS_RDBMS`
- Any user-created native H2 data engine (e.g. `db/<EngineName>/database.mv.db`)
- Quartz's job-store database, if it's configured to use H2 - its connection
  details are set at runtime by `SchedulerFactorySingleton`, not statically in
  `quartz.properties`, so check your deployment's actual scheduler datasource
  config if you're not sure whether this applies

H2Frame's own per-insight cache files (created fresh per session under the
insight cache directory, deleted in `H2Frame.close()`) are ephemeral and do
**not** need migrating - just let them get recreated under the new version.

## Prerequisites

- Both H2 jars available: `h2-1.4.200.jar` and `h2-2.4.240.jar`. Both scripts
  default to the standard Maven local-repo location
  (`~/.m2/repository/com/h2database/h2/<version>/h2-<version>.jar`) and will
  fetch whichever is missing via `mvn dependency:get` if `mvn` is on PATH;
  otherwise pass explicit paths.
- A `java` on PATH recent enough to run a single-file source program
  (`java SomeFile.java`, JDK 11+) - used for the post-migration row-count
  check, not for the migration itself.
- Enough free disk space to hold the original file, its SQL dump, and the new
  file simultaneously (the dump is usually larger than the compacted `.mv.db`
  it came from).

## Usage

Dry-run the discovery first to see what would be touched:

```bash
find /path/to/semoss/base/folder -name '*.mv.db'
```

Migrate everything under a deployment's base folder:

```bash
./migrate-all-h2-databases.sh /path/to/semoss/base/folder
```

Or migrate a single database (path **without** the `.mv.db` extension):

```bash
./migrate-h2-database.sh /path/to/semoss/base/folder/db/MyEngine/database
```

## What it does, and how to roll back

For each database, `migrate-h2-database.sh`:

1. Refuses to run if the backup path from a previous attempt already exists
   (`<path>.mv.db.pre-2.4.240`) - this is also how the bulk script decides
   whether something was already migrated. A file whose header says it is
   already an H2 2.x file is skipped with exit code 3.
2. Dumps the database to `<path>.migration.sql` with the old jar.
3. Moves the original `.mv.db` (and `.trace.db`, if any) to
   `<path>.mv.db.pre-2.4.240` / `<path>.trace.db.pre-2.4.240`.
4. Replays the dump into a fresh file at the original path with the new jar.
   If this step fails, the script automatically restores the step-3 backup so
   the original file is left exactly as it was.
5. Re-runs the per-table `-- N +/- SELECT COUNT(*) FROM SCHEMA.TABLE;` row
   count assertions that H2's own `Script` tool embeds in the dump as
   comments, against the new database, via `VerifyRowCounts.java` - this
   catches a silent partial replay that RunScript itself didn't treat as a
   hard failure.

Nothing deletes the `.migration.sql` dump or the `.pre-2.4.240` backup on
success - remove them yourself once you've confirmed the upgraded deployment
is good. To roll back after the fact, stop SEMOSS, delete the 2.4.240
`.mv.db`, rename `.mv.db.pre-2.4.240` back to `.mv.db`, and reinstall the
1.4.200 dependency.

## Validation

This procedure was verified end-to-end against a freshly-created H2 1.4.200
database covering the shapes SEMOSS actually uses - VARCHAR/INT/TIMESTAMP/
CLOB/BLOB columns, a bare `IDENTITY` column (as `AuditDatabase` uses), an
index, a CHECK constraint, and a foreign key - confirming after migration
that: every row count matched, CLOB content round-tripped byte-for-byte, the
identity sequence correctly continued from its prior value for a brand-new
insert, and the CHECK/FK constraints and index were all still enforced/
present under 2.4.240 (first verified under 2.2.220, then re-verified under
2.4.240 on 2026-11-04). The 2.4.240 re-validation also ran the bulk
script on a mixed tree (two 1.4.200 files, one 2.2.220 file): the 1.4.200 files
migrated, the 2.2.220 file was skipped, and a second run skipped all three.

One thing to know when writing your own test database: a 1.4.200 `CHECK`
declared inline without a name and a primary key can both be auto-named
`CONSTRAINT_A`, and the dump then fails on replay with `Constraint
"CONSTRAINT_A" already exists`. That is a property of such a source file, not of
the version pair - it fails identically on 2.2.220. Name the constraint in the
source schema if you hit it.

## If the *old* jar can't open your file either

If `migrate-h2-database.sh` fails during the `Script` step with an error from
the *old* (1.4.200) jar - not the "write format" error above, but something
like `Unsupported type N` or another MVStore-internal error - that file has a
pre-existing problem that predates this migration and isn't something this
tooling can fix: it means the currently-running 1.4.200 driver already can't
fully open it today. Treat that as a separate incident on the affected engine,
not a blocker for migrating every other (healthy) database.
