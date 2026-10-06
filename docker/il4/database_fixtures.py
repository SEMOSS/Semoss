"""Disposable, TLS-required databases for synthetic SEMOSS integration checks.

Only the caller owns cleanup. No server FIPS claim is made. Passwords stay in the
provided secret volume and inside database containers; they are never returned.
"""
import re
import time


LABEL = "org.semoss.comparison"
# Mirrored from the upstream images of the same digest by
# .github/workflows/il4-accp-mirror-db-fixtures.yml, so the comparison step
# only ever pulls from GHCR (already authenticated) and never docker.io.
POSTGRES_IMAGE = "ghcr.io/semoss/semoss-il4-db-fixtures/postgres@sha256:8478dfec5cc2631908a88f5a188920b83b1fcf62fc6de75ca3227b7c0c8dd8c2"
MARIA_IMAGE = "ghcr.io/semoss/semoss-il4-db-fixtures/mariadb@sha256:d4553c800fa8bb09b4a9bb72707a215edfc7a3b7c33aceeeef86b3c24d281122"

POSTGRES_START = """set -eu
cat > /etc/semoss-pg_hba.conf <<'HBA'
local all postgres peer
local all all reject
hostssl semoss_validation semoss_reader all scram-sha-256
host all all all reject
host replication all all reject
HBA
exec docker-entrypoint.sh postgres \
-c hba_file=/etc/semoss-pg_hba.conf \
-c password_encryption=scram-sha-256 \
-c ssl=on \
-c ssl_cert_file=/run/secrets/db-server.pem \
-c ssl_key_file=/run/secrets/db-server.key \
-c ssl_ca_file=/run/secrets/localhost.pem \
-c ssl_min_protocol_version=TLSv1.2
"""

POSTGRES_INIT = """DO $setup$
DECLARE secret text := rtrim(pg_read_file('/run/secrets/db-reader.password'), E'\\r\\n');
BEGIN
  IF secret !~ '^([0-9a-f]{2}){16,64}$' THEN
    RAISE EXCEPTION 'Invalid synthetic reader password format';
  END IF;
  EXECUTE format('CREATE ROLE semoss_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', secret);
END;
$setup$;
REVOKE ALL ON DATABASE semoss_validation FROM PUBLIC;
GRANT CONNECT ON DATABASE semoss_validation TO semoss_reader;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO semoss_reader;
CREATE TABLE public.measurements (id integer PRIMARY KEY, amount integer NOT NULL);
INSERT INTO public.measurements (id,amount) VALUES (1,2),(2,4),(3,6);
GRANT SELECT ON TABLE public.measurements TO semoss_reader;
ALTER ROLE semoss_reader SET default_transaction_read_only=on;
"""

# Both hex checks accept 16–64 random bytes, encoded as 32–128 lowercase hex characters.
HEX_CHECK = """case "$password" in ''|*[!0-9a-f]*) exit 1;; esac
[ "${#password}" -ge 32 ]
[ "${#password}" -le 128 ]
[ "$(( ${#password} % 2 ))" -eq 0 ]
"""
MARIA_CONFIG = """set -eu
umask 077
password=$(cat /run/secrets/db-root.password)
""" + HEX_CHECK + """printf '[client]\\nuser=root\\npassword=%s\\nprotocol=socket\\n' "$password" > /run/semoss-fixture-client.cnf
unset password
"""
MARIA_CLIENT = ("mariadb --defaults-extra-file=/run/semoss-fixture-client.cnf "
                "--batch --skip-column-names --database=semoss_validation")
MARIA_INIT = """set -eu
password=$(cat /run/secrets/db-reader.password)
""" + HEX_CHECK + MARIA_CLIENT + """ <<SQL
CREATE TABLE measurements (id integer PRIMARY KEY, amount integer NOT NULL);
INSERT INTO measurements (id,amount) VALUES (1,2),(2,4),(3,6);
CREATE USER 'semoss_reader'@'%' IDENTIFIED BY '$password' REQUIRE SSL;
GRANT SELECT ON semoss_validation.measurements TO 'semoss_reader'@'%';
SQL
unset password
"""

POSTGRES_READY = """SELECT current_setting('server_version'), current_setting('ssl'),
current_setting('listen_addresses') <> '';
"""
MARIA_READY = """SELECT VERSION(), @@require_secure_transport, @@skip_networking, @@ssl_cert != '';
"""
POSTGRES_PRIVILEGES = """SELECT
has_table_privilege('semoss_reader','public.measurements','SELECT')
AND NOT has_table_privilege('semoss_reader','public.measurements','INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
AND NOT has_database_privilege('semoss_reader','semoss_validation','CREATE,TEMP')
AND NOT has_schema_privilege('semoss_reader','public','CREATE')
AND NOT (SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls
         FROM pg_roles WHERE rolname='semoss_reader')
AND NOT EXISTS (SELECT 1 FROM pg_auth_members
                WHERE member=(SELECT oid FROM pg_roles WHERE rolname='semoss_reader'))
AND (SELECT rolcanlogin AND rolpassword LIKE 'SCRAM-SHA-256$%'
     FROM pg_authid WHERE rolname='semoss_reader');
"""
MARIA_PRIVILEGES = """SELECT
(SELECT COUNT(*) FROM information_schema.TABLE_PRIVILEGES
 WHERE GRANTEE="'semoss_reader'@'%'" AND TABLE_SCHEMA='semoss_validation'
 AND TABLE_NAME='measurements' AND PRIVILEGE_TYPE='SELECT' AND IS_GRANTABLE='NO')=1
AND NOT EXISTS (SELECT 1 FROM information_schema.TABLE_PRIVILEGES
 WHERE GRANTEE="'semoss_reader'@'%'"
 AND (TABLE_SCHEMA<>'semoss_validation' OR TABLE_NAME<>'measurements' OR PRIVILEGE_TYPE<>'SELECT' OR IS_GRANTABLE<>'NO'))
AND NOT EXISTS (SELECT 1 FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE="'semoss_reader'@'%'")
AND NOT EXISTS (SELECT 1 FROM information_schema.COLUMN_PRIVILEGES WHERE GRANTEE="'semoss_reader'@'%'")
AND NOT EXISTS (SELECT 1 FROM information_schema.USER_PRIVILEGES
 WHERE GRANTEE="'semoss_reader'@'%'" AND PRIVILEGE_TYPE<>'USAGE')
AND NOT EXISTS (SELECT 1 FROM mysql.roles_mapping WHERE User='semoss_reader')
AND (SELECT ssl_type='ANY' FROM mysql.user WHERE User='semoss_reader' AND Host='%')
AND NOT EXISTS (SELECT 1 FROM mysql.user WHERE User='root' AND Host<>'localhost');
"""


def _admin_sql(docker, engine, container, sql, timeout=10):
    if engine == "postgres":
        return docker("exec", "--user=999:999", "-i", container,
                      "psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1",
                      "--tuples-only", "--no-align", "--username=postgres",
                      "--dbname=semoss_validation", input=sql, timeout=timeout)
    return docker("exec", "--user=0:0", "-i", container, "sh", "-ec",
                  MARIA_CONFIG + "exec " + MARIA_CLIENT, input=sql, timeout=timeout)


def _wait_ready(docker, engine, container, deadline):
    query = POSTGRES_READY if engine == "postgres" else MARIA_READY
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(f"{engine} fixture did not become TLS-ready within 180 seconds")
        try:
            output = _admin_sql(docker, engine, container, query, timeout=min(10, remaining)).strip()
            fields = output.split("|" if engine == "postgres" else "\t")
            expected = ["on", "t"] if engine == "postgres" else ["1", "0", "1"]
            version = fields[0]
            if (fields[1:] == expected
                    and re.fullmatch(r"[0-9][A-Za-z0-9.+~() _:-]{0,127}", version)):
                return version
        except RuntimeError:
            pass
        time.sleep(max(0, min(2, deadline - time.monotonic())))


def verify_read_only_accounts(docker, databases):
    """Verify exact reader privileges using local administrator sockets; return booleans only."""
    verified = {}
    for engine, query, expected in (
            ("postgres", POSTGRES_PRIVILEGES, "t"), ("mariadb", MARIA_PRIVILEGES, "1")):
        try:
            result = _admin_sql(docker, engine, databases[engine]["container"], query).strip()
        except RuntimeError:
            raise RuntimeError(f"{engine} fixture read-only verification failed") from None
        if result != expected:
            raise RuntimeError(f"{engine} fixture read-only privileges are not restricted")
        verified[engine] = True
    return verified


def start_databases(docker, prefix, run_id, network, tls_volume, containers, volumes):
    """Start pinned local images, initialize fixtures, verify privileges, and return safe metadata.

    ``docker(*args, input=None, timeout=120)`` must return stdout or raise RuntimeError.
    ``containers`` and ``volumes`` receive each successfully created resource immediately,
    including resources whose subsequent startup fails. The caller must supply unique names
    and clean up on success and failure. Readiness has one shared 180-second deadline.
    """
    for value in (prefix, run_id, network, tls_volume):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", value):
            raise ValueError("Fixture resource identifiers must be simple nonempty names")
    databases = {}
    for engine, image, data_path, settings, command in (
            ("postgres", POSTGRES_IMAGE, "/var/lib/postgresql/data",
             ("POSTGRES_PASSWORD_FILE=/run/secrets/db-root.password", "POSTGRES_DB=semoss_validation",
              "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=peer"),
             ("sh", "-ec", POSTGRES_START)),
            ("mariadb", MARIA_IMAGE, "/var/lib/mysql",
             ("MARIADB_ROOT_PASSWORD_FILE=/run/secrets/db-root.password",
              "MARIADB_ROOT_HOST=localhost", "MARIADB_DATABASE=semoss_validation"),
             ("mariadbd", "--require-secure-transport=ON",
              "--ssl-cert=/run/secrets/db-server.pem", "--ssl-key=/run/secrets/db-server.key",
              "--ssl-ca=/run/secrets/localhost.pem", "--tls-version=TLSv1.2,TLSv1.3"))):
        container = prefix + "-" + engine
        data_volume = container + "-data"
        try:
            docker("volume", "create", "--label", f"{LABEL}={run_id}", data_volume)
            volumes.append(data_volume)
            arguments = [
                "create", "--pull=never", "--name", container, "--label", f"{LABEL}={run_id}",
                f"--network={network}", f"--network-alias={engine}",
                "--memory=512m", "--cpus=1", "--security-opt=no-new-privileges:true",
                "--log-driver=local", "--log-opt=max-size=10m", "--log-opt=max-file=3",
                "--volume", f"{tls_volume}:/run/secrets:ro",
                "--volume", f"{data_volume}:{data_path}",
            ]
            for setting in settings:
                arguments.extend(("--env", setting))
            docker(*arguments, image, *command)
            containers.append(container)
            docker("start", container)
        except RuntimeError:
            raise RuntimeError(f"{engine} fixture resource startup failed") from None
        databases[engine] = {"container": container, "image": image,
                             "database": "semoss_validation", "role": "semoss_reader",
                             "tls_required": True}
    deadline = time.monotonic() + 180
    for engine, metadata in databases.items():
        metadata["version"] = _wait_ready(docker, engine, metadata["container"], deadline)
        try:
            if engine == "postgres":
                _admin_sql(docker, engine, metadata["container"], POSTGRES_INIT, timeout=30)
            else:
                docker("exec", "--user=0:0", "-i", metadata["container"], "sh", "-s",
                       input=MARIA_INIT, timeout=30)
        except RuntimeError:
            raise RuntimeError(f"{engine} fixture SQL initialization failed") from None
    verified = verify_read_only_accounts(docker, databases)
    for engine in databases:
        databases[engine]["read_only_verified"] = verified[engine]
    return databases
