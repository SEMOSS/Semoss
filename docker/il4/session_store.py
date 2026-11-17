"""Prepare Tomcat's shared session store for several containers (SESSION_STORE=jdbc).

By default Tomcat keeps each login session in the memory of the one container that
created it, so a request that reaches a different container is refused. With
SESSION_STORE=jdbc the Monolith web application keeps its sessions in a table in
PostgreSQL as well, and any container can pick a session up when the load balancer
moves a user.

Everything comes from the environment, so no secret ever appears on a command line
or in the JVM options (catalina.sh re-parses those with eval):

  SESSION_STORE                 memory (default) or jdbc
  SESSION_STORE_URL             jdbc:postgresql://host[:port]/database?sslmode=verify-full&...
  SESSION_STORE_USER            database user
  SESSION_STORE_PASSWORD        database password
  SESSION_STORE_TLS_REQUIRED    true (default); false only for a local test database
  SESSION_STORE_POOL_SIZE       connections per container, 1-100 (default 10)
  SESSION_STORE_SYNC_SECONDS    how often sessions are written to the store, 1-60 (default 2).
                                A user moved to another container within this long of a
                                login or a change can be refused once; a smaller number
                                shortens that window and costs more database writes

`prepare DIRECTORY` validates these and writes DIRECTORY/Monolith.xml, a Tomcat context
descriptor with a private mode, for the Host's xmlBase (see conf/server.xml). It prints
the database host and name, never the user or password.
"""
import os
import re
import sys
from urllib.parse import urlsplit, parse_qsl
from xml.sax.saxutils import quoteattr

TABLE = "tomcat_sessions"

URL_PATTERN = re.compile(
    r"jdbc:postgresql://[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?(:[0-9]{1,5})?/[A-Za-z0-9_.-]{1,63}"
    r"(\?[A-Za-z0-9_.=&%:/,+-]{1,512})?"
)
USER_PATTERN = re.compile(r"[A-Za-z0-9_.@-]{1,128}")


class ConfigurationError(Exception):
    pass


def _integer(env, name, default, low, high):
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    if not re.fullmatch(r"[0-9]{1,5}", raw) or not low <= int(raw) <= high:
        raise ConfigurationError(f"{name} must be a whole number from {low} to {high}")
    return int(raw)


def settings(env):
    """Return the validated settings, or None when sessions stay in memory."""
    mode = env.get("SESSION_STORE", "memory") or "memory"
    if mode == "memory":
        return None
    if mode != "jdbc":
        raise ConfigurationError("SESSION_STORE must be memory or jdbc")
    url = env.get("SESSION_STORE_URL", "")
    user = env.get("SESSION_STORE_USER", "")
    password = env.get("SESSION_STORE_PASSWORD", "")
    if not URL_PATTERN.fullmatch(url):
        raise ConfigurationError(
            "SESSION_STORE_URL must look like jdbc:postgresql://host[:port]/database?sslmode=verify-full")
    if not USER_PATTERN.fullmatch(user):
        raise ConfigurationError("SESSION_STORE_USER is missing or has characters outside A-Za-z0-9_.@-")
    if not password or len(password) > 256 or any(ord(c) < 32 or ord(c) == 127 for c in password):
        raise ConfigurationError("SESSION_STORE_PASSWORD is missing, too long, or has control characters")
    tls = env.get("SESSION_STORE_TLS_REQUIRED", "true")
    if tls not in ("true", "false"):
        raise ConfigurationError("SESSION_STORE_TLS_REQUIRED must be true or false")
    query = dict(parse_qsl(urlsplit(url[len("jdbc:"):]).query, keep_blank_values=True))
    if tls == "true" and query.get("sslmode") != "verify-full":
        raise ConfigurationError(
            "SESSION_STORE_URL must set sslmode=verify-full (certificate chain and host name checked); "
            "see RDS.md. Set SESSION_STORE_TLS_REQUIRED=false only for a local test database.")
    return {
        "url": url,
        "user": user,
        "password": password,
        "pool": _integer(env, "SESSION_STORE_POOL_SIZE", 10, 1, 100),
        "sync": _integer(env, "SESSION_STORE_SYNC_SECONDS", 2, 1, 60),
    }


def describe(config):
    """A line safe to log: the database location only."""
    parts = urlsplit(config["url"][len("jdbc:"):])
    return f"session store: PostgreSQL {parts.hostname}{':' + str(parts.port) if parts.port else ''}{parts.path}"


def render(config):
    attrs = quoteattr
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- Generated at start-up by session_store.py; do not edit. Holds the database password. -->
<Context backgroundProcessorDelay="{config["sync"]}">
  <Resource name="jdbc/sessions" auth="Container" type="javax.sql.DataSource"
            factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
            driverClassName="org.postgresql.Driver"
            url={attrs(config["url"])} username={attrs(config["user"])} password={attrs(config["password"])}
            maxActive="{config["pool"]}" maxIdle="{min(config["pool"], 2)}" minIdle="0" initialSize="0"
            maxWait="10000" testOnBorrow="true" validationQuery="SELECT 1" validationInterval="30000"/>
  <Manager className="org.apache.catalina.session.PersistentManager"
           secureRandomProvider="BCFIPS" secureRandomAlgorithm="DEFAULT"
           saveOnRestart="true" maxIdleBackup="0" maxIdleSwap="-1" minIdleSwap="-1"
           processExpiresFrequency="1">
    <Store className="org.apache.catalina.session.DataSourceStore"
           dataSourceName="jdbc/sessions" localDataSource="true" sessionTable="{TABLE}"/>
  </Manager>
</Context>
"""


def write(directory, config):
    os.makedirs(directory, mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    path = os.path.join(directory, "Monolith.xml")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(render(config))
    os.chmod(path, 0o600)
    return path


def main(argv, env):
    if len(argv) != 3 or argv[1] != "prepare":
        print("Usage: session_store.py prepare DIRECTORY", file=sys.stderr)
        return 2
    try:
        config = settings(env)
    except ConfigurationError as error:
        print(f"Session store configuration error: {error}", file=sys.stderr)
        return 1
    if config is None:
        return 0
    write(argv[2], config)
    print(describe(config))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv, os.environ))
