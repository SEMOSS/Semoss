"""Synthetic administrator and live JDBC acceptance, inside an isolated fixture."""
import json
from pathlib import Path
import re
import secrets
import sys

from functional_smoke import Client, pixel_outputs


def checked_pixel(client, expression, stage):
    try:
        return client.pixel(expression)
    except RuntimeError:
        raise RuntimeError(stage + " failed") from None


def successful(result):
    return isinstance(result, dict) and result.get("success") in (True, "true")


def bootstrap_admin(client, credentials, disable_registration):
    client.fetch_csrf()
    assigned = client.request("/adminconfig/setInitialAdmins", {
        "ids": json.dumps([credentials["username"]]),
    })
    if not successful(assigned):
        raise RuntimeError("Initial administrator assignment failed")
    if not successful(client.request("/api/auth/createUser", credentials)):
        raise RuntimeError("Administrator creation failed")
    client.login(credentials)
    if pixel_outputs(checked_pixel(client, "1 + 1;", "Authenticated calculation")) != [2]:
        raise RuntimeError("Authenticated calculation returned an unexpected result")
    disable_registration()
    result = checked_pixel(client, "AdminReloadSocialProperties();", "Administrator privilege check")
    if pixel_outputs(result) != [True]:
        raise RuntimeError("Administrator-only property reload did not succeed")


def check_wrong_password(client, credentials):
    client.fetch_csrf()
    try:
        result = client.request("/api/auth/login", {
            "username": credentials["username"], "password": "deliberately-wrong-password",
            "disableRedirect": "true",
        })
    except RuntimeError as error:
        if re.match(r"/api/auth/login: HTTP (401|403):", str(error)):
            return
        raise RuntimeError("Wrong-password check failed without an authentication rejection") from None
    if not isinstance(result, dict) or result.get("success") not in (False, "false"):
        raise RuntimeError("Wrong password was not explicitly rejected")


def registration_expression(database, password):
    if database == "postgres":
        driver, schema = "POSTGRES", "public"
        url = ("jdbc:postgresql://postgres:5432/semoss_validation"
               "?sslmode=verify-full&sslrootcert=/run/secrets/localhost.pem"
               "&connectTimeout=10&socketTimeout=30")
    elif database == "mariadb":
        driver, schema = "MARIA_DB", "semoss_validation"
        url = ("jdbc:mariadb://mariadb:3306/semoss_validation"
               "?sslMode=verify-full&serverSslCert=/run/secrets/localhost.pem"
               "&connectTimeout=10000&socketTimeout=30000&allowLocalInfile=false")
    else:
        raise ValueError("Unsupported acceptance database")
    details = {"RDBMS_TYPE": driver, "CONNECTION_URL": url,
               "USERNAME": "semoss_reader", "PASSWORD": password,
               "hostname": database, "database": "semoss_validation", "schema": schema}
    metamodel = {"tables": {"measurements.id": ["id", "amount"]}, "relationships": []}
    return ('RdbmsExternalUpload(database=["Comparison_' + database + '"], conDetails=['
            + json.dumps(details) + "], metamodel=[" + json.dumps(metamodel) + "]);")


def rows(document):
    outputs = pixel_outputs(document)
    last = outputs[-1]
    data = last.get("data") if isinstance(last, dict) else None
    values = data.get("values") if isinstance(data, dict) else None
    if not isinstance(values, list) or not all(isinstance(row, list) for row in values):
        raise RuntimeError("Malformed database query result")
    return values


def check_database(client, database, password):
    result = checked_pixel(client, registration_expression(database, password), "Registration")
    engine = pixel_outputs(result)[-1]
    if (not isinstance(engine, dict) or not isinstance(engine.get("database_id"), str)
            or not engine["database_id"] or engine.get("engine_global") is not False):
        raise RuntimeError("Registration did not return a private database engine")

    def query(sql):
        expression = ("Database(" + json.dumps(engine["database_id"]) + ") | Query("
                      + json.dumps(sql) + ") | Collect(10);")
        return rows(checked_pixel(client, expression, database + " query"))

    aggregate = query("SELECT COUNT(*) AS row_count, SUM(amount) AS total_amount FROM measurements")
    if aggregate != [[3, 12]]:
        raise RuntimeError(database + " returned incorrect fixture data")
    if database == "postgres":
        if query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()") != [[True]]:
            raise RuntimeError("PostgreSQL session is not encrypted")
        privileges = query(
            "SELECT has_table_privilege(current_user, 'public.measurements', 'INSERT'), "
            "has_table_privilege(current_user, 'public.measurements', 'UPDATE'), "
            "has_table_privilege(current_user, 'public.measurements', 'DELETE'), "
            "has_schema_privilege(current_user, 'public', 'CREATE')")
        if privileges != [[False, False, False, False]]:
            raise RuntimeError("PostgreSQL fixture account has write privileges")
    else:
        tls = query("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        if (len(tls) != 1 or len(tls[0]) != 2 or tls[0][0] != "Ssl_cipher"
                or not isinstance(tls[0][1], str) or not tls[0][1]):
            raise RuntimeError("MariaDB session is not encrypted")
        grants = query("SHOW GRANTS")
        if (not grants or any(len(row) != 1 or not isinstance(row[0], str)
                or not re.match(
                    r"GRANT (USAGE ON \*\.\*|SELECT ON `semoss_validation`\.`measurements`) TO ",
                    row[0]) for row in grants)
                or not any(row[0].startswith("GRANT SELECT ON ") for row in grants)):
            raise RuntimeError("MariaDB fixture account does not have SELECT-only grants")
    return {"registration": "pass", "aggregate": aggregate, "tls": "pass", "readonly_account": "pass"}


def disable_registration():
    with Path("/opt/semosshome/social.properties").open("a") as stream:
        stream.write("\nnative_registration=false\n")


def main():
    credentials = {
        "username": "comparison-admin", "password": "Aa1!" + secrets.token_urlsafe(32),
        "email": "comparison-admin@example.invalid", "name": "Synthetic Comparison Administrator",
    }
    client = Client("https://localhost:8443/Monolith", "/run/secrets/localhost.pem")
    bootstrap_admin(client, credentials, disable_registration)
    check_wrong_password(Client("https://localhost:8443/Monolith", "/run/secrets/localhost.pem"), credentials)
    password = Path("/run/secrets/db-reader.password").read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{48}", password):
        raise RuntimeError("Invalid synthetic database credential")
    results = {database: check_database(client, database, password)
               for database in ("postgres", "mariadb")}
    print(json.dumps({"admin_login": "pass", "admin_only_operation": "pass",
                      "registration_disabled": "pass", "wrong_password_rejected": "pass",
                      "databases": results}))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError):
        # Server errors can contain credentials or connection details.
        sys.exit("Application acceptance failed; inspect the disposable fixture privately.")
