"""Compare local images with isolated homes, administrator login, and TLS databases."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

from database_fixtures import start_databases, verify_read_only_accounts


LABEL = "org.semoss.comparison"
HEALTH = ("python -I /run/secrets/readiness.py --ca /run/secrets/localhost.pem "
          "--url https://localhost:8443/Monolith/health/ready --timeout 5")
SEED = r"""
set -eu
umask 077
python -I -c 'import json, sys; from pathlib import Path
assets = json.load(sys.stdin)
for name in ("readiness.py", "functional_smoke.py", "application_acceptance.py"):
    Path("/fixture", name).write_text(assets[name])'
python -I -c 'from pathlib import Path; import secrets; Path("/fixture/server.password").write_text(secrets.token_urlsafe(48)+"\n")'
keytool -genkeypair -alias localhost -keyalg RSA -keysize 3072 -sigalg SHA384withRSA \
  -validity 2 -dname CN=localhost -ext SAN=dns:localhost,dns:postgres,dns:mariadb -ext BC=ca:true \
  -keystore /fixture/server.bcfks -storetype BCFKS \
  -storepass:file /fixture/server.password -keypass:file /fixture/server.password \
  -providerclass org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
  -providerpath /opt/fips/bc-fips-2.1.3.jar -J-Dorg.bouncycastle.native.cpu_variant=java
keytool -importkeystore -noprompt \
  -srckeystore /fixture/server.bcfks -srcstoretype BCFKS -srcstorepass:file /fixture/server.password \
  -destkeystore /fixture/server.p12 -deststoretype PKCS12 -deststorepass:file /fixture/server.password \
  -providerclass org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
  -providerpath /opt/fips/bc-fips-2.1.3.jar -J-Dorg.bouncycastle.native.cpu_variant=java
keytool -exportcert -rfc -alias localhost -keystore /fixture/server.p12 -storetype PKCS12 \
  -storepass:file /fixture/server.password -file /fixture/localhost.pem
python -I -c 'from pathlib import Path; import secrets
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption, pkcs12
root = Path("/fixture")
key, cert, _ = pkcs12.load_key_and_certificates((root/"server.p12").read_bytes(), (root/"server.password").read_bytes().strip())
(root/"db-server.key").write_bytes(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
(root/"db-server.pem").write_bytes(cert.public_bytes(Encoding.PEM))
for name in ("db-root.password", "db-reader.password"):
    (root/name).write_text(secrets.token_hex(24)+"\n")'
chmod 600 /fixture/server.bcfks /fixture/server.p12 /fixture/server.password
chmod 644 /fixture/*.py /fixture/localhost.pem /fixture/db-server.pem
chown 10001:10001 /fixture/server.bcfks /fixture/server.p12 /fixture/server.password
chmod 600 /fixture/db-server.key
chown 999:999 /fixture/db-server.key
chmod 640 /fixture/db-reader.password
chown 10001:999 /fixture/db-reader.password
chmod 400 /fixture/db-root.password
"""
BOOTSTRAP_START = r"""
set -eu
test -f /opt/semosshome/social.properties
printf '\nnative_registration=true\n' >> /opt/semosshome/social.properties
exec /usr/local/bin/semoss-entrypoint run
"""
APPLICATION_PROBE = (
    "import runpy, sys; sys.path.insert(0, '/run/secrets'); "
    "runpy.run_path('/run/secrets/application_acceptance.py', run_name='__main__')"
)
RUNTIME_PROBE = r"""
import errno, http.client, json, os, ssl, subprocess, sys
from pathlib import Path
assert os.getuid() == 10001 and os.getgid() == 10001
status = dict(line.split(":", 1) for line in Path("/proc/1/status").read_text().splitlines())
assert status["NoNewPrivs"].strip() == "1"
assert all(int(status[k].strip(), 16) == 0 for k in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"))
probe = Path("/opt/semoss-comparison-write")
try:
    probe.write_text("synthetic")
except OSError as error:
    assert error.errno == errno.EROFS
else:
    probe.unlink()
    raise AssertionError("Writable root")
probe = Path("/tmp/semoss-comparison-exec")
try:
    probe.write_text("#!/bin/sh\nexit 0\n"); probe.chmod(0o700)
    denied = subprocess.run(["/bin/sh", "-c", "/tmp/semoss-comparison-exec"], capture_output=True, text=True, timeout=5)
    assert denied.returncode == 126 and "Permission denied" in denied.stderr
finally:
    probe.unlink(missing_ok=True)
ca = "/run/secrets/localhost.pem"
for host, trust, expected in (("localhost", ca, 0), ("127.0.0.1", ca, 1),
                              ("localhost", "/etc/pki/tls/certs/ca-bundle.crt", 1)):
    checked = subprocess.run([sys.executable, "-I", "/run/secrets/readiness.py", "--ca", trust,
                              "--url", "https://"+host+":8443/Monolith/health/ready"],
                             capture_output=True, text=True, timeout=10)
    assert checked.returncode == expected, "Readiness/trust rejection mismatch"
connection = http.client.HTTPSConnection("localhost", 8443, context=ssl.create_default_context(cafile=ca), timeout=10)
try:
    connection.request("GET", "/SemossWeb/")
    response = connection.getresponse()
    assert response.status == 200 and b"<html" in response.read(1024*1024).lower()
finally:
    connection.close()
print(json.dumps({"uid_gid": "pass", "no_new_privileges": "pass", "capabilities": "pass",
                  "root_readonly": "pass", "tmp_noexec": "pass", "readiness": "pass",
                  "wrong_hostname_rejected": "pass", "wrong_ca_rejected": "pass", "ui_https": "pass"}))
"""


def image_reference(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]*", value):
        raise argparse.ArgumentTypeError("Expected a Docker image reference, not options or shell syntax")
    return value


def docker(*args, input=None, timeout=120):
    try:
        result = subprocess.run(["docker", *args], input=input, text=True,
                                capture_output=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError("Docker command unavailable or timed out: " + args[0]) from error
    if result.returncode:
        raise RuntimeError("Docker command failed: " + args[0] + ", exit " + str(result.returncode))
    return result.stdout


def dump_diagnostics(containers):
    """Best-effort container state/log dump for the CI log, so a readiness
    failure is diagnosable from the job output alone: the containers are
    destroyed by cleanup() moments after wait_healthy raises, and this
    comparison never otherwise surfaces per-container detail."""
    for name in containers:
        try:
            state = docker("inspect", "--format", "{{json .State}}", name)
            print("-- {} state --\n{}".format(name, state.strip()), file=sys.stderr)
        except RuntimeError as error:
            print("-- {} state unavailable: {} --".format(name, error), file=sys.stderr)
        try:
            tail = docker("logs", "--tail", "200", name)
        except RuntimeError as error:
            tail = "(unavailable: {})".format(error)
        print("-- {} logs (last 200 lines) --\n{}".format(name, tail), file=sys.stderr)


def wait_healthy(containers):
    pending = set(containers)
    deadline = time.monotonic() + 300
    while pending:
        for name in sorted(pending):
            state = json.loads(docker("inspect", "--format", "{{json .State}}", name))
            if not isinstance(state, dict) or state.get("Running") is not True:
                dump_diagnostics(containers)
                raise RuntimeError("Comparison container exited before readiness")
            details = state.get("Health")
            health = details.get("Status") if isinstance(details, dict) else None
            if health not in ("starting", "healthy", "unhealthy"):
                dump_diagnostics(containers)
                raise RuntimeError("Comparison container has invalid health metadata")
            if health == "healthy":
                pending.remove(name)
        if pending:
            if time.monotonic() >= deadline:
                dump_diagnostics(containers)
                raise RuntimeError("Comparison readiness deadline exceeded")
            time.sleep(3)


def cleanup(containers, volumes, run_id, networks=()):
    errors = []
    for name in reversed(containers):
        try:
            owner = docker("inspect", "--format", '{{index .Config.Labels "' + LABEL + '"}}', name).strip()
            if owner != run_id:
                raise RuntimeError("Container ownership mismatch")
            docker("stop", "--time", "60", name)
            docker("rm", "--force", name)
        except RuntimeError as error:
            errors.append(str(error))
    for name in reversed(networks):
        try:
            owner = docker("network", "inspect", "--format", '{{index .Labels "' + LABEL + '"}}', name).strip()
            if owner != run_id:
                raise RuntimeError("Network ownership mismatch")
            docker("network", "rm", name)
        except RuntimeError as error:
            errors.append(str(error))
    for name in reversed(volumes):
        try:
            owner = docker("volume", "inspect", "--format", '{{index .Labels "' + LABEL + '"}}', name).strip()
            if owner != run_id:
                raise RuntimeError("Volume ownership mismatch")
            docker("volume", "rm", name)
        except RuntimeError as error:
            errors.append(str(error))
    if errors:
        raise RuntimeError("Comparison cleanup incomplete: " + "; ".join(errors))


def compare(baseline, candidate):
    image_ids = [docker("image", "inspect", "--format", "{{.Id}}", image).strip()
                 for image in (baseline, candidate)]
    if not all(image_ids) or image_ids[0] == image_ids[1]:
        raise RuntimeError("Comparison requires two distinct images already available locally")
    run_id = uuid.uuid4().hex
    prefix = "semoss-compare-" + run_id
    containers, volumes, networks, apps, results = [], [], [], [], []
    restrictions = ["--pull=never", "--platform", "linux/amd64",
                    "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                    "--tmpfs", "/tmp:rw,noexec,nosuid,nodev"]
    try:
        for suffix in ("tls", "baseline-home", "candidate-home"):
            name = prefix + "-" + suffix
            docker("volume", "create", "--label", LABEL + "=" + run_id, name)
            volumes.append(name)
        seed = prefix + "-seed"
        docker("create", *restrictions, "--network", "none", "--name", seed, "--label", LABEL + "=" + run_id,
               "--user", "0", "--cap-add", "CHOWN", "--interactive",
               "--mount", "type=volume,src=" + volumes[0] + ",dst=/fixture",
               "--entrypoint", "/bin/sh", baseline, "-ec", SEED)
        containers.append(seed)
        root = Path(__file__).parent
        assets = {name: (root / directory / name).read_text() for directory, name in (
            ("", "readiness.py"), ("tests", "functional_smoke.py"), ("tests", "application_acceptance.py"))}
        docker("start", "--attach", "--interactive", seed, input=json.dumps(assets), timeout=180)
        network = prefix + "-network"
        docker("network", "create", "--internal", "--label", LABEL + "=" + run_id, network)
        networks.append(network)
        databases = start_databases(docker, prefix, run_id, network, volumes[0], containers, volumes)
        verify_read_only_accounts(docker, databases)
        for index, (role, image) in enumerate((("baseline", baseline), ("candidate", candidate))):
            name = prefix + "-" + role
            options = ["--user", "10001:10001", "--pids-limit", "512", "--memory", "4g",
                       "--cpus", "2", "--env", "CATALINA_OPTS=-Xmx1536m",
                       "--log-driver", "local", "--log-opt", "max-size=10m", "--log-opt", "max-file=3",
                       "--health-cmd", HEALTH, "--health-interval", "5s", "--health-timeout", "8s",
                       "--health-start-period", "180s", "--health-retries", "3",
                       "--mount", "type=volume,src=" + volumes[0] + ",dst=/run/secrets,readonly",
                       "--mount", "type=volume,src=" + volumes[index + 1] + ",dst=/opt/semosshome"]
            for directory in ("temp", "work", "logs"):
                options += ["--tmpfs", "/opt/tomcat/" + directory + ":rw,noexec,nosuid,nodev,uid=10001,gid=10001"]
            if role == "candidate":
                options += ["--env", "SEMOSS_ALLOW_NONVALIDATED_ACCP=true"]
            docker("create", *restrictions, "--network", network, *options, "--label", LABEL + "=" + run_id,
                   "--name", name, "--entrypoint", "/bin/sh", image, "-ec", BOOTSTRAP_START)
            containers.append(name)
            apps.append(name)
            docker("start", name)
        wait_healthy(apps)
        for name, role, image_id in zip(apps, ("baseline", "candidate"), image_ids):
            checked = docker("exec", name, "/usr/local/bin/semoss-entrypoint", "--check")
            if ("BCFIPS" if role == "baseline" else "ACCP") not in checked:
                raise RuntimeError("Unexpected provider check output for " + role)
            result = json.loads(docker("exec", name, "python", "-I", "-c", RUNTIME_PROBE))
            if not isinstance(result, dict) or not result or any(value != "pass" for value in result.values()):
                raise RuntimeError("Runtime comparison checks failed")
            application = json.loads(docker("exec", name, "python", "-I", "-c", APPLICATION_PROBE, timeout=300))
            required = ("admin_login", "admin_only_operation", "registration_disabled", "wrong_password_rejected")
            if not isinstance(application, dict) or any(application.get(key) != "pass" for key in required):
                raise RuntimeError("Administrator acceptance checks failed")
            connections = application.get("databases")
            if not isinstance(connections, dict) or set(connections) != {"postgres", "mariadb"}:
                raise RuntimeError("Missing database acceptance results")
            for checks in connections.values():
                if (not isinstance(checks, dict) or checks.get("aggregate") != [[3, 12]]
                        or any(checks.get(key) != "pass" for key in ("registration", "tls", "readonly_account"))):
                    raise RuntimeError("Database acceptance checks failed")
            results.append({"role": role, "image_id": image_id, "checks": result, "application": application})
        return {"status": "passed", "baseline": baseline, "candidate": candidate, "results": results,
                "database_fixtures": databases,
                "scope": "Synthetic compatibility checks only; candidate remains NONVALIDATED. "
                         "Local native administrator and real TLS PostgreSQL/MariaDB connectors tested; "
                         "no production credentials, SSO, or SQL Server validation."}
    finally:
        cleanup(containers, volumes, run_id, networks)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=image_reference)
    parser.add_argument("--candidate", required=True, type=image_reference)
    args = parser.parse_args(argv)
    try:
        report = compare(args.baseline, args.candidate)
    except (RuntimeError, ValueError, OSError) as error:
        print("Comparison failed: " + str(error), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
