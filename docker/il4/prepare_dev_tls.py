"""Explicitly provision seven-day, deployment-unique development HTTPS material."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import uuid


LABEL = "org.semoss.development-tls"
FORMATS = {"BCFKS": "bcfks", "PKCS12": "p12"}


def hostname(value):
    if (len(value) > 253 or not all(re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
                                    for label in value.split("."))):
        raise ValueError("Expected a DNS hostname without wildcards, ports, or SAN syntax")
    return value


def volume_name(value):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", value):
        raise ValueError("Expected a named Docker volume, not a path")
    return value


def image_reference(value):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:/@-]*", value):
        raise ValueError("Expected a Docker image reference")
    return value


def seed(root, store_type, dns_name):
    if store_type not in FORMATS:
        raise ValueError("Unsupported keystore format")
    hostname(dns_name)
    if any(root.iterdir()):
        raise RuntimeError("TLS destination must be empty; existing material is never overwritten")
    root.chmod(0o755)
    password = root / "server.password"
    keystore = root / ("server." + FORMATS[store_type])
    certificate = root / "readiness-ca.pem"
    password.write_text(secrets.token_hex(48) + "\n")
    password.chmod(0o600)
    provider = ([] if store_type == "PKCS12" else [
        "-providerclass", "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider",
        "-providerpath", "/opt/fips/bc-fips-2.1.3.jar",
        "-J-Dorg.bouncycastle.native.cpu_variant=java",
    ])
    common = ["-keystore", str(keystore), "-storetype", store_type,
              "-storepass:file", str(password), *provider]
    names = "dns:localhost" + ("" if dns_name == "localhost" else ",dns:" + dns_name)
    commands = [
        ["keytool", "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "3072",
         "-sigalg", "SHA384withRSA", "-validity", "7", "-dname", "CN=" + dns_name,
         "-ext", "SAN=" + names, "-ext", "BC=ca:false",
         "-ext", "KU=digitalSignature,keyEncipherment", "-ext", "EKU=serverAuth",
         "-keypass:file", str(password), *common],
        ["keytool", "-exportcert", "-rfc", "-alias", "server", "-file", str(certificate), *common],
    ]
    try:
        for command in commands:
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.PIPE, timeout=120)
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        raise RuntimeError("Keytool development TLS preparation failed") from None
    keystore.chmod(0o600)
    certificate.chmod(0o644)
    for path in (password, keystore):
        os.chown(path, 10001, 10001)
    return {"format": store_type, "hostname": dns_name, "validity_days": 7,
            "certificate_pem": certificate.read_text()}


def docker(*args, input=None, timeout=120):
    try:
        result = subprocess.run(["docker", *args], input=input, capture_output=True,
                                text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError("Docker unavailable or timed out") from None
    if result.returncode:
        raise RuntimeError("Docker " + args[0] + " failed")
    return result.stdout


def owner(kind, name):
    labels = ".Config.Labels" if kind == "container" else ".Labels"
    return docker(kind, "inspect", "--format", '{{index ' + labels + ' "' + LABEL + '"}}', name).strip()


def remove_owned(kind, name, run_id):
    if owner(kind, name) != run_id:
        raise RuntimeError("TLS resource ownership mismatch; refusing removal")
    docker(kind, "rm", *(["--force"] if kind == "container" else []), name)


def prepare(image, volume, store_type, dns_name):
    image_reference(image)
    volume_name(volume)
    hostname(dns_name)
    if store_type not in FORMATS:
        raise ValueError("Unsupported keystore format")
    docker("image", "inspect", "--", image)
    if volume in docker("volume", "ls", "--format", "{{.Name}}").splitlines():
        raise RuntimeError("TLS volume already exists; reuse it unchanged or choose a new volume for rotation")
    run_id = uuid.uuid4().hex
    container = "semoss-dev-tls-" + run_id
    owned_volume = created_container = success = False
    try:
        docker("volume", "create", "--label", LABEL + "=" + run_id, volume)
        if owner("volume", volume) != run_id:
            raise RuntimeError("TLS volume ownership mismatch; refusing modification")
        owned_volume = True
        docker("create", "--pull=never", "--platform", "linux/amd64", "--network", "none",
               "--read-only", "--cap-drop", "ALL", "--cap-add", "CHOWN",
               "--security-opt", "no-new-privileges", "--user", "0",
               "--pids-limit", "128", "--memory", "512m", "--cpus", "1",
               "--tmpfs", "/tmp:rw,noexec,nosuid,nodev", "--interactive",
               "--name", container, "--label", LABEL + "=" + run_id,
               "--mount", "type=volume,src=" + volume + ",dst=/tls",
               "--entrypoint", "/opt/semoss-python/bin/python", image,
               "-I", "-", "--worker", store_type, dns_name)
        created_container = True
        output = docker("start", "--attach", "--interactive", container,
                        input=Path(__file__).read_text(), timeout=300)
        if docker("container", "inspect", "--format", "{{.State.ExitCode}}", container).strip() != "0":
            raise RuntimeError("TLS worker failed; no development certificate was accepted")
        report = json.loads(output)
        if (not isinstance(report, dict) or report.get("format") != store_type
                or report.get("hostname") != dns_name or report.get("validity_days") != 7
                or not isinstance(report.get("certificate_pem"), str)):
            raise RuntimeError("Invalid development TLS worker report")
        success = True
        return {**report, "volume": volume, "development_only": True,
                "keystore": "/run/secrets/server." + FORMATS[store_type],
                "password_file": "/run/secrets/server.password"}
    finally:
        failures = []
        for kind, name, needed in (("container", container, created_container),
                                    ("volume", volume, owned_volume and not success)):
            if needed:
                try:
                    remove_owned(kind, name, run_id)
                except RuntimeError as error:
                    failures.append(str(error))
        if failures:
            raise RuntimeError("Development TLS cleanup incomplete: " + "; ".join(failures))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, type=image_reference)
    parser.add_argument("--volume", required=True, type=volume_name)
    parser.add_argument("--format", required=True, choices=FORMATS)
    parser.add_argument("--hostname", type=hostname, default="localhost")
    parser.add_argument("--allow-self-signed", action="store_true")
    args = parser.parse_args(argv)
    if not args.allow_self_signed:
        parser.error("Development-only generation requires --allow-self-signed")
    print("DEVELOPMENT ONLY: seven-day self-signed HTTPS certificate; not FIPS or IL4 evidence.", file=sys.stderr)
    try:
        report = prepare(args.image, args.volume, args.format, args.hostname)
    except (RuntimeError, ValueError, OSError) as error:
        print("Development TLS preparation failed: " + str(error), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    if sys.argv[1:2] == ["--worker"]:
        os.umask(0o077)
        print(json.dumps(seed(Path("/tls"), sys.argv[2], sys.argv[3])))
    else:
        sys.exit(main())
