"""Read-only Docker metadata audit; reporting only unless --strict is supplied."""

import argparse
import ipaddress
import json
import posixpath
import re
import subprocess
import sys


SCHEMA_VERSION = 1
SCOPE = ("Narrow metadata baseline, not proof of effective syscall, permission, TLS, "
         "or authorization controls. No repair, FIPS validation, or egress policy.")
IMAGE_ID = r"sha256:[0-9a-f]{64}"
REPO_COMPONENT = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
IMAGE_REF = (r"(?:[a-z0-9][a-z0-9.-]*(?::[0-9]+)?/)?"
             + REPO_COMPONENT + r"(?:/" + REPO_COMPONENT + r")*@sha256:[0-9a-f]{64}")
HOME = "/opt/semosshome"
TMPFS = ("/tmp", "/opt/tomcat/temp", "/opt/tomcat/work", "/opt/tomcat/logs")
TLS = ("/run/secrets/server.bcfks", "/run/secrets/server.password",
       "/run/secrets/readiness-ca.pem")


class AuditError(Exception):
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise AuditError("invalid arguments; use --help")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def invalid_constant(value):
    raise ValueError


def decode(text):
    return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def require(value, kind):
    if type(value) is not kind:
        raise AuditError("invalid Docker inspect shape")
    return value


def field(obj, key, kind, default=None):
    return require(obj.get(key, default), kind)


def nullable_field(obj, key, kind):
    if key not in obj:
        raise AuditError("invalid Docker inspect shape")
    return kind() if obj[key] is None else require(obj[key], kind)


def strings(value):
    return [require(item, str) for item in require(value, list)]


def inspect(kind, name):
    try:
        result = subprocess.run(["docker", kind, "inspect", name], capture_output=True,
                                text=True, timeout=10, check=False)
    except subprocess.TimeoutExpired:
        raise AuditError("Docker inspect timed out") from None
    except (OSError, UnicodeError):
        raise AuditError("Docker inspect unavailable") from None
    if result.returncode != 0:
        raise AuditError("Docker inspect failed")
    try:
        data = decode(result.stdout)
    except (ValueError, RecursionError):
        raise AuditError("invalid Docker inspect JSON") from None
    if type(data) is not list or len(data) != 1:
        raise AuditError("invalid Docker inspect shape")
    return require(data[0], dict)


def runtime_path(path):
    # This catches conventional sockets and directories exposing them, not arbitrary
    # renamed sockets; inspect metadata cannot establish an effective permissions proof.
    path = posixpath.normpath(path)
    roots = ("/dev", "/var/lib/docker", "/var/lib/containerd",
             "/run/docker", "/run/containerd", "/run/podman", "/run/crio", "/run/user",
             "/var/run/user",
             "/var/run/docker", "/var/run/containerd", "/var/run/podman", "/var/run/crio")
    return (path == "/" or path in ("/run", "/var/run") or
            any(path == root or path.startswith(root + "/") or
                              root.startswith(path + "/") for root in roots)
            or path.rsplit("/", 1)[-1] in
            ("docker.sock", "podman.sock", "containerd.sock", "crio.sock"))


def tmpfs_hardened(options):
    flags = options.split(",")
    return all(good in flags and (bad not in flags or
               len(flags) - 1 - flags[::-1].index(good) >
               len(flags) - 1 - flags[::-1].index(bad))
               for good, bad in (("noexec", "exec"), ("nosuid", "suid"), ("nodev", "dev"))) \
        and "ro" not in flags


def audit(container, image, expected):
    config = field(container, "Config", dict)
    state = field(container, "State", dict)
    host = field(container, "HostConfig", dict)
    actual_id = field(container, "Image", str)
    expected_id = field(image, "Id", str)
    if not re.fullmatch(IMAGE_ID, actual_id) or not re.fullmatch(IMAGE_ID, expected_id):
        raise AuditError("invalid Docker image ID")
    digests = strings(nullable_field(image, "RepoDigests", list))
    user = field(config, "User", str)
    numeric_user = re.fullmatch(r"([0-9]+)(?::([0-9]+))?", user)
    try:
        uid = int(numeric_user[1]) if numeric_user else -1
        gid = int(numeric_user[2]) if numeric_user and numeric_user[2] else -1
    except ValueError:
        raise AuditError("invalid Docker user ID") from None
    cap_drop = strings(nullable_field(host, "CapDrop", list))
    cap_add = strings(nullable_field(host, "CapAdd", list))
    security = strings(nullable_field(host, "SecurityOpt", list))
    resources = {key: field(host, source, int) for key, source in (
        ("memory_bytes", "Memory"), ("nano_cpus", "NanoCpus"),
        ("cpu_quota", "CpuQuota"), ("cpu_period", "CpuPeriod"))}
    resources["pids"] = nullable_field(host, "PidsLimit", int)
    pid_mode = field(host, "PidMode", str)
    ipc_mode = field(host, "IpcMode", str)
    network_mode = field(host, "NetworkMode", str)
    devices = []
    for key in ("Devices", "DeviceRequests", "DeviceCgroupRules"):
        entries = nullable_field(host, key, list)
        for entry in entries:
            require(entry, str if key == "DeviceCgroupRules" else dict)
        devices.extend(entries)
    mounts = field(container, "Mounts", list)
    destinations = {}
    socket_mount = False
    unapproved_writable = 0
    other_readonly = 0
    for mount in mounts:
        require(mount, dict)
        destination = field(mount, "Destination", str)
        mount_type = field(mount, "Type", str)
        source = field(mount, "Source", str, "" if mount_type == "tmpfs" else None)
        writable = field(mount, "RW", bool)
        if (mount_type not in ("bind", "volume", "tmpfs")
                or (mount_type != "tmpfs" and not source.startswith("/"))
                or not destination.startswith("/") or destination != posixpath.normpath(destination)
                or destination in destinations):
            raise AuditError("invalid Docker mount metadata")
        destinations[destination] = (mount_type, writable)
        # Docker volume Sources are daemon-managed data directories, not binds of
        # the runtime itself (normally /var/lib/docker/volumes/<name>/_data).
        volume_data = mount_type == "volume" and re.fullmatch(
            r"/var/lib/docker/volumes/[^/.][^/]*/_data", source) is not None
        socket_mount |= bool(source) and not volume_data and runtime_path(source)
        socket_mount |= runtime_path(destination)
        unapproved_writable += int(writable and not (
            (destination == HOME and mount_type in ("bind", "volume")) or
            (destination in TMPFS and mount_type == "tmpfs")))
        other_readonly += int(not writable and destination not in TLS)
    tmpfs = nullable_field(host, "Tmpfs", dict)
    for path, options in tmpfs.items():
        require(options, str)
        if not path.startswith("/"):
            raise AuditError("invalid Docker tmpfs metadata")
        unapproved_writable += int(path not in TMPFS)
    hardened_tmpfs = all(
        path in tmpfs and tmpfs_hardened(tmpfs[path]) and
        (path not in destinations or destinations[path] == ("tmpfs", True))
        for path in TMPFS)
    log = field(host, "LogConfig", dict)
    log_type = field(log, "Type", str)
    log_options = field(log, "Config", dict)
    for value in log_options.values():
        require(value, str)
    size = re.fullmatch(r"([1-9][0-9]*)([kmg]?)", log_options.get("max-size", ""))
    count = re.fullmatch(r"[1-9][0-9]*", log_options.get("max-file", ""))
    try:
        log_size = int(size[1]) * 1024 ** ("", "k", "m", "g").index(size[2]) if size else 0
        log_count = int(count[0]) if count else 0
    except ValueError:
        raise AuditError("invalid Docker log limits") from None
    healthcheck = config.get("Healthcheck")
    healthcheck = {} if healthcheck is None else require(healthcheck, dict)
    test = strings(healthcheck.get("Test", []))
    health = state.get("Health")
    health = {} if health is None else require(health, dict)
    health_status = field(health, "Status", str, "")
    publication = {"loopback": 0, "non_loopback": 0}
    ports = nullable_field(host, "PortBindings", dict)
    for bindings in ports.values():
        for binding in ([] if bindings is None else require(bindings, list)):
            require(binding, dict)
            address = field(binding, "HostIp", str)
            field(binding, "HostPort", str)
            try:
                loopback = ipaddress.ip_address(address).is_loopback
            except ValueError:
                loopback = False
            publication["loopback" if loopback else "non_loopback"] += 1
    checks = {
        "image_identity": actual_id == expected_id and expected in digests,
        "running": field(state, "Running", bool),
        "non_root": uid > 0 and (gid > 0 or (numeric_user is not None and numeric_user[2] is None)),
        "non_privileged": not field(host, "Privileged", bool),
        "capabilities": "ALL" in cap_drop and not cap_add,
        "no_new_privileges": any(option in security for option in
                                 ("no-new-privileges", "no-new-privileges:true"))
                             and "no-new-privileges:false" not in security,
        "read_only_root": field(host, "ReadonlyRootfs", bool),
        "resource_limits": resources["memory_bytes"] > 0 and resources["pids"] > 0 and
                           (resources["nano_cpus"] > 0 or
                            (resources["cpu_quota"] > 0 and resources["cpu_period"] > 0)),
        "isolated_namespaces": pid_mode in ("", "private") and ipc_mode in ("", "private", "none")
                               and network_mode != "host" and not network_mode.startswith("container:"),
        "no_devices_or_runtime_sockets": not devices and not socket_mount,
        "writable_home": destinations.get(HOME) in (("volume", True), ("bind", True))
                         and HOME not in tmpfs,
        "approved_writable_mounts": unapproved_writable == 0,
        "hardened_tmpfs": hardened_tmpfs,
        "readonly_tls_mounts": all(destinations.get(path) == ("bind", False) for path in TLS),
        "bounded_local_logging": log_type == "local" and log_size > 0 and log_count > 0,
        "healthcheck": len(test) > 1 and test[0] in ("CMD", "CMD-SHELL") and bool(test[1].strip()),
        "healthy": health_status == "healthy",
    }
    snapshot = {
        "image_id": actual_id, "checks": checks.copy(), "user": {"uid": uid, "gid": gid},
        "resources": resources, "publication": publication,
        "mounts": {"home_is_volume": destinations.get(HOME) == ("volume", True),
                   "unapproved_writable": unapproved_writable, "other_readonly": other_readonly},
        "logging": {"max_size_bytes": log_size, "max_files": log_count},
    }
    return {"schema_version": SCHEMA_VERSION, "scope": SCOPE, "image_id": actual_id,
            "checks": checks, "snapshot": snapshot,
            "drift": {"compared": False, "detected": False, "fields": []}}


def differences(previous, current, prefix=""):
    if type(previous) is not type(current):
        raise AuditError("invalid baseline report")
    if isinstance(current, dict):
        if previous.keys() != current.keys():
            raise AuditError("invalid baseline report")
        changed = []
        for key in current:
            changed.extend(differences(previous[key], current[key],
                                       prefix + "." + key if prefix else key))
        return changed
    return [prefix] if previous != current else []


def snapshot_fields(snapshot, prefix=""):
    result = []
    for key, value in snapshot.items():
        name = prefix + "." + key if prefix else key
        result.extend(snapshot_fields(value, name) if isinstance(value, dict) else [name])
    return result


def compare_baseline(path, report):
    try:
        with open(path, encoding="utf-8") as handle:
            previous = decode(handle.read())
        if (type(previous) is not dict or previous.keys() != report.keys()
                or type(previous["schema_version"]) is not int
                or previous["schema_version"] != SCHEMA_VERSION
                or previous["scope"] != SCOPE
                or not isinstance(previous["image_id"], str)
                or not re.fullmatch(IMAGE_ID, previous["image_id"])):
            raise ValueError
        differences(previous["checks"], report["checks"])
        drift = previous["drift"]
        if (type(drift) is not dict or drift.keys() != report["drift"].keys()
                or type(drift["compared"]) is not bool or type(drift["detected"]) is not bool
                or type(drift["fields"]) is not list
                or any(type(item) is not str or item not in snapshot_fields(report["snapshot"])
                       for item in drift["fields"])
                or drift["detected"] != bool(drift["fields"])
                or (drift["detected"] and not drift["compared"])):
            raise ValueError
        if (previous["snapshot"]["image_id"] != previous["image_id"]
                or previous["snapshot"]["checks"] != previous["checks"]):
            raise ValueError
        fields = sorted(differences(previous["snapshot"], report["snapshot"]))
    except (OSError, ValueError, RecursionError, KeyError, TypeError):
        raise AuditError("invalid or unreadable baseline report") from None
    report["drift"] = {"compared": True, "detected": bool(fields), "fields": fields}


def main(argv=None):
    parser = Parser(description=__doc__ + " " + SCOPE, allow_abbrev=False)
    parser.add_argument("--container", required=True, help="container name or ID")
    parser.add_argument("--expected-image", required=True, help="REPO@sha256:<64 lowercase hex>")
    parser.add_argument("--baseline", help="previous versioned JSON report; compare normalized metadata only")
    parser.add_argument("--strict", action="store_true", help="exit 1 for failed checks or snapshot drift")
    try:
        args = parser.parse_args(argv)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.container):
            raise AuditError("invalid container name")
        if not re.fullmatch(IMAGE_REF, args.expected_image):
            raise AuditError("expected image must be an immutable repository digest")
        report = audit(inspect("container", args.container),
                       inspect("image", args.expected_image), args.expected_image)
        if args.baseline is not None:
            compare_baseline(args.baseline, report)
    except SystemExit as error:
        return error.code
    except AuditError as error:
        print("audit_runtime: " + str(error), file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True))
    return int(args.strict and (not all(report["checks"].values()) or report["drift"]["detected"]))


if __name__ == "__main__":
    sys.exit(main())
