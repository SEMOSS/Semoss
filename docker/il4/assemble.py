"""Resolve locked upstream release artifacts and assemble the container payload."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import xml.etree.ElementTree as ET
import zipfile

from audio_compat import patch_audio
from accp_support import ACCP_COORDINATE, install_accp


JDBC_OVERLAYS = (
    ("mariadb-java-client-1.1.9.jar", "org.mariadb.jdbc:mariadb-java-client:3.5.10:jar"),
    ("mssql-jdbc-11.2.4.jre11.jar", "com.microsoft.sqlserver:mssql-jdbc:13.6.0.jre11:jar"),
)


def overlay_jdbc(library, files):
    for old, coordinate in JDBC_OVERLAYS:
        artifact = coordinate.split(":")[1]
        if sorted(path.name for path in library.glob(artifact + "-*.jar")) != [old]:
            raise ValueError("Unexpected JDBC baseline for " + artifact)
    report = []
    for old, coordinate in JDBC_OVERLAYS:
        source = files[coordinate]
        original = library / old
        report.append({
            "removed": old,
            "removed_sha256": hashlib.sha256(original.read_bytes()).hexdigest(),
            "coordinate": coordinate,
            "replacement": source.name,
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        })
        shutil.copy2(source, library / source.name)
        original.unlink()
    return report


def archive_path(name, root):
    parts = PurePosixPath(name).parts
    if "\\" in name or name.startswith("/") or ".." in parts:
        raise ValueError("Unsafe archive path: " + name)
    if root:
        if not parts or parts[0] != root:
            raise ValueError("Unexpected archive root: " + name)
        parts = parts[1:]
    return "/".join(parts)


def linux_paths(text):
    return re.sub(r"C:(?:/+|\\+)workspace(?:/+|\\+)Semoss"
                  r"((?:(?:/+|\\+)[^\s<>\";]*)?)",
                  lambda match: re.sub(r"\\+", "/", match[0])
                  .replace("C:/workspace/Semoss", "/opt/semosshome"), text)


def has_bc_classes(source):
    with zipfile.ZipFile(source) as jar:
        return any("org/bouncycastle/" in name and name.endswith(".class")
                   for name in jar.namelist())


def set_properties(text, values):
    for key, value in values.items():
        text, count = re.subn(r"(?m)^" + re.escape(key) + r"(?:\s+|=)[^\r\n]*$",
                             key + "\t" + value, text)
        if count != 1:
            raise ValueError("Expected exactly one configuration key: " + key)
    return text


def configure_web(root):
    namespace = root.tag.split("}")[0] + "}" if "}" in root.tag else ""
    if namespace:
        ET.register_namespace("", namespace[1:-1])
    overrides = {"RDF-MAP": "/opt/semosshome/RDF_Map.prop",
                 "file-upload": "/opt/semosshome/upload",
                 "temp-file-upload": "/tmp",
                 "log4jConfiguration": "file:///opt/semosshome/log4j2.xml"}
    for param in root.findall(namespace + "context-param"):
        name = param.find(namespace + "param-name")
        value = param.find(namespace + "param-value")
        if name is not None and value is not None and name.text in overrides:
            value.text = overrides[name.text]
    session = root.find(namespace + "session-config")
    if session is None:
        session = ET.SubElement(root, namespace + "session-config")
    timeout = session.find(namespace + "session-timeout")
    if timeout is None:
        timeout = ET.SubElement(session, namespace + "session-timeout")
    timeout.text = "15"
    cookies = session.find(namespace + "cookie-config")
    if cookies is None:
        cookies = ET.SubElement(session, namespace + "cookie-config")
    for key in ("http-only", "secure"):
        element = cookies.find(namespace + key)
        if element is None:
            element = ET.SubElement(cookies, namespace + key)
        element.text = "true"


def extract(source, destination, root=None):
    destination.mkdir(parents=True, exist_ok=True)
    if source.name.endswith(".tar.gz"):
        with tarfile.open(source) as archive:
            for member in archive:
                relative = archive_path(member.name, root)
                if member.isdir():
                    (destination / relative).mkdir(parents=True, exist_ok=True)
                elif member.isfile() and relative:
                    target = destination / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.extractfile(member) as stream, target.open("wb") as output:
                        shutil.copyfileobj(stream, output)
                    target.chmod(0o755 if member.mode & 0o111 else 0o644)
                else:
                    raise ValueError("Unsupported archive member: " + member.name)
    else:
        with zipfile.ZipFile(source) as archive:
            for member in archive.infolist():
                relative = archive_path(member.filename, root)
                if (member.external_attr >> 16) & 0o170000 == 0o120000:
                    raise ValueError("Archive symlink: " + member.filename)
                target = destination / relative
                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member) as stream, target.open("wb") as output:
                        shutil.copyfileobj(stream, output)


def resolve(manifest, downloads):
    project = ET.Element("project")
    for key, value in (("modelVersion", "4.0.0"), ("groupId", "local"),
                       ("artifactId", "semoss-container"), ("version", "1")):
        ET.SubElement(project, key).text = value
    plugin = ET.SubElement(ET.SubElement(ET.SubElement(project, "build"), "plugins"), "plugin")
    for key, value in (("groupId", "org.apache.maven.plugins"),
                       ("artifactId", "maven-dependency-plugin"), ("version", "3.8.1")):
        ET.SubElement(plugin, key).text = value
    config = ET.SubElement(plugin, "configuration")
    ET.SubElement(config, "outputDirectory").text = str(downloads)
    items = ET.SubElement(config, "artifactItems")
    files = {}
    for entry in manifest:
        group, artifact, version, kind, *classifier = entry["coordinate"].split(":")
        item = ET.SubElement(items, "artifactItem")
        for key, value in (("groupId", group), ("artifactId", artifact),
                           ("version", version), ("type", kind)):
            ET.SubElement(item, key).text = value
        suffix = "-" + classifier[0] if classifier else ""
        if classifier:
            ET.SubElement(item, "classifier").text = classifier[0]
        files[entry["coordinate"]] = downloads / (artifact + "-" + version + suffix + "." + kind)
    ET.ElementTree(project).write("pom.xml", encoding="utf-8", xml_declaration=True)
    subprocess.run(["mvn", "-B", "-ntp", "-C",
                    "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy"], check=True)
    for entry in manifest:
        with files[entry["coordinate"]].open("rb") as stream:
            digest = hashlib.sha256()
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != entry["sha256"]:
            raise ValueError("SHA-256 mismatch: " + entry["coordinate"])
    return files


def main(output=Path("/out")):
    manifest = json.loads(Path("artifacts.lock.json").read_text())
    files = resolve(manifest, Path("/build/downloads"))
    tomcat = output / "tomcat"
    home = output / "semosshome"
    accp = output / "accp"
    accp.mkdir(parents=True)
    extract(files["org.apache.tomcat:tomcat:9.0.119:tar.gz"], tomcat, "apache-tomcat-9.0.119")
    shutil.rmtree(tomcat / "webapps")
    extract(files["org.semoss:semoss:5.4.0:tar.gz:semosshome"], home, "semoss-5.4.0")
    monolith = tomcat / "webapps/Monolith"
    extract(files["org.semoss:monolith:5.4.0:war"], monolith)
    extract(files["org.semoss:monolith:5.4.0:tar.gz:libraries"], monolith, "monolith-5.4.0")
    extract(files["org.semoss:semossweb:5.4.0:war"], tomcat / "webapps/SemossWeb")
    removed = []
    excluded_connectors = []
    for jar in sorted(monolith.glob("WEB-INF/lib/*.jar")):
        if re.fullmatch(r"bc(?:prov|pkix|util|tls)-jdk(?:15on|15to18|18on)-[\d.]+\.jar", jar.name):
            removed.append(jar.name)
            jar.unlink()
        elif jar.name == "snowflake-jdbc-3.22.0.jar":
            excluded_connectors.append(jar.name)
            jar.unlink()
    if not removed:
        raise ValueError("Expected stock Bouncy Castle dependencies were not found")
    jdbc_overlays = overlay_jdbc(monolith / "WEB-INF/lib", files)
    # Shaded copies are not rewritten: doing so can break signed or relocated code.
    for jar in sorted(tomcat.rglob("*.jar")):
        if has_bc_classes(jar):
            raise ValueError("Bundled Bouncy Castle classes require review: " + str(jar))
    sqlite_jar = monolith / "WEB-INF/lib/sqlite-jdbc-3.43.2.1.jar"
    sqlite_entry = "org/sqlite/native/Linux/x86_64/libsqlitejdbc.so"
    sqlite_dir = output / "sqlite"
    sqlite_dir.mkdir()
    with zipfile.ZipFile(sqlite_jar) as archive:
        sqlite_native = archive.read(sqlite_entry)
    sqlite_library = sqlite_dir / "libsqlitejdbc.so"
    sqlite_library.write_bytes(sqlite_native)
    sqlite_library.chmod(0o555)
    for coordinate, file in files.items():
        if coordinate.startswith("org.bouncycastle:"):
            # Retain ASN.1/PEM feature dependencies, not registered JCA/JSSE providers.
            shutil.copy2(file, accp)
    accp_native = install_accp(files[ACCP_COORDINATE], accp)
    for file in home.rglob("*"):
        if file.is_file() and file.suffix in (".prop", ".properties", ".smss", ".xml"):
            text = file.read_text(encoding="cp1252")
            file.write_text(linux_paths(text), encoding="cp1252")
    runtime_flags = {"USE_PYTHON": "true", "PYTHONHOME": "/opt/semoss-python",
                     "NETTY_PYTHON": "true", "NATIVE_PY_SERVER": "true",
                     "DEFAULT_SCRIPTING_LANGUAGE": "PY",
                     "R_KILL_ON_STARTUP": "false",
                     "NOTIFICATION_DATABASE_ENABLED": "false"}
    rdf = home / "RDF_Map.prop"
    rdf.write_text(set_properties(rdf.read_text(encoding="cp1252"), runtime_flags),
                   encoding="cp1252")
    audio_patch = patch_audio(home / "py/audio/lk_to_pcat.py")
    social = home / "social.properties"
    social.write_text(set_properties(social.read_text(encoding="cp1252"),
                                    {"native_registration": "false",
                                     "redirect": "https://localhost:8443/SemossWeb/"}),
                      encoding="cp1252")
    web = monolith / "WEB-INF/web.xml"
    tree = ET.parse(web)
    configure_web(tree.getroot())
    tree.write(web, encoding="utf-8", xml_declaration=True)
    for name in ("upload", "InsightCache", "project", "user", "model", "storage", "vector",
                 "function", "guardrail", "venv", "temp"):
        (home / name).mkdir(exist_ok=True)
    report = output / "provenance"
    report.mkdir()
    shutil.copy2("artifacts.lock.json", report)
    (report / "assembly.json").write_text(json.dumps({
        "semoss_source": "https://github.com/SEMOSS/Semoss/tree/v5.4.0",
        "semoss_commit": "565b2c1825011304a93eabcb21cf2cd33135a42a",
        "mode": "assemble published 5.4.0 binaries; not a source rebuild",
        "removed_standard_bc_jars": removed,
        "excluded_connectors": excluded_connectors,
        "jdbc_overlays": jdbc_overlays,
        "crypto_status": "experimental/nonvalidated; no certificate coverage claimed",
        "accp_native": accp_native,
        "bc_feature_dependencies": "ASN.1/PEM only; BCFIPS and BCJSSE are not registered",
        "runtime_flags": runtime_flags,
        "audio_compatibility_patch": audio_patch,
        "sqlite_native": {
            "source_jar": sqlite_jar.name,
            "source_jar_sha256": hashlib.sha256(sqlite_jar.read_bytes()).hexdigest(),
            "entry": sqlite_entry,
            "sha256": hashlib.sha256(sqlite_native).hexdigest(),
            "runtime_path": "/opt/sqlite/libsqlitejdbc.so",
        },
        "native_registration": False,
        "login_redirect": "https://localhost:8443/SemossWeb/",
        "changes": ["Experimental/nonvalidated ACCP + SunJSSE + PKCS12 overlay", "Linux home paths",
                    "15-minute sessions; Secure and HttpOnly cookies"]
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
