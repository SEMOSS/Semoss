import io
import hashlib
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
import zipfile

from assemble import archive_path, has_bc_classes, linux_paths, configure_web, set_properties
from assemble import extract, main, resolve, overlay_jdbc, JDBC_OVERLAYS
from accp_support import ACCP_COORDINATE, NATIVE_ENTRY


def zip_bytes(entries):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return output.getvalue()


def make_tar(path, entries):
    with tarfile.open(path, "w:gz") as archive:
        for name, data in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            member.mode = 0o755 if name.endswith(".sh") else 0o644
            archive.addfile(member, io.BytesIO(data))


class AssemblyTests(unittest.TestCase):
    def jdbc_fixtures(self, base):
        library = base / "lib"
        library.mkdir()
        files = {}
        for old, coordinate in JDBC_OVERLAYS:
            (library / old).write_bytes(zip_bytes({"Old.class": b"old"}))
            _, artifact, version, _ = coordinate.split(":")
            new = base / (artifact + "-" + version + ".jar")
            new.write_bytes(zip_bytes({"New.class": b"new"}))
            files[coordinate] = new
        return library, files

    def test_jdbc_overlay_records_exact_replacements(self):
        with tempfile.TemporaryDirectory() as temporary:
            library, files = self.jdbc_fixtures(Path(temporary))
            report = overlay_jdbc(library, files)
            self.assertEqual(len(report), 2)
            for entry, (old, coordinate) in zip(report, JDBC_OVERLAYS):
                self.assertFalse((library / old).exists())
                self.assertEqual((library / files[coordinate].name).read_bytes(),
                                 files[coordinate].read_bytes())
                self.assertEqual(entry["removed"], old)
                self.assertEqual(entry["coordinate"], coordinate)
                self.assertEqual(entry["sha256"], hashlib.sha256(
                    files[coordinate].read_bytes()).hexdigest())
                self.assertEqual(len(entry["removed_sha256"]), 64)

    def test_jdbc_overlay_rejects_missing_or_duplicate_baseline(self):
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate), tempfile.TemporaryDirectory() as temporary:
                library, files = self.jdbc_fixtures(Path(temporary))
                old, coordinate = JDBC_OVERLAYS[0]
                if duplicate:
                    (library / files[coordinate].name).write_bytes(b"unexpected duplicate")
                else:
                    (library / old).unlink()
                with self.assertRaisesRegex(ValueError, "JDBC baseline"):
                    overlay_jdbc(library, files)

    def test_archive_paths(self):
        self.assertEqual(archive_path("bundle/WEB-INF/lib/a.jar", "bundle"),
                         "WEB-INF/lib/a.jar")
        self.assertEqual(archive_path("WEB-INF/web.xml", None), "WEB-INF/web.xml")
        self.assertEqual(archive_path("bundle/", "bundle"), "")

    def test_reject_unsafe_or_wrong_root_paths(self):
        for name in ("/etc/passwd", "../bad", "bundle/../../bad",
                     "wrong/file", "bundle\\file", "bundle/a/../file"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                archive_path(name, "bundle")

    def test_linux_paths(self):
        for path in ("C:/workspace/Semoss/db", r"C:\workspace\Semoss\db",
                     r"C:\\workspace\\Semoss\\db"):
            with self.subTest(path=path):
                self.assertEqual(linux_paths(path), "/opt/semosshome/db")
        self.assertEqual(linux_paths("https://example.test/api"), "https://example.test/api")

    def test_property_overrides(self):
        text = "# switch\nUSE_PYTHON true\nKEY=value\n"
        self.assertEqual(set_properties(text, {"USE_PYTHON": "false", "KEY": "updated"}),
                         "# switch\nUSE_PYTHON\tfalse\nKEY\tupdated\n")
        with self.assertRaises(ValueError):
            set_properties(text, {"MISSING": "false"})
        with self.assertRaises(ValueError):
            set_properties("KEY a\nKEY b\n", {"KEY": "c"})

    def test_bc_class_detection(self):
        for name, expected in (("org/bouncycastle/crypto/Test.class", True),
                               ("shaded/org/bouncycastle/crypto/Test.class", True),
                               ("META-INF/maven/org.bouncycastle/pom.xml", False),
                               ("org/example/Test.class", False)):
            with self.subTest(name=name):
                data = io.BytesIO(zip_bytes({name: b"test"}))
                self.assertEqual(has_bc_classes(data), expected)

    def test_web_configuration(self):
        root = ET.fromstring('''<web-app xmlns="http://xmlns.jcp.org/xml/ns/javaee">
          <context-param><param-name>RDF-MAP</param-name>
          <param-value>C:/workspace/Semoss/RDF_Map.prop</param-value></context-param>
          <session-config><session-timeout>120</session-timeout>
          <cookie-config><http-only>false</http-only><secure>false</secure>
          </cookie-config></session-config></web-app>''')
        configure_web(root)
        self.assertTrue(ET.tostring(root).startswith(b"<web-app "))
        ns = {"j": "http://xmlns.jcp.org/xml/ns/javaee"}
        self.assertEqual(root.find(".//j:param-value", ns).text,
                         "/opt/semosshome/RDF_Map.prop")
        self.assertEqual(root.find(".//j:http-only", ns).text, "true")
        self.assertEqual(root.find(".//j:secure", ns).text, "true")
        self.assertEqual(root.find(".//j:session-timeout", ns).text, "15")

    def test_web_configuration_creates_session(self):
        for xml in ("<web-app/>", '<web-app xmlns="urn:test"/>'):
            with self.subTest(xml=xml):
                root = ET.fromstring(xml)
                configure_web(root)
                self.assertIn(b"session-timeout", ET.tostring(root))
                self.assertIn(b"cookie-config", ET.tostring(root))

    def test_extract_archives(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            tar = base / "bundle.tar.gz"
            make_tar(tar, {"bundle/bin/start.sh": b"test", "bundle/conf/file": b"config"})
            extract(tar, base / "tar", "bundle")
            self.assertEqual((base / "tar/bin/start.sh").read_bytes(), b"test")
            self.assertEqual((base / "tar/bin/start.sh").stat().st_mode & 0o777, 0o755)
            war = base / "test.war"
            war.write_bytes(zip_bytes({"WEB-INF/": b"", "WEB-INF/web.xml": b"<web-app/>"}))
            extract(war, base / "zip")
            self.assertEqual((base / "zip/WEB-INF/web.xml").read_bytes(), b"<web-app/>")

    def test_reject_archive_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            tar = base / "bad.tar.gz"
            with tarfile.open(tar, "w:gz") as archive:
                member = tarfile.TarInfo("link")
                member.type = tarfile.SYMTYPE
                member.linkname = "/etc/passwd"
                archive.addfile(member)
            with self.assertRaisesRegex(ValueError, "Unsupported"):
                extract(tar, base / "tar")
            war = base / "bad.war"
            with zipfile.ZipFile(war, "w") as archive:
                member = zipfile.ZipInfo("link")
                member.external_attr = 0o120777 << 16
                archive.writestr(member, "/etc/passwd")
            with self.assertRaisesRegex(ValueError, "symlink"):
                extract(war, base / "zip")

    @patch("assemble.ET.ElementTree.write")
    @patch("assemble.subprocess.run")
    def test_resolve_verifies_hashes_and_maven_failure(self, run, write):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            payload = base / "a-1-tests.jar"
            payload.write_bytes(b"locked")
            manifest = [{"coordinate": "example:a:1:jar:tests",
                         "sha256": hashlib.sha256(b"locked").hexdigest()}]
            self.assertEqual(resolve(manifest, base)[manifest[0]["coordinate"]], payload)
            self.assertTrue(run.call_args.kwargs["check"])
            self.assertIn("-C", run.call_args.args[0])
            self.assertTrue(write.called)
            payload.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                resolve(manifest, base)
            run.side_effect = RuntimeError("Maven failed")
            with self.assertRaisesRegex(RuntimeError, "Maven failed"):
                resolve(manifest, base)

    def test_full_assembly_with_local_fixtures(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            files = {}
            tomcat = base / "tomcat.tar.gz"
            make_tar(tomcat, {"apache-tomcat-9.0.119/webapps/ROOT/index.html": b"remove",
                             "apache-tomcat-9.0.119/bin/catalina.sh": b"test"})
            files["org.apache.tomcat:tomcat:9.0.119:tar.gz"] = tomcat
            home = base / "home.tar.gz"
            make_tar(home, {"semoss-5.4.0/RDF_Map.prop":
                           b"USE_PYTHON false\nPYTHONHOME /missing\nNETTY_PYTHON false\n"
                           b"NATIVE_PY_SERVER false\nDEFAULT_SCRIPTING_LANGUAGE R\n"
                           b"R_KILL_ON_STARTUP true\nNOTIFICATION_DATABASE_ENABLED true\n",
                           "semoss-5.4.0/social.properties":
                           b"native_registration true\nredirect http://localhost:9090/SemossWeb/\n"})
            files["org.semoss:semoss:5.4.0:tar.gz:semosshome"] = home
            war = base / "monolith.war"
            war.write_bytes(zip_bytes({"WEB-INF/web.xml": b"<web-app/>"}))
            files["org.semoss:monolith:5.4.0:war"] = war
            ui = base / "ui.war"
            ui.write_bytes(zip_bytes({"index.html": b"UI"}))
            files["org.semoss:semossweb:5.4.0:war"] = ui
            libraries = base / "libraries.tar.gz"
            standard = zip_bytes({"org/bouncycastle/Test.class": b"test"})
            entries = {"monolith-5.4.0/WEB-INF/lib/bcprov-jdk18on-1.78.1.jar": standard,
                       "monolith-5.4.0/WEB-INF/lib/snowflake-jdbc-3.22.0.jar": standard,
                       "monolith-5.4.0/WEB-INF/lib/sqlite-jdbc-3.43.2.1.jar":
                       zip_bytes({"org/sqlite/native/Linux/x86_64/libsqlitejdbc.so": b"native fixture"}),
                       "monolith-5.4.0/WEB-INF/lib/app.jar": zip_bytes({"App.class": b"test"})}
            jdbc_library, jdbc_files = self.jdbc_fixtures(base)
            files.update(jdbc_files)
            for jar in jdbc_library.iterdir():
                entries["monolith-5.4.0/WEB-INF/lib/" + jar.name] = jar.read_bytes()
            make_tar(libraries, entries)
            files["org.semoss:monolith:5.4.0:tar.gz:libraries"] = libraries
            fips = base / "bc-fips-2.1.3.jar"
            fips.write_bytes(standard)
            files["org.bouncycastle:bc-fips:2.1.3:jar"] = fips
            accp = base / "accp.jar"
            accp.write_bytes(zip_bytes({NATIVE_ENTRY: b"accp native fixture"}))
            files[ACCP_COORDINATE] = accp
            with patch("assemble.resolve", return_value=files), \
                    patch("assemble.patch_audio", return_value={"change": "tested separately"}) as audio_patch:
                output = base / "out"
                main(output)
                report = json.loads((output / "provenance/assembly.json").read_text())
                audio_patch.assert_called_once_with(output / "semosshome/py/audio/lk_to_pcat.py")
                self.assertEqual(report["audio_compatibility_patch"], {"change": "tested separately"})
                self.assertEqual(report["excluded_connectors"], ["snowflake-jdbc-3.22.0.jar"])
                self.assertEqual(len(report["jdbc_overlays"]), 2)
                self.assertFalse((output / "tomcat/webapps/ROOT").exists())
                self.assertEqual((output / "accp/bc-fips-2.1.3.jar").read_bytes(), standard)
                self.assertEqual((output / "accp/accp.jar").read_bytes(), accp.read_bytes())
                self.assertEqual((output / "accp/lib/libamazonCorrettoCryptoProvider.so").read_bytes(),
                                 b"accp native fixture")
                self.assertEqual(report["accp_native"]["runtime_path"],
                                 "/opt/accp/lib/libamazonCorrettoCryptoProvider.so")
                self.assertEqual((output / "sqlite/libsqlitejdbc.so").read_bytes(), b"native fixture")
                properties = (output / "semosshome/RDF_Map.prop").read_text()
                self.assertIn("USE_PYTHON\ttrue", properties)
                self.assertIn("PYTHONHOME\t/opt/semoss-python", properties)
                self.assertIn("NETTY_PYTHON\ttrue", properties)
                self.assertIn("NATIVE_PY_SERVER\ttrue", properties)
                self.assertIn("DEFAULT_SCRIPTING_LANGUAGE\tPY", properties)
                social = (output / "semosshome/social.properties").read_text()
                self.assertIn("native_registration\tfalse", social)
                self.assertIn("redirect\thttps://localhost:8443/SemossWeb/", social)
                entries["monolith-5.4.0/WEB-INF/lib/hidden.jar"] = standard
                make_tar(libraries, entries)
                with self.assertRaisesRegex(ValueError, "Bundled Bouncy Castle"):
                    main(base / "hidden")
                make_tar(libraries, {"monolith-5.4.0/WEB-INF/lib/app.jar":
                                     zip_bytes({"App.class": b"test"})})
                with self.assertRaisesRegex(ValueError, "Expected stock Bouncy Castle"):
                    main(base / "missing")


if __name__ == "__main__":
    unittest.main()
