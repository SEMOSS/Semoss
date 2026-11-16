"""Exercise staged Office skills using SEMOSS's configured Python environment."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import re
import runpy
import shutil
import tempfile
import types
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from docx import Document
from lxml import etree
from openpyxl import Workbook, load_workbook

REPO = Path(__file__).resolve().parents[2]
W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def rewrite_package(source, destination, changes):
    with ZipFile(source) as old, ZipFile(destination, "w") as new:
        for item in old.infolist():
            new.writestr(item, changes.get(item.filename, old.read(item)))


class OfficeSkillsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.skills = self.root / ".claude/skills"
        for name in ("docx", "xlsx"):
            shutil.copytree(REPO / f"project/platform__{name}/app_root/version/assets/public",
                            self.skills / name)
        self.replace = runpy.run_path(str(self.skills / "docx/scripts/replace_text.py"))["replace_text"]
        self.inspect = runpy.run_path(str(self.skills / "xlsx/scripts/inspect_workbook.py"))["inspect_workbook"]

    def example(self, name, file):
        smssutil = types.ModuleType("smssutil")
        smssutil.smss_get_runtime_var = lambda key: str(self.root) if key == "ROOT" else None
        with patch.dict("sys.modules", {"smssutil": smssutil}), redirect_stdout(io.StringIO()):
            return runpy.run_path(str(self.skills / name / "examples" / file))["result"]

    def test_examples_use_runtime_root_and_preserve_user_edits(self):
        self.assertNotEqual(self.root, Path.cwd())
        doc_result = self.example("docx", "create-document.py")
        document_path = self.root / doc_result["filePath"]
        document = Document(document_path)
        self.assertEqual(document.paragraphs[0].text, "Project brief")
        self.assertEqual(len(document.tables[0].rows), 3)
        self.assertEqual(document.sections[0].footer.paragraphs[0].text, "Project brief")
        document.add_paragraph("A later user edit")
        document.save(document_path)
        before = document_path.read_bytes()
        with self.assertRaises(FileExistsError):
            self.example("docx", "create-document.py")
        self.assertEqual(before, document_path.read_bytes())

        workbook_result = self.example("xlsx", "create-workbook.py")
        workbook_path = self.root / workbook_result["filePath"]
        workbook = load_workbook(workbook_path)
        self.assertEqual(workbook.sheetnames, ["Cost plan", "Notes"])
        self.assertEqual(workbook["Cost plan"]["D4"].value, "=SUM(D2:D3)")
        self.assertIn("CostItems", workbook["Cost plan"].tables)
        self.assertEqual(len(workbook["Cost plan"]._charts), 1)
        self.assertEqual(workbook["Cost plan"].freeze_panes, "B2")
        workbook["Cost plan"]["B2"] = 3
        revised = self.root / "revised.xlsx"
        workbook.save(revised)
        workbook.close()
        report = self.inspect(revised)
        self.assertEqual(report["total_formulas"], 3)
        self.assertEqual(report["missing_caches"], 3)
        self.assertFalse(report["calculation_verified"])
        with self.assertRaises(FileExistsError):
            self.example("xlsx", "create-workbook.py")

    def test_word_replacement_spans_formatted_runs_and_preserves_other_parts(self):
        source, output = self.root / "source.docx", self.root / "updated.docx"
        document = Document()
        paragraph = document.add_paragraph()
        paragraph.add_run("Before old ").bold = True
        paragraph.add_run("title after").italic = True
        other = document.add_paragraph("Unchanged comment anchor")
        document.add_comment(other.runs, "Keep this comment", author="Reviewer")
        document.sections[0].header.paragraphs[0].text = "Keep this header"
        document.save(source)
        self.replace(source, output, "old title", "new heading")
        reopened = Document(output)
        self.assertEqual(reopened.paragraphs[0].text, "Before new heading after")
        self.assertTrue(reopened.paragraphs[0].runs[0].bold)
        self.assertTrue(reopened.paragraphs[0].runs[1].italic)
        with ZipFile(source) as old, ZipFile(output) as new:
            self.assertEqual(set(old.namelist()), set(new.namelist()))
            for name in old.namelist():
                if name != "word/document.xml":
                    self.assertEqual(old.read(name), new.read(name), name)

    def test_word_replacement_refuses_ambiguity_and_complex_runs_without_output(self):
        source, output = self.root / "source.docx", self.root / "updated.docx"
        document = Document()
        document.add_paragraph("old old")
        document.save(source)
        with self.assertRaisesRegex(ValueError, "Expected 1 matches, found 2"):
            self.replace(source, output, "old", "new")
        self.assertFalse(output.exists())
        self.replace(source, output, "old", "newer", expected=2)
        self.assertEqual(Document(output).paragraphs[0].text, "newer newer")
        with self.assertRaises(ValueError):
            self.replace(source, source, "old", "new", expected=2)
        document = Document()
        document.add_paragraph().add_run("old\ttext")
        document.save(source)
        with self.assertRaisesRegex(ValueError, "non-text run"):
            self.replace(source, self.root / "complex.docx", "old", "new")
        self.assertFalse((self.root / "complex.docx").exists())

    def test_workbook_inventory_distinguishes_errors_blank_caches_and_freshness(self):
        source, candidate = self.root / "source.xlsx", self.root / "candidate.xlsx"
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "Model"
        sheet["A1"], sheet["A2"], sheet["A3"] = "=1+2", '=IF(TRUE,"",1)', "=1/0"
        sheet["B1"] = "#DIV/0!"
        sheet["B1"].data_type = "s"  # Literal user text is not an Excel error.
        hidden = workbook.create_sheet("Hidden")
        hidden.sheet_state = "hidden"
        hidden["A1"] = "#REF!"
        workbook.save(source)
        with ZipFile(source) as package:
            xml = etree.fromstring(package.read("xl/worksheets/sheet1.xml"))
            for cell in xml.iter(S + "c"):
                coordinate = cell.get("r")
                if coordinate == "A1":
                    cell.find(S + "v").text = "3"
                if coordinate == "A2":
                    cell.set("t", "str")
                if coordinate == "A3":
                    cell.set("t", "e")
                    cell.find(S + "v").text = "#DIV/0!"
            rewrite_package(source, candidate, {"xl/worksheets/sheet1.xml": etree.tostring(xml)})
        report = self.inspect(candidate, sample_limit=0)
        self.assertEqual(report["total_formulas"], 3)
        self.assertEqual(report["total_errors"], 2)
        self.assertEqual(report["missing_caches"], 0)
        self.assertEqual(report["cache_status"], "present_unverified")
        self.assertFalse(report["calculation_verified"])
        self.assertEqual(report["sheets"][0]["error_samples"], [])
        self.assertEqual(report["sheets"][1]["state"], "hidden")

    def test_inventory_reports_link_and_macro_risks_without_rewriting(self):
        source = self.root / "source.xlsx"
        Workbook().save(source)
        with ZipFile(source, "a") as package:
            package.writestr("xl/externalLinks/externalLink1.xml", "<link/>")
            package.writestr("xl/vbaProject.bin", b"preserve-me")
        before = source.read_bytes()
        report = self.inspect(source)
        self.assertEqual(report["cache_status"], "not_applicable")
        self.assertIn("external_links", report["preservation_risks"])
        self.assertIn("macros", report["preservation_risks"])
        self.assertEqual(source.read_bytes(), before)

    def test_resources_are_readable_and_links_survive_staging(self):
        for name in ("docx", "xlsx"):
            skill = self.skills / name
            for file in skill.rglob("*"):
                if not file.is_file() or file.suffix not in (".md", ".py"):
                    continue
                self.assertLessEqual(file.stat().st_size, 8192, str(file))
                if file.suffix == ".md":
                    prose = re.sub(r"```.*?```", "", file.read_text(), flags=re.DOTALL)
                    for target in re.findall(r"\[[^\]\n]+\]\(([^)\n]+)\)", prose):
                        if not target.startswith(("http", "room:", "#")):
                            self.assertTrue((file.parent / target).is_file(), target)

    def test_xml_helpers_reject_dtds_without_expanding_entities(self):
        document_path = self.root / "document.docx"
        document = Document()
        document.add_paragraph("old")
        document.save(document_path)
        hostile_docx = self.root / "hostile.docx"
        malicious = (b'<!DOCTYPE document [<!ENTITY payload SYSTEM "file:///nonexistent">]>'
                     b'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                     b'<w:body><w:p><w:r><w:t>&payload;</w:t></w:r></w:p></w:body></w:document>')
        rewrite_package(document_path, hostile_docx, {"word/document.xml": malicious})
        with self.assertRaisesRegex(ValueError, "type declarations"):
            self.replace(hostile_docx, self.root / "blocked.docx", "old", "new")
        self.assertFalse((self.root / "blocked.docx").exists())
        workbook_path = self.root / "workbook.xlsx"
        Workbook().save(workbook_path)
        hostile_xlsx = self.root / "hostile.xlsx"
        rewrite_package(workbook_path, hostile_xlsx, {"xl/workbook.xml": malicious})
        with self.assertRaisesRegex(ValueError, "type declarations"):
            self.inspect(hostile_xlsx)

    def test_word_helper_preserves_existing_revision_markup(self):
        source, marked = self.root / "source.docx", self.root / "marked.docx"
        document = Document()
        document.add_paragraph("old")
        document.save(source)
        with ZipFile(source) as package:
            xml = etree.fromstring(package.read("word/document.xml"))
        paragraph = next(xml.iter(W + "p"))
        properties = etree.Element(W + "pPr")
        etree.SubElement(properties, W + "pPrChange")
        paragraph.insert(0, properties)
        rewrite_package(source, marked, {"word/document.xml": etree.tostring(xml)})
        with self.assertRaisesRegex(ValueError, "revision markup"):
            self.replace(marked, self.root / "blocked.docx", "old", "new")
        self.assertFalse((self.root / "blocked.docx").exists())


if __name__ == "__main__":
    unittest.main()
