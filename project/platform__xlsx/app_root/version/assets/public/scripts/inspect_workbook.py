"""Read stored workbook facts without saving or evaluating formulas."""
import posixpath
from zipfile import ZipFile

from lxml import etree

S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


def inspect_workbook(path, *, sample_limit=25):
    """Count all worksheet formula anchors, stored errors, and absent caches.

    Location lists are bounded; counts are not. A present cache is never proof of
    freshness. Empty string results (t=str with an empty v) are valid stored caches.
    """
    if sample_limit < 0:
        raise ValueError("sample_limit must be nonnegative")
    with ZipFile(path) as package:
        names = package.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate ZIP members are not supported")
        if sum(item.file_size for item in package.infolist()) > 256 * 1024 * 1024:
            raise ValueError("Workbook exceeds the 256 MiB uncompressed inspection limit")
        bad_member = package.testzip()
        if bad_member:
            raise ValueError(f"Corrupt ZIP member: {bad_member}")

        def read_xml(name):
            node = etree.fromstring(package.read(name), etree.XMLParser(
                resolve_entities=False, no_network=True, load_dtd=False))
            if node.getroottree().docinfo.doctype:
                raise ValueError("Document type declarations are not supported")
            return node

        workbook = read_xml("xl/workbook.xml")
        if workbook.tag != S + "workbook":
            raise ValueError("This helper supports transitional OOXML workbooks only")
        relationships = {
            node.get("Id"): node for node in read_xml("xl/_rels/workbook.xml.rels")
        }
        risks = []
        for label, prefix in [
            ("external_links", "xl/externalLinks/"),
            ("macros", "xl/vbaProject"),
            ("digital_signatures", "_xmlsignatures/"),
            ("pivots", "xl/pivot"),
            ("slicers", "xl/slicer"),
            ("data_connections", "xl/connections.xml"),
            ("embedded_objects", "xl/embeddings/"),
            ("drawings", "xl/drawings/"),
        ]:
            if any(name.startswith(prefix) for name in names):
                risks.append(label)
        sheets = []
        for sheet in workbook.findall(S + "sheets/" + S + "sheet"):
            rel = relationships[sheet.get(R + "id")]
            if rel.get("TargetMode") == "External":
                raise ValueError("An externally located worksheet cannot be inspected")
            target = rel.get("Target", "")
            member = posixpath.normpath(target.lstrip("/") if target.startswith("/")
                                       else posixpath.join("xl", target))
            if not member.startswith("xl/"):
                raise ValueError("Worksheet relationship leaves the workbook directory")
            sheet_xml = read_xml(member)
            report = {"name": sheet.get("name"), "state": sheet.get("state", "visible"),
                      "part": member, "formulas": 0, "missing_caches": 0, "errors": 0,
                      "missing_cache_samples": [], "error_samples": []}
            for cell in sheet_xml.iter(S + "c"):
                value = cell.find(S + "v")
                coordinate = cell.get("r")
                if cell.get("t") == "e":
                    report["errors"] += 1
                    if len(report["error_samples"]) < sample_limit:
                        report["error_samples"].append({"cell": coordinate,
                                                        "error": value.text if value is not None else None})
                if cell.find(S + "f") is not None:
                    report["formulas"] += 1
                    if value is None or (value.text is None and cell.get("t") != "str"):
                        report["missing_caches"] += 1
                        if len(report["missing_cache_samples"]) < sample_limit:
                            report["missing_cache_samples"].append(coordinate)
            sheets.append(report)
        formulas = sum(sheet["formulas"] for sheet in sheets)
        missing = sum(sheet["missing_caches"] for sheet in sheets)
        errors = sum(sheet["errors"] for sheet in sheets)
        return {"sheets": sheets, "total_formulas": formulas, "total_errors": errors,
                "missing_caches": missing, "preservation_risks": risks,
                "cache_status": ("missing" if missing else "present_unverified") if formulas else "not_applicable",
                "calculation_verified": False}
