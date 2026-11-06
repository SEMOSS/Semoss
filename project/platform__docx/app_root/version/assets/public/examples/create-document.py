"""Adapt this example's content and filename, then save it as a room generator."""
from pathlib import Path

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from smssutil import smss_get_runtime_var


def build():
    root_value = smss_get_runtime_var("ROOT")
    if not root_value:
        raise RuntimeError("ROOT is unavailable for this execution")
    output = Path(root_value) / "Project brief.docx"
    # A generator must not erase later edits made in the browser or Word.
    if output.exists():
        raise FileExistsError(f"Inspect the existing document before replacing it: {output.name}")
    document = Document()
    section = document.sections[0]
    section.page_width, section.page_height = Inches(8.5), Inches(11)
    section.top_margin = section.bottom_margin = Inches(0.8)
    section.left_margin = section.right_margin = Inches(0.85)
    normal = document.styles["Normal"]
    normal.font.name, normal.font.size = "Arial", Pt(11)
    normal.paragraph_format.space_after = Pt(7)
    for name in ("Title", "Heading 1", "Heading 2"):
        document.styles[name].font.name = "Arial"
        document.styles[name].font.color.rgb = RGBColor.from_string("16324F")
    document.add_heading("Project brief", 0)
    document.add_paragraph("Example content — replace with the user's actual brief.", style="Subtitle")
    document.add_heading("Purpose", 1)
    document.add_paragraph("Describe the requested outcome, its audience, and the decision this document supports.")
    document.add_heading("Scope", 1)
    document.add_paragraph("List the agreed deliverables and their acceptance criteria.", style="List Bullet")
    document.add_paragraph("Record unresolved assumptions without presenting them as facts.", style="List Bullet")
    document.add_heading("Responsibilities", 1)
    table = document.add_table(rows=1, cols=2)
    table.style = "Light Shading Accent 1"
    table.autofit = False
    widths = (Inches(2.1), Inches(4.7))
    for column, width in zip(table.columns, widths):
        column.width = width
    for cell, text in zip(table.rows[0].cells, ("Role", "Responsibility")):
        cell.text = text
    for values in (("Project owner", "Confirm scope and acceptance criteria."),
                   ("Delivery lead", "Maintain the agreed plan and report changes.")):
        for cell, text in zip(table.add_row().cells, values):
            cell.text = text
    for row in table.rows:
        for cell, width in zip(row.cells, widths):
            cell.width = width
    section.footer.paragraphs[0].text = "Project brief"
    document.save(output)
    reopened = Document(output)
    if reopened.paragraphs[0].text != "Project brief" or len(reopened.tables[0].rows) != 3:
        raise ValueError("Saved document content did not match the generator")
    return {"filePath": output.name, "bytes": output.stat().st_size, "rendered_review": False}


result = build()
print(result)
