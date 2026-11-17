"""Adapt this example's data and filename, then save it as a room generator."""
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.workbook.properties import CalcProperties
from openpyxl.worksheet.table import Table, TableStyleInfo
from smssutil import smss_get_runtime_var


def build():
    root_value = smss_get_runtime_var("ROOT")
    if not root_value:
        raise RuntimeError("ROOT is unavailable for this execution")
    output = Path(root_value) / "Cost plan.xlsx"
    if output.exists():
        raise FileExistsError(f"Inspect the existing workbook before replacing it: {output.name}")
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Cost plan"
    sheet.append(["Item", "Quantity", "Unit cost ($)", "Total ($)"])
    # Demonstration data only; replace with actual supplied values for a real task.
    for row, values in enumerate((("Workshop", 2, 350), ("Materials", 12, 25)), start=2):
        sheet.append([*values, f"=B{row}*C{row}"])
        for column in (2, 3):
            sheet.cell(row, column).font = Font(name="Arial", color="0000FF", size=11)
    sheet["A4"], sheet["D4"] = "Total", "=SUM(D2:D3)"
    table = Table(displayName="CostItems", ref="A1:D3")
    table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
    sheet.add_table(table)
    for row in sheet.iter_rows(min_row=1, max_row=4, max_col=4):
        for cell in row:
            if cell.row == 1:
                cell.font = Font(name="Arial", bold=True, color="FFFFFF", size=11)
                cell.fill = PatternFill("solid", fgColor="16324F")
            elif not (cell.row in (2, 3) and cell.column in (2, 3)):
                cell.font = Font(name="Arial", size=11, bold=cell.row == 4)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if cell.row > 1 and cell.column in (3, 4):
                cell.number_format = '"$"#,##0.00;[Red]("$"#,##0.00)'
    for column, width in {"A": 26, "B": 14, "C": 20, "D": 20}.items():
        sheet.column_dimensions[column].width = width
    sheet.freeze_panes = "B2"
    chart = BarChart()
    chart.title, chart.y_axis.title = "Cost by item", "Dollars"
    chart.add_data(Reference(sheet, min_col=4, min_row=1, max_row=3), titles_from_data=True)
    chart.set_categories(Reference(sheet, min_col=1, min_row=2, max_row=3))
    sheet.add_chart(chart, "F2")
    notes = workbook.create_sheet("Notes")
    notes.append(["Source", "Illustrative example values; replace with the user's source."])
    notes.append(["Inputs", "Blue text: edit quantity and unit cost. Totals contain formulas."])
    notes.column_dimensions["A"].width = 16
    notes.column_dimensions["B"].width = 85
    workbook.calculation = CalcProperties(calcMode="auto", fullCalcOnLoad=True, forceFullCalc=True)
    workbook.save(output)
    reopened = load_workbook(output, data_only=False)
    try:
        if reopened.sheetnames != ["Cost plan", "Notes"] or reopened["Cost plan"]["D4"].value != "=SUM(D2:D3)":
            raise ValueError("Saved workbook content did not match the generator")
    finally:
        reopened.close()
    return {"filePath": output.name, "bytes": output.stat().st_size,
            "recalculated": False, "expected_total_from_inputs": 2 * 350 + 12 * 25}


result = build()
print(result)
