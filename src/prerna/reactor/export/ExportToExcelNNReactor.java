/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataConsolidateFunction;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFPivotTable;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openqa.selenium.chrome.ChromeDriver;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.ChromeDriverUtility;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class ExportToExcelNNReactor extends TableToXLSXReactor {

	private static final String EXPORT_TEMPLATE_KEY = "EXCEL_EXPORT_TEMPLATE";

	public ExportToExcelNNReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.USE_PANEL.getKey(), ReactorKeysEnum.EXPORT_TEMPLATE.getKey(),
				ReactorKeysEnum.EXPORT_AUDIT.getKey() };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// throw error if user doesn't have rights to export data
		if (AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);

		String insightFolder = this.insight.getInsightFolder();
		if (keyValue.containsKey(ReactorKeysEnum.FILE_PATH.getKey())) {
			insightFolder = Utility.normalizePath(keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
			insightFile.setDeleteOnInsightClose(false);
		}

		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.insight.getLiveURL();
		boolean panel = false;

		if (keyValue.containsKey(ReactorKeysEnum.USE_PANEL.getKey())) {
			String panelUse = keyValue.get(ReactorKeysEnum.USE_PANEL.getKey());
			panel = panelUse.equalsIgnoreCase("yes") || panelUse.equalsIgnoreCase("true");
		}

		boolean exportAudit = false;
		if (keyValue.containsKey(ReactorKeysEnum.EXPORT_AUDIT.getKey())) {
			String auditParam = keyValue.get(ReactorKeysEnum.EXPORT_AUDIT.getKey());
			exportAudit = auditParam.equalsIgnoreCase("yes") || auditParam.equalsIgnoreCase("true");
		}

		// see if someone has pushed a template file into insight
		String template = insight.getProperty(EXPORT_TEMPLATE_KEY);

		Map<String, InsightSheet> allSheets = insight.getInsightSheets();
		Map<String, InsightPanel> allPanels = insight.getInsightPanels();

		// sort out the panels by sheet
		// only get the pivot panels
		Map<String, InsightPanel> pivotPanelsBySheet = new HashMap<String, InsightPanel>();
		Iterator<InsightPanel> allPanelsIterator = allPanels.values().iterator();
		while (allPanelsIterator.hasNext()) {
			InsightPanel thisPanel = allPanelsIterator.next();
			TaskOptions options = thisPanel.getLastTaskOptions();
			// options can be null - example is text widget
			if (options != null) {
				String chartLayout = options.getLayout(thisPanel.getPanelId());
				if (chartLayout.equalsIgnoreCase("PivotTable")) {
					pivotPanelsBySheet.put(thisPanel.getSheetId(), thisPanel);
				}
			}
		}

		Iterator<String> keys = allSheets.keySet().iterator();
		if (panel) {
			keys = allPanels.keySet().iterator();
		}

		XSSFWorkbook wb = null;
		Object driver = null;
		ChromeDriverUtility util = this.insight.getChromeDriver();
		try {
			if (template != null) {
				wb = new XSSFWorkbook(template);
			} else {
				wb = new XSSFWorkbook();
			}
			while (keys.hasNext()) {
				String thisKey = keys.next();
				String sheetAppender = "";
				String panelAppender = "";
				String sheetLabel = "";
				String sheetKey = "";

				if (panel) {
					InsightPanel thisPanel = allPanels.get(thisKey);
					panelAppender = "&panel=" + thisKey;

					InsightSheet thisSheet = allSheets.get(thisPanel.getSheetId());
					sheetAppender = "&sheet=" + thisSheet.getSheetId();
					sheetKey = thisSheet.getSheetId();
					sheetLabel = thisSheet.getSheetLabel();
					if (sheetLabel == null) {
						sheetLabel = "Sheet" + (Integer.parseInt(sheetKey) + 1);
					}
					sheetLabel = sheetLabel + " Panel - " + thisKey;
				} else {
					InsightSheet thisSheet = allSheets.get(thisKey);

					sheetAppender = "&sheet=" + thisKey;
					sheetKey = thisKey;
					sheetLabel = thisSheet.getSheetLabel();
					if (sheetLabel == null) {
						sheetLabel = "Sheet" + (Integer.parseInt(thisKey) + 1);
					}
				}

				Sheet sheet = null;
				if (template != null) {
					sheet = wb.cloneSheet(wb.getSheetIndex("Template"));
				} else {
					sheet = wb.createSheet(sheetLabel);
				}

				if (!pivotPanelsBySheet.containsKey(thisKey)) {
					// now capture the image and fill it
					String prefixName = Utility.getRandomString(8);
					String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "png");
					String fileLocation = insightFolder + DIR_SEPARATOR + exportName;

					if (driver == null) {
						driver = util.makeChromeDriver(baseUrl, imageUrl + sheetAppender + panelAppender, 800, 600);
					}
					// download this file
					util.captureImagePersistent(driver, baseUrl, imageUrl + sheetAppender + panelAppender, fileLocation,
							sessionId, 10_000);

					// add the captured image to the sheet
					// FileInputStream obtains input bytes from the image file
					InputStream inputStream = new FileInputStream(fileLocation);
					// Get the contents of an InputStream as a byte[].
					byte[] bytes = IOUtils.toByteArray(inputStream);
					// Adds a picture to the workbook
					int pictureIdx = wb.addPicture(bytes, Workbook.PICTURE_TYPE_PNG);
					// close the input stream
					inputStream.close();

					FileUtils.forceDelete(new File(fileLocation));

					// Returns an object that handles instantiating concrete classes
					CreationHelper helper = wb.getCreationHelper();
					// Creates the top-level drawing patriarch.
					Drawing drawing = sheet.createDrawingPatriarch();

					// Create an anchor that is attached to the worksheet
					ClientAnchor anchor = helper.createClientAnchor();

					// create an anchor with upper left cell _and_ bottom right cell
					anchor.setCol1(1); // Column B
					anchor.setRow1(2); // Row 3
					anchor.setCol2(2); // Column C (col2/row2 are ignored after resize())
					anchor.setRow2(4); // Row 4

					// Creates a picture
					Picture pict = drawing.createPicture(anchor, pictureIdx);
					pict.resize();

					// Create the Cell B3
					sheet.createRow(2).createCell(1);
				} else {
					// this is the gen pivot logic
					// need to write the data
					// and then generate the pivot from it

					// get the task options
					// make the frame
					// call the genXLPivot
					InsightPanel pivotPanel = pivotPanelsBySheet.get(thisKey);
					TaskOptions taskOptions = pivotPanel.getLastTaskOptions();
					ITask task = InsightUtility.constructTaskFromQs(this.insight, pivotPanel.getLastQs());
					task.setLogger(this.getLogger(this.getClass().getName()));
					task.setTaskOptions(taskOptions);

					Map<String, Object> columnMap = writeData(wb, (XSSFSheet) sheet, task,
							pivotPanel.getPanelFormatValues());

					// get other data now
					Map<String, Object> pivotMakerOptions = taskOptions.getAlignmentMap(pivotPanel.getPanelId());
					// get the rows
					List<String> rows = (List<String>) pivotMakerOptions.get("rows");
					List<String> columns = (List<String>) pivotMakerOptions.get("columns");
					// calculations is being kept directly in task options so going to pick from
					// there
					List<String> values = (List<String>) taskOptions.getOptions().get("values");

					List<String> newValues = new ArrayList<>();
					List<String> functions = new ArrayList<>();
					// now generate the pivot
					// need to parse values and functions separately
					for (int valIndex = 0; valIndex < values.size(); valIndex++) {
						String curValue = values.get(valIndex);

						// get the operator and selector
						String operator = curValue.substring(0, curValue.indexOf("(")).trim();
						String operand = curValue.substring(curValue.indexOf("(") + 1, curValue.length() - 1).trim();
						newValues.add(operand);
						functions.add(operator);
					}

					genXLPivot((XSSFSheet) sheet, rows, columns, newValues, functions, columnMap);
				}
			}

			// remove the template sheet when you finally save it
			// it is no longer needed
			if (template != null) {
				wb.removeSheetAt(wb.getSheetIndex(wb.getSheet("Template")));
			}

			// process and apply the audit param sheet if the export Audit has been opted
			// exportMap stores all the export related properties
			if (exportAudit) {
				makeParamSheet(wb, this.insight, false, exportMap);
			}
			String prefixName = Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
			String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "xlsx");
			String fileLocation = insightFolder + DIR_SEPARATOR + exportName;

			// write / encrypt file
			String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
			if (password != null) {
				ExcelUtility.encrypt(wb, fileLocation, password);
			} else {
				ExcelUtility.writeToFile(wb, fileLocation);
			}

			insightFile.setFilePath(fileLocation);
			this.insight.addExportFile(downloadKey, insightFile);

			NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
					PixelOperationType.FILE_DOWNLOAD);
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the excel file"));
			return retNoun;
		} catch (IOException e) {
			throw new IllegalArgumentException("An error occurred generating the excel file", e);
		} finally {
			if (driver != null && driver instanceof ChromeDriver) {
				((ChromeDriver) driver).quit();
			}
		}
	}

	private Map<String, Object> writeData(XSSFWorkbook workbook, XSSFSheet sheet, ITask task,
			Map<String, Map<String, String>> panelFormatting) {
		CreationHelper createHelper = workbook.getCreationHelper();
		// freeze the first row
		sheet.createFreezePane(0, 1);

		int i = 0;
		int size = 0;
		// create typesArr as an array for faster searching
		String[] headers = null;
		SemossDataType[] typesArr = null;
		String[] additionalDataTypeArr = null;
		CellStyle[] stylingArr = null;

		// style dates
		CellStyle dateCellStyle = workbook.createCellStyle();
		dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));
		// style timestamps
		CellStyle timeStampCellStyle = workbook.createCellStyle();
		timeStampCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy HH:mm:ss"));

		// the excel data row
		// endRow > 0 only when writing into a pre-populated template sheet
		Row excelRow = null;
		int excelColStart = 0;
		int curSheetCol = i + excelColStart;
		int endRow = 0;
		int excelRowCounter = 0;

		// we need to iterate and write the headers during the first time
		if (task.hasNext()) {
			IHeadersDataRow row = task.next();
			List<Map<String, Object>> headerInfo = task.getHeaderInfo();

			// create the header row
			Row headerRow = null;
			if (excelRowCounter < endRow) {
				headerRow = sheet.getRow(excelRowCounter++);
			} else {
				headerRow = sheet.createRow(excelRowCounter++);
			}
			// create a Font for styling header cells
			Font headerFont = workbook.createFont();
			headerFont.setBold(true);
			// create a CellStyle with the font
			CellStyle headerCellStyle = workbook.createCellStyle();
			headerCellStyle.setFont(headerFont);
			headerCellStyle.setAlignment(HorizontalAlignment.CENTER);
			headerCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);

			// generate the header row
			// and define constants used throughout like size, and types
			i = 0;
			headers = row.getHeaders();
			size = headers.length;
			typesArr = new SemossDataType[size];
			additionalDataTypeArr = new String[size];
			stylingArr = new CellStyle[size];
			for (; i < size; i++) {
				curSheetCol = i + excelColStart;
				Cell cell = headerRow.createCell(curSheetCol);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
				// grab metadata from iterator
				typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
				additionalDataTypeArr[i] = headerInfo.get(i).get("additionalDataType") + "";
				try {
					stylingArr[i] = POIExportUtility.getCurrentStyle(workbook, additionalDataTypeArr[i],
							panelFormatting.get(headers[i]));
				} catch (Exception e) {
					// ignore
				}
				if (stylingArr[i] == null) {
					if (typesArr[i] == SemossDataType.DATE) {
						stylingArr[i] = dateCellStyle;
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						stylingArr[i] = timeStampCellStyle;
					}
				}
			}

			// generate the data row
			if (excelRowCounter < endRow) {
				excelRow = sheet.getRow(excelRowCounter++);
			} else {
				excelRow = sheet.createRow(excelRowCounter++);

			}
			Object[] dataRow = row.getValues();
			i = 0;
			for (; i < size; i++) {
				curSheetCol = i + excelColStart;
				Cell cell = excelRow.createCell(curSheetCol);
				Object value = dataRow[i];
				if (value == null || value.toString().length() == 0) {
					cell.setCellValue("");
				} else {
					if (typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if (typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue(((Number) value).doubleValue());
					} else if (typesArr[i] == SemossDataType.DATE) {
						if (value instanceof SemossDate) {
							cell.setCellValue(((SemossDate) value).getDate());
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						if (value instanceof SemossDate) {
							cell.setCellValue(((SemossDate) value).getDate());
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue((boolean) value);
					} else {
						cell.setCellValue(value + "");
					}

					if (stylingArr[i] != null) {
						cell.setCellStyle(stylingArr[i]);
					}
				}
			}
		}

		// now iterate through all the data
		while (task.hasNext()) {
			if (excelRowCounter < endRow) {
				excelRow = sheet.getRow(excelRowCounter++);
			} else {
				excelRow = sheet.createRow(excelRowCounter++);
			}
			IHeadersDataRow row = task.next();
			Object[] dataRow = row.getValues();
			i = 0;
			for (; i < size; i++) {
				curSheetCol = i + excelColStart;
				Cell cell = excelRow.createCell(curSheetCol);
				Object value = dataRow[i];
				if (value == null || value.toString().length() == 0) {
					cell.setCellValue("");
				} else {
					if (typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if (typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue(((Number) value).doubleValue());
					} else if (typesArr[i] == SemossDataType.DATE) {
						if (value instanceof SemossDate) {
							cell.setCellValue(((SemossDate) value).getDate());
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						if (value instanceof SemossDate) {
							cell.setCellValue(((SemossDate) value).getDate());
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue((boolean) value);
					} else {
						cell.setCellValue(value + "");
					}

					if (stylingArr[i] != null) {
						cell.setCellStyle(stylingArr[i]);
					}
				}
			}
		}

		Map<String, Object> columnMap = new HashMap<>();
		// Update col and row bounds for sheet
		int endCol = curSheetCol;
		columnMap.put("colIndex", endCol + 1);
		if (excelRowCounter > endRow) {
			columnMap.put("rowIndex", excelRowCounter);
		}

		if (headers != null && headers.length > 0) {
			columnMap.put("startRow", 0);
			columnMap.put("endRow", excelRowCounter - 1);
			// find header index in list
			columnMap.put("startCol", 0);
			columnMap.put("endCol", excelColStart + (headers.length - 1));
		}

		return columnMap;
	}

	// now generate the excel pivot
	public void genXLPivot(XSSFSheet srcSheet, List<String> rows, List<String> columns, List<String> values,
			List<String> functions, Map<String, Object> columnMap) {
		// values and functions must be paired 1:1
		if (values.size() != functions.size()) {
			return;
		}

		// find the upper limits and lower limits
		int firstRow = (Integer) columnMap.get("startRow");
		int firstCol = (Integer) columnMap.get("startCol");
		int lastRow = (Integer) columnMap.get("endRow");
		int lastCol = (Integer) columnMap.get("endCol");

		// add an outline to the source sheet
		// and collapse it
		srcSheet.groupColumn(firstCol, lastCol);
		srcSheet.setColumnGroupCollapsed(firstCol, true);

		// impute the headers
		// build ordered header list so labels can be resolved to column indexes
		Row headerRow = srcSheet.getRow(firstRow);
		List<String> xlHeaders = new ArrayList<>();

		for (int cellIndex = firstCol; cellIndex <= lastCol; cellIndex++) {
			xlHeaders.add(headerRow.getCell(cellIndex).toString());
		}

		// compose the area to use
		AreaReference pivSource = new AreaReference(new CellReference(firstRow, firstCol),
				new CellReference(lastRow, lastCol), SpreadsheetVersion.EXCEL2007);

		XSSFPivotTable pivotTable = srcSheet.createPivotTable(pivSource,
				new CellReference((firstRow + 2), (lastCol + 2)));
		pivotTable.getCTPivotTableDefinition().getPivotTableStyleInfo().setShowColHeaders(true);
		pivotTable.getCTPivotTableDefinition().getPivotTableStyleInfo().setShowRowHeaders(true);

		// and now we start adding the rows and columns
		for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
			String rowHeader = rows.get(rowIndex);
			int xlHeaderIndex = xlHeaders.indexOf(rowHeader);

			// set the first header
			if (rowIndex == 0) {
				pivotTable.getCTPivotTableDefinition().setRowHeaderCaption(rowHeader);
			}

			pivotTable.addRowLabel(xlHeaderIndex);
			pivotTable.getCTPivotTableDefinition().getPivotFields().getPivotFieldArray(xlHeaderIndex).setOutline(false);
		}

		for (int valIndex = 0; valIndex < values.size(); valIndex++) {
			String value = values.get(valIndex);
			String function = functions.get(valIndex);

			DataConsolidateFunction xlFun = POIExportUtility.convertToExcelFunction(function);
			int xlHeaderIndex = xlHeaders.indexOf(value);

			pivotTable.addColumnLabel(xlFun, xlHeaderIndex);
		}

		// adding columns
		for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
			String column = columns.get(colIndex);
			int xlHeaderIndex = xlHeaders.indexOf(column);
			pivotTable.getCTPivotTableDefinition().getPivotFields().getPivotFieldArray(xlHeaderIndex)
					.setAxis(org.openxmlformats.schemas.spreadsheetml.x2006.main.STAxis.AXIS_COL);
			pivotTable.getCTPivotTableDefinition().getPivotFields().getPivotFieldArray(xlHeaderIndex).addNewItems();
			pivotTable.getCTPivotTableDefinition().getPivotFields().getPivotFieldArray(xlHeaderIndex).getItems()
					.addNewItem().setT(org.openxmlformats.schemas.spreadsheetml.x2006.main.STItemType.DEFAULT);
			pivotTable.getCTPivotTableDefinition().addNewColFields().addNewField().setX(xlHeaderIndex);
		}
	}

}
