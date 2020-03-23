package prerna.sablecc2.reactor.export;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.PresetColor;
import org.apache.poi.xddf.usermodel.XDDFColor;
import org.apache.poi.xddf.usermodel.XDDFLineProperties;
import org.apache.poi.xddf.usermodel.XDDFShapeProperties;
import org.apache.poi.xddf.usermodel.XDDFSolidFillProperties;
import org.apache.poi.xddf.usermodel.chart.AxisCrosses;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.BarDirection;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.MarkerStyle;
import org.apache.poi.xddf.usermodel.chart.XDDFAreaChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFBarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFLineChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFPieChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFRadarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFScatterChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFValueAxis;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFDrawing;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.AbstractReactor;

public class ExportToExcelReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExportToExcelReactor.class.getName();

	protected String fileLocation = null;
	protected Logger logger;

	private Map<String, Map<String, Object>> chartPanelLayout = new HashMap<>();

	public ExportToExcelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.logger = getLogger(CLASS_NAME);
		NounMetadata retNoun = null;
		// get a random file name
		String exportName = AbstractExportTxtReactor.getExportFileName("xlsx");
		// grab file path to write the file
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(exportName, fileLocation);
			retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} else {
			retNoun = new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
		}

		Map<String, InsightPanel> panelMap = this.insight.getInsightPanels();
		Map<String, InsightSheet> sheetMap = this.insight.getInsightSheets();

		// Use in-memory XSSF workbook to be able to plot charts
		XSSFWorkbook workbook = new XSSFWorkbook();
		// sheet alias
		Map<String, String> sheetAlias = new HashMap<>();
		// create each sheet
		for (String sheetId : sheetMap.keySet()) {
			InsightSheet sheet = sheetMap.get(sheetId);
			String sheetName = sheet.getSheetLabel();
			sheetAlias.put(sheetId, sheetName);
			HashMap<String, Object> sheetChartMap = new HashMap<String, Object>();
			sheetChartMap.put("colIndex", 0);
			sheetChartMap.put("rowIndex", 0);
			sheetChartMap.put("chartIndex", 1);
			this.chartPanelLayout.put(sheetId, sheetChartMap);
		}

		// iterate through panel map to figure out layout
		for (String panelId : panelMap.keySet()) {
			InsightPanel panel = panelMap.get(panelId);
			String sheetId = panel.getSheetId();
			// for each panel get the task and task options
			SelectQueryStruct qs = panel.getLastQS();
			TaskOptions taskOptions = panel.getOptions();
			IQuerySelector firstSelector = qs.getSelectors().get(0);
			if (firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
				qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
			} else {
				qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
			}
			ITask task = new BasicIteratorTask(qs);
			task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
			task.setTaskOptions(taskOptions);
			Map<String, Object> panelChartMap = new HashMap<>();
			setChartLayout(panelChartMap, taskOptions, panelId);
			this.chartPanelLayout.get(sheetId).put(panelId, panelChartMap);
			writeData(workbook, task, sheetId, panelId);
		}

		// now build charts
		for (String panelId : panelMap.keySet()) {
			InsightPanel panel = panelMap.get(panelId);
			String sheetId = panel.getSheetId();
			// for each panel get the task and task options
			SelectQueryStruct qs = panel.getLastQS();
			TaskOptions taskOptions = panel.getOptions();
			ITask task = new BasicIteratorTask(qs);
			task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
			task.setTaskOptions(taskOptions);
			// add chart
			processTask(workbook, task, sheetId, panelId);
		}

		// rename sheets
		for (String sheetId : sheetAlias.keySet()) {
			String sheetName = sheetAlias.get(sheetId);
			if (sheetName == null) {
				sheetName = "Sheet " + sheetId;
			}
			workbook.setSheetName(workbook.getSheetIndex(sheetId), sheetName);
		}

		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if(password != null) {
			// encrypt file
			ExcelUtility.encrypt(workbook, fileLocation, password);
		} else {
			// write file
			ExcelUtility.writeToFile(workbook, fileLocation);
		}

		return retNoun;
	}

	private void setChartLayout(Map<String, Object> panelChartMap, TaskOptions taskOptions, String panelId) {
		String chartLayout = taskOptions.getLayout(panelId);
		panelChartMap.put("chartType", chartLayout);
		Map<String, Object> alignmentMap = taskOptions.getAlignmentMap(panelId);
		if (chartLayout.equals("Line") || chartLayout.equals("Area") || chartLayout.equals("Column")
				|| chartLayout.equals("Pie") || chartLayout.equals("Radar")) {
			List<String> label = (Vector) alignmentMap.get("label");
			panelChartMap.put("x-axis", label);
			List<String> yColumnNames = (Vector) alignmentMap.get("value");
			panelChartMap.put("y-axis", yColumnNames);
			for (String column : label) {
				panelChartMap.put(column, new HashMap<>());
			}
			for (String column : yColumnNames) {
				panelChartMap.put(column, new HashMap<>());
			}
		} else if (chartLayout.equals("Scatter")) {
			List<String> label = (Vector) alignmentMap.get("label");
			panelChartMap.put("label", label);
			List<String> x = (Vector) alignmentMap.get("x");
			panelChartMap.put("x-axis", x);
			List<String> yColumnNames = (Vector) alignmentMap.get("y");
			panelChartMap.put("y-axis", yColumnNames);
			for (String column : label) {
				panelChartMap.put(column, new HashMap<>());
			}
		}
	}

	private void processTask(XSSFWorkbook workbook, ITask task, String sheetId, String panelId) {
		TaskOptions tOptions = task.getTaskOptions();
		Map<String, Object> options = tOptions.getOptions();
		XSSFSheet sheet = workbook.getSheet(sheetId);

		// Insert chart if supported
		String plotType = (String) tOptions.getLayout(panelId);
		if (plotType.equals("Line")) {
			insertLineChart(sheet, options, sheetId, panelId);
		} else if (plotType.equals("Scatter")) {
			insertScatterChart(sheet, options, sheetId, panelId);
		} else if (plotType.equals("Area")) {
			insertAreaChart(sheet, options, sheetId, panelId);
		} else if (plotType.equals("Column")) {
			insertBarChart(sheet, options, sheetId, panelId);
		} else if (plotType.equals("Pie")) {
			insertPieChart(sheet, options, sheetId, panelId);
		} else if (plotType.equals("Radar")) {
			insertRadarChart(sheet, options, sheetId, panelId);
		}
	}

	private void writeData(XSSFWorkbook workbook, ITask task, String sheetId, String panelId) {
		CreationHelper createHelper = workbook.getCreationHelper();
		XSSFSheet sheet = workbook.getSheet(sheetId);
		if (sheet == null) {
			sheet = workbook.createSheet(sheetId);
		}
		// freeze the first row
		sheet.createFreezePane(0, 1);

		int i = 0;
		int size = 0;
		// create typesArr as an array for faster searching
		String[] headers = null;
		SemossDataType[] typesArr = null;

		// style dates
		CellStyle dateCellStyle = workbook.createCellStyle();
		dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy"));
		// style timestamps
		CellStyle timeStampCellStyle = workbook.createCellStyle();
		timeStampCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("dd-MM-yyyy HH:mm:ss"));

		// the excel data row
		Row excelRow = null;
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		int excelColStart = (int) sheetMap.get("colIndex");
		int curSheetCol = i + excelColStart;
		int endRow = (int) sheetMap.get("rowIndex");
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
			for (; i < size; i++) {
				curSheetCol = i + excelColStart;
				Cell cell = headerRow.createCell(curSheetCol);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
				typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
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
				if (value == null) {
					cell.setCellValue("");
				} else {
					if (typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if (typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue(((Number) value).doubleValue());
					} else if (typesArr[i] == SemossDataType.DATE) {
						cell.setCellValue(((SemossDate) value).getDate());
						cell.setCellStyle(dateCellStyle);
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						cell.setCellValue(((SemossDate) value).getDate());
						cell.setCellStyle(timeStampCellStyle);
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue((boolean) value);
					} else {
						cell.setCellValue(value + "");
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
				if (value == null) {
					cell.setCellValue("");
				} else {
					if (typesArr[i] == SemossDataType.STRING) {
						cell.setCellValue(value + "");
					} else if (typesArr[i] == SemossDataType.INT || typesArr[i] == SemossDataType.DOUBLE) {
						cell.setCellValue(((Number) value).doubleValue());
					} else if (typesArr[i] == SemossDataType.DATE) {
						cell.setCellValue(((SemossDate) value).getDate());
						cell.setCellStyle(dateCellStyle);
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						cell.setCellValue(((SemossDate) value).getDate());
						cell.setCellStyle(timeStampCellStyle);
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue((boolean) value);
					} else {
						cell.setCellValue(value + "");
					}
				}
			}
		}

		// Update col and row bounds for sheet
		int endCol = curSheetCol;
		sheetMap.put("colIndex", endCol + 1);
		if (excelRowCounter > endRow) {
			sheetMap.put("rowIndex", excelRowCounter);
		}

		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> headerList = Arrays.asList(headers);

		for (String header : headers) {
			Map<String, Object> columnMap = new HashMap<>();
			columnMap.put("startRow", 1);
			columnMap.put("endRow", excelRowCounter - 1);
			// find header index in list
			int headerIndex = headerList.indexOf(header);
			columnMap.put("startCol", excelColStart + headerIndex);
			columnMap.put("endCol", excelColStart + headerIndex);
			panelMap.put(header, columnMap);
		}

	}

	private void insertLineChart(XSSFSheet sheet, Map<String, Object> options, String sheetId, String panelId) {
		LegendPosition legendPosition = LegendPosition.TOP_RIGHT;
		AxisPosition bottomAxisPosition = AxisPosition.BOTTOM;
		AxisPosition leftAxisPosition = AxisPosition.LEFT;
		AxisCrosses leftAxisCrosses = AxisCrosses.AUTO_ZERO;
		ChartTypes chartType = ChartTypes.LINE;

		// Parse input data
		// label is name of column of x vals
		// value is name(s) of column(s) of y vals
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> label = (Vector) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		leftAxis.setCrosses(leftAxisCrosses);
		XDDFLineChartData data = (XDDFLineChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(sheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(sheet, yColumnMap);
			XDDFLineChartData.Series chartSeries = (XDDFLineChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
			// Standardize markers
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(PresetColor.ROYAL_BLUE));
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			XDDFShapeProperties propertiesMarker = new XDDFShapeProperties();
			propertiesMarker.setFillProperties(fillProperties);
			chart.getCTChart().getPlotArea().getLineChartArray(0).getSerArray(0).getMarker().addNewSpPr()
					.set(propertiesMarker.getXmlObject());
			// Standardize line
			XDDFLineProperties lineProperties = new XDDFLineProperties();
			lineProperties.setFillProperties(fillProperties);
			chartSeries.setLineProperties(lineProperties);
		}

		chart.plot(data);
	}

	private void insertScatterChart(XSSFSheet sheet, Map<String, Object> options, String sheetId, String panelId) {
		LegendPosition legendPosition = LegendPosition.TOP_RIGHT;
		AxisPosition bottomAxisPosition = AxisPosition.BOTTOM;
		AxisPosition leftAxisPosition = AxisPosition.LEFT;
		AxisCrosses leftAxisCrosses = AxisCrosses.AUTO_ZERO;
		ChartTypes chartType = ChartTypes.SCATTER;

		// Parse input data
		// label is name of column of x vals
		// value is name(s) of column(s) of y vals
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> label = (Vector) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		leftAxis.setCrosses(leftAxisCrosses);
		XDDFScatterChartData data = (XDDFScatterChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(sheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(sheet, yColumnMap);
			XDDFScatterChartData.Series chartSeries = (XDDFScatterChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
			chartSeries.setSmooth(false);
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			chart.getCTChart().getPlotArea().getScatterChartArray(0).getSerArray(0).addNewSpPr().addNewLn()
					.addNewNoFill();
		}

		chart.plot(data);
	}

	private void insertBarChart(XSSFSheet sheet, Map<String, Object> options, String sheetId, String panelId) {
		LegendPosition legendPosition = LegendPosition.TOP_RIGHT;
		AxisPosition bottomAxisPosition = AxisPosition.BOTTOM;
		AxisPosition leftAxisPosition = AxisPosition.LEFT;
		AxisCrosses leftAxisCrosses = AxisCrosses.AUTO_ZERO;
		ChartTypes chartType = ChartTypes.BAR;

		// Parse input data
		// label is name of column of x vals
		// value is name(s) of column(s) of y vals
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> label = (Vector) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		leftAxis.setCrosses(leftAxisCrosses);
		XDDFBarChartData data = (XDDFBarChartData) chart.createData(chartType, bottomAxis, leftAxis);
		data.setBarDirection(BarDirection.COL);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(sheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(sheet, yColumnMap);
			XDDFBarChartData.Series chartSeries = (XDDFBarChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
		}

		chart.plot(data);
	}

	private void insertAreaChart(XSSFSheet sheet, Map<String, Object> options, String sheetId, String panelId) {
		LegendPosition legendPosition = LegendPosition.TOP_RIGHT;
		AxisPosition bottomAxisPosition = AxisPosition.BOTTOM;
		AxisPosition leftAxisPosition = AxisPosition.LEFT;
		AxisCrosses leftAxisCrosses = AxisCrosses.AUTO_ZERO;
		ChartTypes chartType = ChartTypes.AREA;

		// Parse input data
		// label is name of column of x vals
		// value is name(s) of column(s) of y vals
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> label = (Vector) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		leftAxis.setCrosses(leftAxisCrosses);
		XDDFAreaChartData data = (XDDFAreaChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(sheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(sheet, yColumnMap);
			XDDFAreaChartData.Series chartSeries = (XDDFAreaChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
		}

		chart.plot(data);
	}

	private void insertPieChart(XSSFSheet sheet, Map<String, Object> options, String sheetId, String panelId) {
		LegendPosition legendPosition = LegendPosition.TOP_RIGHT;
		AxisPosition bottomAxisPosition = AxisPosition.BOTTOM;
		AxisPosition leftAxisPosition = AxisPosition.LEFT;
		AxisCrosses leftAxisCrosses = AxisCrosses.AUTO_ZERO;
		ChartTypes chartType = ChartTypes.PIE;

		// Parse input data
		// label is name of column of x vals
		// value is name(s) of column(s) of y vals
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> label = (Vector) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFPieChartData data = (XDDFPieChartData) chart.createData(chartType, null, null);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(sheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(sheet, yColumnMap);
			XDDFPieChartData.Series chartSeries = (XDDFPieChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
			chartSeries.setExplosion((long) 0);
		}

		chart.plot(data);
	}

	private void insertRadarChart(XSSFSheet sheet, Map<String, Object> options, String sheetId, String panelId) {
		LegendPosition legendPosition = LegendPosition.TOP_RIGHT;
		AxisPosition bottomAxisPosition = AxisPosition.BOTTOM;
		AxisPosition leftAxisPosition = AxisPosition.LEFT;
		AxisCrosses leftAxisCrosses = AxisCrosses.AUTO_ZERO;
		ChartTypes chartType = ChartTypes.RADAR;

		// Parse input data
		// label is name of column of x vals
		// value is name(s) of column(s) of y vals
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> label = (Vector) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		leftAxis.setCrosses(leftAxisCrosses);
		XDDFRadarChartData data = (XDDFRadarChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(sheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(sheet, yColumnMap);
			XDDFRadarChartData.Series chartSeries = (XDDFRadarChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
		}

		chart.plot(data);
	}

	private XSSFChart createBaseChart(XSSFSheet sheet, Map<String, Object> sheetMap, LegendPosition legendPosition) {
		XSSFDrawing drawing = sheet.createDrawingPatriarch();
		// Put chart to the right of any data columns
		int colIndex = (int) sheetMap.get("colIndex");
		int chartIndex = (int) sheetMap.get("chartIndex");
		XSSFClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, colIndex, chartIndex, colIndex + 10,
				chartIndex + 10);
		// Increment for positioning other objects correctly
		sheetMap.put("chartIndex", chartIndex + 10);
		XSSFChart chart = drawing.createChart(anchor);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(legendPosition);

		return chart;
	}

	private XDDFNumericalDataSource<Double> createXAxis(XSSFSheet sheet, Map<String, Object> xColumnMap) {
		int xStartCol = (int) xColumnMap.get("startCol");
		int xEndCol = (int) xColumnMap.get("endCol");
		int xStartRow = (int) xColumnMap.get("startRow");
		int xEndRow = (int) xColumnMap.get("endRow");
		CellRangeAddress xsCellRange = new CellRangeAddress(xStartRow, xEndRow, xStartCol, xEndCol);
		XDDFNumericalDataSource<Double> xs = XDDFDataSourcesFactory.fromNumericCellRange(sheet, xsCellRange);
		return xs;
	}

	private XDDFNumericalDataSource<Double> createYAxis(XSSFSheet sheet, Map<String, Object> yColumnMap) {
		int yStartCol = (int) yColumnMap.get("startCol");
		int yEndCol = (int) yColumnMap.get("endCol");
		int yStartRow = (int) yColumnMap.get("startRow");
		int yEndRow = (int) yColumnMap.get("endRow");
		CellRangeAddress ysCellRange = new CellRangeAddress(yStartRow, yEndRow, yStartCol, yEndCol);
		XDDFNumericalDataSource<Double> ys = XDDFDataSourcesFactory.fromNumericCellRange(sheet, ysCellRange);
		return ys;
	}
}
