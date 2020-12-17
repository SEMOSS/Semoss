package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.XDDFColor;
import org.apache.poi.xddf.usermodel.XDDFLineProperties;
import org.apache.poi.xddf.usermodel.XDDFShapeProperties;
import org.apache.poi.xddf.usermodel.XDDFSolidFillProperties;
import org.apache.poi.xddf.usermodel.chart.AxisCrossBetween;
import org.apache.poi.xddf.usermodel.chart.AxisCrosses;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.BarDirection;
import org.apache.poi.xddf.usermodel.chart.BarGrouping;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.MarkerStyle;
import org.apache.poi.xddf.usermodel.chart.XDDFAreaChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFBarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
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
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLbls;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTScatterChart;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.ChromeDriverUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class ExportToExcelReactor extends TableToXLSXReactor {

	private static final String CLASS_NAME = ExportToExcelReactor.class.getName();
	private static final String GRID_ON_X = "tools.shared.editGrid.x";
	private static final String GRID_ON_Y = "tools.shared.editGrid.y";
	private static final String DISPLAY_VALUES = "tools.shared.displayValues";
	private static final String SHOW_Y_AXIS_TITLE = "tools.shared.editYAxis.title.show";
	private static final String SHOW_X_AXIS_TITLE = "tools.shared.editXAxis.title.show";
	private static final String Y_AXIS_TITLE_NAME = "tools.shared.editYAxis.title.name";
	private static final String X_AXIS_TITLE_NAME = "tools.shared.editXAxis.title.name";
	private static final String COLOR_NAME = "tools.shared.colorName";
	private static final String CUSTOM_COLOR_ARRAY = "tools.shared.customColors";
	private static final String COLOR_ARRAY = "tools.shared.color";
	

	protected String fileLocation = null;
	Map <String, List<String>> orderOfPanelsMap = null;
	
	protected Logger logger;

	ChromeDriver driver = null;
	private Map<String, Map<String, Object>> chartPanelLayout = new HashMap<>();
	int height = 10;
	int width = 10;
	
	// sheet alias
	Map<String, String> sheetAlias = new HashMap<>();

	public ExportToExcelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.HEIGHT.getKey(), 
				ReactorKeysEnum.WIDTH.getKey(), 
				ReactorKeysEnum.HEADERS.getKey(), 
				ReactorKeysEnum.ROW_GUTTER.getKey(),
				ReactorKeysEnum.COLUMN_GUTTER.getKey(),
				ReactorKeysEnum.TABLE_HEADER.getKey(),
				ReactorKeysEnum.TABLE_FOOTER.getKey(),
				ReactorKeysEnum.MERGE_CELLS.getKey(), ReactorKeysEnum.EXPORT_TEMPLATE.getKey(),
				ReactorKeysEnum.PANEL_ORDER_IDS.getKey()
			};
		this.keyRequired = new int[] {0,0,0,0,0,0,0,0,0,0,0,0,0};
		this.keyMulti = new int[] {0,0,0,0,0,0,1,0,0,0,0,0,0};
	}
	
	public void processPayload() {
		super.processPayload();
		if(keyValue.containsKey(ReactorKeysEnum.HEIGHT.getKey())) {
			this.height = Integer.parseInt(keyValue.get(ReactorKeysEnum.HEIGHT.getKey())+"");
		} else if(exportMap.containsKey(ReactorKeysEnum.HEIGHT.getKey())) {
			this.height = Integer.parseInt(keyValue.get(ReactorKeysEnum.HEIGHT.getKey())+"");
		}
		
		if(keyValue.containsKey(ReactorKeysEnum.WIDTH.getKey())) {
			this.width = Integer.parseInt(keyValue.get(ReactorKeysEnum.WIDTH.getKey())+"");
		} else if(exportMap.containsKey(ReactorKeysEnum.WIDTH.getKey())) {
			this.width = Integer.parseInt(keyValue.get(ReactorKeysEnum.WIDTH.getKey())+"");
		}
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get the map also
		getMap(insight.getInsightId());
		processPayload();
		this.logger = getLogger(CLASS_NAME);
		NounMetadata retNoun = null;

		// get a random file name
		String prefixName = exportMap.get("FILE_NAME") + ""; 
		
		// this whole logic will be moved eventually
		//////>>>>>>>>>>>>>>>
		String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "xlsx");
		// grab file path to write the file
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			exportMap.put("FILE_LOCATION", fileLocation);
			// store it in the insight so the FE can download it
			// only from the given insight
			this.insight.addExportFile(exportName, fileLocation);
			retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		} else {
			retNoun = new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
		}
		/// <<<<<<<
		// Grab number of rows to export to know how many rows to iterate
		// through
		String limit = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		int numRowsToExport = -1;
		// If limit not set, export all rows
		if (limit != null) {
			try {
				numRowsToExport = Integer.parseInt(limit);
			} catch (NumberFormatException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}

		Map<String, InsightPanel> panelMap = this.insight.getInsightPanels();
		Map<String, InsightSheet> sheetMap = this.insight.getInsightSheets();
		
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PANEL_ORDER_IDS.getKey());
		
		/** If we need to send the map
		//((HashMap)this.store.getNoun("panelOrderIds").get(0)).get("a")
		// ((List)((HashMap)this.store.getNoun("panelOrderIds").get(0)).get("a")).get(0)
		
		// ExportToExcel ( panelOrderIds = [ { "a" : [ '1' , '2' ] } ] ) ;
		//this.store.getNoun("panelOrderIds").get(2)
		//orderOfPanelsMap = (HashMap)this.store.getNoun("panelOrderIds").get(0);
		*/
		List <String> orderOfPanels = null;

		//setting the order of panels for export
		//  ExportToExcel(panelOrderIds = ["0", "1", "2"])
		if(grs != null)
		{
			
			orderOfPanels = new ArrayList<String>(); //(Arrays.asList(((String)keyValue.get(ReactorKeysEnum.PANEL_ORDER_IDS.getKey())).split(",")));
			for(int idx = 0;idx < grs.size();idx++)
				orderOfPanels.add(grs.get(idx) + "");
		}
		else
		{
			orderOfPanels = new ArrayList(Arrays.asList(panelMap.keySet().toArray()));
		}

		// Use in-memory XSSF workbook to be able to plot charts

		// this should take care of templates too
		XSSFWorkbook workbook = getWorkBook();
		// create each sheet
		// this is purely for positioning where we put the panel
		for (String sheetId : sheetMap.keySet()) {
			InsightSheet sheet = sheetMap.get(sheetId);
			if (sheet == null) {
				continue;
			}
			String sheetName = sheet.getSheetLabel();
			if (sheetName == null) {
				// since we are 0 based, add 1
				try {
					sheetName = "Sheet" + (Integer.parseInt(sheetId) + 1);
				} catch (Exception ignore) {
					sheetName = "Sheet " + sheetId;
				}
			}
			// this is where the alias is being kept
			sheetAlias.put(sheetId, sheetName);
			HashMap<String, Object> sheetChartMap = new HashMap<>();
			sheetChartMap.put("colIndex", 0);
			sheetChartMap.put("rowIndex", 0);
			sheetChartMap.put("chartIndex", 1);
			this.chartPanelLayout.put(sheetId, sheetChartMap);
		}

		// iterate through panel map to figure out layout
		for (String panelId : orderOfPanels) {
			InsightPanel panel = panelMap.get(panelId);
			String sheetId = panel.getSheetId();
			// for each panel get the task and task options
			SelectQueryStruct qs = panel.getLastQs();
			TaskOptions taskOptions = panel.getTaskOptions();
			if (qs == null || taskOptions == null) {
				continue;
			}
			qs.setLimit(numRowsToExport);
			IQuerySelector firstSelector = qs.getSelectors().get(0);
			if (firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
				qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
			} else {
				qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
			}
			ITask task = InsightUtility.constructTaskFromQs(this.insight, qs);
			task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
			task.setTaskOptions(taskOptions);
			Map<String, Object> panelChartMap = new HashMap<>();
			setChartLayout(panelChartMap, taskOptions, panelId);
			this.chartPanelLayout.get(sheetId).put(panelId, panelChartMap);
			writeData(workbook, task, sheetId, panelId, panel.getPanelFormatValues());
		}

		// now build charts
		for (String panelId : orderOfPanels) {
			InsightPanel panel = panelMap.get(panelId);
			// for each panel get the task and task options
			SelectQueryStruct qs = panel.getLastQs();
			TaskOptions taskOptions = panel.getTaskOptions();
			if (qs == null || taskOptions == null) {
				continue;
			}
			ITask task = InsightUtility.constructTaskFromQs(this.insight, qs);
			task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
			task.setTaskOptions(taskOptions);
			// add chart
			processTask(workbook, task, panel);
		}
		
		// Insert Semoss Logo after the last chart on each sheet
		//addLogo(workbook, sheetAlias);

		// rename sheets
		// not sure we need this
		/*
		for (String sheetId : sheetAlias.keySet()) {
			String sheetName = sheetAlias.get(sheetId);
			int sheetIndex = workbook.getSheetIndex(sheetId);
			if(sheetIndex >= 0) {
				workbook.setSheetName(sheetIndex, sheetName);
			}
		}*/

		
		// add the last row count
		// put the export map back
		// count is already added by the way of createBaseChart
		//putMap();
		
		// fill the headers
		String para1 = null;
		String para2 = null;
		
		if(exportMap.containsKey("para1"))
			para1 = (String)exportMap.get("para1");
		if(exportMap.containsKey("para2"))
			para2 = (String)exportMap.get("para2");

		if(para1 != null || para2 != null)
			fillHeader(workbook, exportMap, para1, para2);
		
		// fill the footers
		if(exportMap.containsKey("footer"))
			fillFooter(workbook, exportMap, (String)exportMap.get("footer"));

		// close the driver
		if(driver != null)
		  driver.quit();

		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		
		
		if (password != null) {
			// encrypt file
			ExcelUtility.encrypt(workbook, fileLocation, password);
		} else {
			// write file
			String newFileLocation = fileLocation.substring(0, fileLocation.indexOf(".xlsx"));
			newFileLocation = newFileLocation + "1.xlsx";
			
			ExcelUtility.writeToFile(workbook, newFileLocation);
			new File(fileLocation).delete();
			this.insight.addExportFile(exportName, newFileLocation);
		}
		
		return retNoun;
	}
	
	

	private void addLogo(XSSFWorkbook workbook, Map<String, String> sheetAlias) {
		String semossLogoPath = DIHelper.getInstance().getProperty("EXPORT_SEMOSS_LOGO");
		if (semossLogoPath != null) {
			File logo = new File(semossLogoPath);
			if (logo.exists()) {
				// Load image
				byte[] picture = null;
				try {
					picture = IOUtils.toByteArray(new FileInputStream(semossLogoPath));
				} catch (FileNotFoundException e) {
					logger.error(Constants.STACKTRACE, e);
				} catch (IOException ioe) {
					logger.error(Constants.STACKTRACE, ioe);
				}

				// Insert image into workbook
				int pictureIndex = workbook.addPicture(picture, Workbook.PICTURE_TYPE_PNG);

				// Insert logo into each sheet
				Iterator<String> sheetIdIterator = sheetAlias.keySet().iterator();
				while (sheetIdIterator.hasNext()) {
					String sheetId = sheetIdIterator.next();
					// Get location for logo on current sheet
					Map<String, Object> sheetChartMap = this.chartPanelLayout.get(sheetId);
					int colIndex = (int) sheetChartMap.get("colIndex");
					// add an additional space
					colIndex++;
					int chartIndex = (int) sheetChartMap.get("chartIndex");
					// Helper returns an object that handles instantiating
					// concrete classes
					CreationHelper helper = workbook.getCreationHelper();
					int sheetIndex = workbook.getSheetIndex(sheetId);
					// if we have a sheet that is empty
					// it doesn't get created
					// so that will return -1
					// will also remove so we dont try to rename this
					// in the next step
					if (sheetIndex == -1) {
						sheetIdIterator.remove();
						continue;
					}
					XSSFSheet sheet = workbook.getSheetAt(sheetIndex);
					Drawing drawing = sheet.createDrawingPatriarch();
					// Create an anchor that is attached to the worksheet
					ClientAnchor anchor = helper.createClientAnchor();
					// Attach locations to anchor
					// The seemingly random numbers we're adding by are to make
					// the image look good/not stretched out
					anchor.setCol1(colIndex + 2);
					anchor.setRow1(chartIndex + 1);
					anchor.setCol2(colIndex + 8);
					anchor.setRow2(chartIndex + 4);
					// Create the picture
					Picture pict = drawing.createPicture(anchor, pictureIndex);
				}
			}
		}
	}

	private void setChartLayout(Map<String, Object> panelChartMap, TaskOptions taskOptions, String panelId) {
		String chartLayout = taskOptions.getLayout(panelId);
		panelChartMap.put("chartType", chartLayout);
		Map<String, Object> alignmentMap = taskOptions.getAlignmentMap(panelId);
		if (chartLayout.equals("Line") || chartLayout.equals("Area") || chartLayout.equals("Column")
				|| chartLayout.equals("Pie") || chartLayout.equals("Radar")) {
			List<String> label = (List) alignmentMap.get("label");
			panelChartMap.put("x-axis", label);
			List<String> yColumnNames = (List) alignmentMap.get("value");
			panelChartMap.put("y-axis", yColumnNames);
			for (String column : label) {
				panelChartMap.put(column, new HashMap<>());
			}
			for (String column : yColumnNames) {
				panelChartMap.put(column, new HashMap<>());
			}
		} else if (chartLayout.equals("Scatter")) {
			List<String> label = (List) alignmentMap.get("label");
			panelChartMap.put("label", label);
			List<String> x = (List) alignmentMap.get("x");
			panelChartMap.put("x-axis", x);
			List<String> yColumnNames = (List) alignmentMap.get("y");
			panelChartMap.put("y-axis", yColumnNames);
			for (String column : label) {
				panelChartMap.put(column, new HashMap<>());
			}
		} // making accomodation for pivot
		else if (chartLayout.contentEquals("Pivot Table"))
		{
			// rows
			// columns
			// calculations
			List <String> rows = (List)alignmentMap.get("rows");
			List <String> columns = (List)alignmentMap.get("columns");
			List <String> calcs = (List)alignmentMap.get("calculations");
		}
	}

	private void processTask(XSSFWorkbook workbook, ITask task, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();
		String sheetName = sheetAlias.get(sheetId);
		TaskOptions tOptions = task.getTaskOptions();
		Map<String, Object> options = tOptions.getOptions();
		XSSFSheet sheet = workbook.getSheet(sheetName);
		XSSFSheet dataSheet = workbook.getSheet(sheetName + "_Data");

		// Insert chart if supported
		try
		{
			String plotType = tOptions.getLayout(panelId);
			if (plotType.equals("Line")) {
				insertLineChart(sheet, dataSheet, options, panel);
			} else if (plotType.equals("Scatter")) {
				insertScatterChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Area")) {
				insertAreaChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Column")) {
				insertBarChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Pie")) {
				insertPieChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Radar")) {
				insertRadarChart(sheet, dataSheet,options, panel);
			} else if(!plotType.equals("Grid") && !plotType.equals("PivotTable")) { // do it only for non grid.. for grid we still need to do something else
				insertImage(workbook, sheet, sheetId, panelId);
				//PivotTable
			}else if(plotType.equals("Grid") || plotType.equals("PivotTable")) { // do it only for non grid.. for grid we still need to do something else
				insertGrid(sheet.getSheetName(), panelId);
			}

		} catch(Exception ex) {
			ex.printStackTrace();
			if(driver != null) {
				driver.quit();
			}
			driver = null;
		} finally {
			if(driver != null) {
				driver.quit();
			}
			driver = null;
		}
	}

	private void writeData(XSSFWorkbook workbook, ITask task, String sheetId, String panelId, Map<String , Map<String, String>> panelFormatting) {
		CreationHelper createHelper = workbook.getCreationHelper();
		String sheetName = sheetAlias.get(sheetId);
		XSSFSheet sheet = workbook.getSheet(sheetName);
		if (sheet == null) {
			sheet = getSheet(workbook, sheetName);
			// also create a data sheet
			// also hide the sheet
			// no need to worry about creating template for data sheet
			sheet = workbook.createSheet(sheetName + "_Data");
			workbook.setSheetHidden(workbook.getSheetIndex(sheet), true);
		}
		else // create the data sheet as well
			sheet = workbook.getSheet(sheetName + "_Data");
			
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
		Row excelRow = null;
		// the task data is being dumped vertically
		// we need to know where to start putting in data
		Map<String, Object> sheetMap = this.chartPanelLayout.get(sheetId);
		int curSheetCol = 0;
		int endRow = (int) sheetMap.get("rowIndex");
		int excelRowCounter = endRow;
				

		// we need to iterate and write the headers during the first time
		if (task.hasNext()) {
			IHeadersDataRow row = task.next();
			List<Map<String, Object>> headerInfo = task.getHeaderInfo();

			// create the header row
			Row headerRow = sheet.createRow(excelRowCounter++);
			
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
				curSheetCol = i;
				Cell cell = headerRow.createCell(curSheetCol);
				cell.setCellValue(headers[i]);
				cell.setCellStyle(headerCellStyle);
				// grab metadata from iterator
				typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
				additionalDataTypeArr[i] = headerInfo.get(i).get("additionalDataType") + "";
				try {
					stylingArr[i] = POIExportUtility.getCurrentStyle(workbook, additionalDataTypeArr[i], panelFormatting.get(headers[i])); 
				} catch(Exception e) {
					// ignore
				}
				if(stylingArr[i] == null) {
					if(typesArr[i] == SemossDataType.DATE) {
						stylingArr[i] = dateCellStyle;
					} else if(typesArr[i] == SemossDataType.TIMESTAMP) {
						stylingArr[i] = timeStampCellStyle;
					}
				}
			}

			// generate the data row

			excelRow = sheet.createRow(excelRowCounter++);

			Object[] dataRow = row.getValues();
			i = 0;
			for (; i < size; i++) {
				curSheetCol = i;
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
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue((boolean) value);
					} else {
						cell.setCellValue(value + "");
					}
					
					if(stylingArr[i] != null) {
						cell.setCellStyle(stylingArr[i]);
					}
				}
			}
		}

		// now iterate through all the data
		while (task.hasNext()) {
			excelRow = sheet.createRow(excelRowCounter++);
			IHeadersDataRow row = task.next();
			Object[] dataRow = row.getValues();
			i = 0;
			for (; i < size; i++) {
				curSheetCol = i;
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
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.TIMESTAMP) {
						if(value instanceof SemossDate) {
							cell.setCellValue( ((SemossDate) value).getDate() ) ;
						} else {
							cell.setCellValue(value + "");
						}
					} else if (typesArr[i] == SemossDataType.BOOLEAN) {
						cell.setCellValue((boolean) value);
					} else {
						cell.setCellValue(value + "");
					}
					
					if(stylingArr[i] != null) {
						cell.setCellStyle(stylingArr[i]);
					}
				}
			}
		}

		// Update col and row bounds for sheet
		// this keeps track at the sheet level where to add next task
		sheetMap.put("colIndex", 0);
		// add the offset of rows
		sheetMap.put("rowIndex", endRow + (excelRowCounter - endRow));		

		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);
		List<String> headerList = Arrays.asList(headers);

		// this map defines the start and end for each column
		if (headers != null && headers.length > 0) {
			for (String header : headers) {
				Map<String, Object> columnMap = new HashMap<>();
				columnMap.put("startRow", endRow + 1);
				columnMap.put("endRow", excelRowCounter - 1);
				int headerIndex = headerList.indexOf(header);
				columnMap.put("startCol", headerIndex);
				columnMap.put("endCol", headerIndex);
				panelMap.put(header, columnMap);
			}
		}
	}

	private void insertLineChart(XSSFSheet sheet, XSSFSheet dataSheet, Map<String, Object> options, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();

		// retrieve ornaments
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.editGrid.x") + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.editGrid.y") + "");
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.displayValues") + "");
        String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
 		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("{}") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("{}") ? Boolean.parseBoolean(xAxisFlag) : true;
 		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
 		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";
 		String colorName = panel.getMapInput(panel.getOrnaments(), COLOR_NAME) + "";
		Object customColors = panel.getMapInput(panel.getOrnaments(), CUSTOM_COLOR_ARRAY);
        Object colorObject = panel.getMapInput(panel.getOrnaments(), COLOR_ARRAY);
        String[] colorArray = POIExportUtility.getHexColorCode(colorName, customColors, colorObject);

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
		List<String> label = (List<String>) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(leftAxisCrosses);

		// Add Y Axis Title
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("{}")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("{}")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}

		XDDFLineChartData data = (XDDFLineChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		// do it based on data sheet
		
		XDDFNumericalDataSource xs = createXAxis(dataSheet, xColumnMap);

		// Add in y vals
		int yCounter = 0;
		for (int i = 0; i < yColumnNames.size(); i++) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnNames.get(i));
			XDDFNumericalDataSource ys = createYAxis(dataSheet, yColumnMap);
			XDDFLineChartData.Series chartSeries = (XDDFLineChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnNames.get(i).replaceAll("_", " "), null);
			// Standardize markers
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(POIExportUtility.hex2Rgb(colorArray[yCounter%colorArray.length])));
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			XDDFShapeProperties propertiesMarker = new XDDFShapeProperties();
			propertiesMarker.setFillProperties(fillProperties);
			chart.getCTChart().getPlotArea().getLineChartArray(0).getSerArray(i).getMarker().addNewSpPr()
					.set(propertiesMarker.getXmlObject());
			// Standardize line
			XDDFLineProperties lineProperties = new XDDFLineProperties();
			lineProperties.setFillProperties(fillProperties);
			chartSeries.setLineProperties(lineProperties);
			yCounter++;
		}

		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			POIExportUtility.displayValues(ChartTypes.LINE, chart);
		}
		
	}
	
	private void insertScatterChart(XSSFSheet sheet, XSSFSheet dataSheet, Map<String, Object> options, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();

		// retrieve ornaments
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
		String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("{}") ? Boolean.parseBoolean(yAxisFlag) : true;
		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("{}") ? Boolean.parseBoolean(xAxisFlag) : true;
		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";

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
		List<String> label = (List<String>) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, null);
		XDDFValueAxis bottomAxis = chart.createValueAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
	//	leftAxis.setMinimum(0.4);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(leftAxisCrosses);

		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("{}")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("{}")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}

		XDDFScatterChartData data = (XDDFScatterChartData) chart.createData(chartType, bottomAxis, leftAxis);
		// Add in x vals
		XDDFDataSource xs = createXAxis(dataSheet, xColumnMap);

		// Add in y vals
		for (int i = 0; i < yColumnNames.size(); i++) {
			String yColumnName = yColumnNames.get(i);
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(dataSheet, yColumnMap);
			XDDFScatterChartData.Series chartSeries = (XDDFScatterChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			chartSeries.setSmooth(false);
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			CTScatterChart scatterSeries = chart.getCTChart().getPlotArea().getScatterChartArray(0);
			scatterSeries.getSerArray(i).addNewSpPr().addNewLn().addNewNoFill();
			scatterSeries.addNewVaryColors().setVal(false);
		}
		
		chart.plot(data);
		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			POIExportUtility.displayValues(ChartTypes.SCATTER, chart);
		}
	}

	private void insertBarChart(XSSFSheet sheet, XSSFSheet dataSheet, Map<String, Object> options, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();

		// retrieve ornaments
		Boolean toggleStack = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.toggleStack") + "");
		Boolean flipAxis = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.rotateAxis") + "");
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
		String displayValuesPosition = panel.getMapInput(panel.getOrnaments(), "tools.shared.customizeBarLabel.position") + "";
		String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("{}") ? Boolean.parseBoolean(yAxisFlag) : true;
		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("{}") ? Boolean.parseBoolean(xAxisFlag) : true;
		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";
		String colorName = panel.getMapInput(panel.getOrnaments(), COLOR_NAME) + "";
		Object customColors = panel.getMapInput(panel.getOrnaments(), CUSTOM_COLOR_ARRAY);
        Object colorObject = panel.getMapInput(panel.getOrnaments(), COLOR_ARRAY);
        String[] colorArray = POIExportUtility.getHexColorCode(colorName, customColors, colorObject);
        
        
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
		List<String> label = (List<String>) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		//chart.setTitleText("Title Test");
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		leftAxis.setCrossBetween(AxisCrossBetween.BETWEEN);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(leftAxisCrosses);

		// Add Y Axis Title
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("{}")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("{}")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}

		XDDFBarChartData data = (XDDFBarChartData) chart.createData(chartType, bottomAxis, leftAxis);

		if (flipAxis) {
			data.setBarDirection(BarDirection.BAR);
		} else {
			data.setBarDirection(BarDirection.COL);
		}

		if (toggleStack) {
			data.setBarGrouping(BarGrouping.STACKED);
			// correcting the overlap so bars really are stacked and not side by side
			chart.getCTChart().getPlotArea().getBarChartArray(0).addNewOverlap().setVal((byte) 100);
		}
		data.setGapWidth(10);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(dataSheet, xColumnMap);

		// Add in y vals
		int yCounter = 0;
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(dataSheet, yColumnMap);
			XDDFBarChartData.Series chartSeries = (XDDFBarChartData.Series) data.addSeries(xs, ys);
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(POIExportUtility.hex2Rgb(colorArray[yCounter%colorArray.length])));
            chartSeries.setFillProperties(fillProperties);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			yCounter++;
		}

		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			CTDLbls dLbls = POIExportUtility.displayValues(ChartTypes.BAR, chart);
			POIExportUtility.positionDisplayValues(ChartTypes.BAR, dLbls, displayValuesPosition);
		}
	}
	
	private void insertAreaChart(XSSFSheet sheet, XSSFSheet dataSheet, Map<String, Object> options, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();

		// retrieve ornaments
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
        String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
 		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("{}") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("{}") ? Boolean.parseBoolean(xAxisFlag) : true;
 		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
 		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";
 		String colorName = panel.getMapInput(panel.getOrnaments(), COLOR_NAME) + "";
		Object customColors = panel.getMapInput(panel.getOrnaments(), CUSTOM_COLOR_ARRAY);
        Object colorObject = panel.getMapInput(panel.getOrnaments(), COLOR_ARRAY);
        String[] colorArray = POIExportUtility.getHexColorCode(colorName, customColors, colorObject);
        
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
		List<String> label = (List<String>) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(leftAxisCrosses);

		// Add Y Axis Title
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("{}")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("{}")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}

		XDDFAreaChartData data = (XDDFAreaChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(dataSheet, xColumnMap);

		// Add in y vals
		int yCounter = 0;
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(dataSheet, yColumnMap);
			XDDFAreaChartData.Series chartSeries = (XDDFAreaChartData.Series) data.addSeries(xs, ys);
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(POIExportUtility.hex2Rgb(colorArray[yCounter%colorArray.length])));
			chartSeries.setFillProperties(fillProperties);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			yCounter++;
		}

		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			POIExportUtility.displayValues(ChartTypes.AREA, chart);
		}
	}

	private void insertPieChart(XSSFSheet sheet, XSSFSheet dataSheet,  Map<String, Object> options, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();

		// retrieve ornaments
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
		String displayValuesPosition = panel.getMapInput(panel.getOrnaments(), "tools.shared.customizePieLabel.position") + "";

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
		List<String> label = (List<String>) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFPieChartData data = (XDDFPieChartData) chart.createData(chartType, null, null);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(dataSheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(dataSheet, yColumnMap);
			XDDFPieChartData.Series chartSeries = (XDDFPieChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			chartSeries.setExplosion((long) 0);
		}
		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			CTDLbls dLbls = POIExportUtility.displayValues(ChartTypes.PIE, chart);
			POIExportUtility.positionDisplayValues(ChartTypes.PIE, dLbls, displayValuesPosition);
		}
	}

	private void insertRadarChart(XSSFSheet sheet, XSSFSheet dataSheet, Map<String, Object> options, InsightPanel panel) {
		String panelId = panel.getPanelId();
		String sheetId = panel.getSheetId();

		// retrieve ornaments
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
        String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
 		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("{}") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("{}") ? Boolean.parseBoolean(xAxisFlag) : true;
 		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
 		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";		             

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
		List<String> label = (List<String>) panelMap.get("x-axis");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) panelMap.get("y-axis");
		Map<String, Object> xColumnMap = (Map<String, Object>) panelMap.get(xColumnName);

		// Build chart
		XSSFChart chart = createBaseChart(sheet, sheetMap, legendPosition);
		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(bottomAxisPosition);
		XDDFValueAxis leftAxis = chart.createValueAxis(leftAxisPosition);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(leftAxisCrosses);

		// Add Y Axis Title
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("{}")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("{}")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}

		XDDFRadarChartData data = (XDDFRadarChartData) chart.createData(chartType, bottomAxis, leftAxis);

		// Add in x vals
		XDDFNumericalDataSource xs = createXAxis(dataSheet, xColumnMap);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Map<String, Object> yColumnMap = (Map<String, Object>) panelMap.get(yColumnName);
			XDDFNumericalDataSource ys = createYAxis(dataSheet, yColumnMap);
			XDDFRadarChartData.Series chartSeries = (XDDFRadarChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
		}

		chart.plot(data);
	}

	// this is where the chart is being placed. Need to see this properly
	private XSSFChart createBaseChart(XSSFSheet sheet, Map<String, Object> sheetMap, LegendPosition legendPosition) {
		XSSFDrawing drawing = sheet.createDrawingPatriarch();
		// Put chart to the right of any data columns
		int colIndex = (int) sheetMap.get("colIndex");
		int chartIndex = (int) sheetMap.get("chartIndex");
		// drawing can be at 0 ,0
		int drawingColIndex = startColumn + this.columnGutter ;
		int sheetLastRow = 0; 
		if(exportMap.containsKey(sheet.getSheetName() + "ROW_COUNT"))
			sheetLastRow = Integer.parseInt(exportMap.get(sheet.getSheetName() + "ROW_COUNT") + "");
		
		XSSFClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, drawingColIndex, sheetLastRow, drawingColIndex + width,
				sheetLastRow + height);
		// Increment for positioning other objects correctly
		sheetMap.put("chartIndex", chartIndex + height + rowGutter);
		XSSFChart chart = drawing.createChart(anchor);
		if(legendPosition != null) {
			XDDFChartLegend legend = chart.getOrAddLegend();
			legend.setPosition(legendPosition);
		}
		sheetLastRow = sheetLastRow + height + rowGutter;
		exportMap.put(sheet.getSheetName() + "ROW_COUNT", sheetLastRow);
		
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
	
	private void insertGrid(String sheetId, String panelId) {
		//http://localhost:9090/semoss/#!/html?engine=95079463-9643-474a-be55-cca8bf91b358&id=735f32dd-4ec0-46ce-b2fa-4194cc270c7a&panel=0 
		//http://localhost:9090/semoss/#!/html?insightId=95079463-9643-474a-be55-cca8bf91b358&panel=0  
		// http://localhost:8080/appui/#!/html?insightId=d08a5e71-af2f-43d8-89e1-f806ff0527ea&panel=5 - this worked
		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String htmlUrl = baseUrl + "html?insightId=" + insight.getInsightId() + "&sheet=" + sheetId + "&panel=" + panelId;
		logger.info("Generating grid at " + htmlUrl);
		if(driver == null) {
			driver = ChromeDriverUtility.makeChromeDriver(baseUrl, htmlUrl, sessionId, 800, 600);
		}
		logger.info("Generating grid view");
		ChromeDriverUtility.captureDataPersistent(driver, baseUrl, htmlUrl, sessionId);
		WebElement we = driver.findElement(By.xpath("//html/body//table"));
		String html2 = driver.executeScript("return arguments[0].outerHTML;", we) + "";
		
		//WebElement elem1 = new WebDriverWait(driver, 10)
		//        .until(ExpectedConditions.elementToBeClickable(By.xpath("//html/body//table")));
		//html = driver.executeScript("return document.documentElement.outerHTML;") + "";
		//System.out.println(html);
		//System.out.println(html2);
		//driver.quit();
		//driver = null; 
		
		TableToXLSXReactor txl = new TableToXLSXReactor();
		txl.exportMap = exportMap;
		txl.html = html2;
		txl.sheetName = sheetId;
		String fileName = (String)exportMap.get("FILE_NAME");
		
		txl.processTable(sheetId, html2, fileName);
		logger.info("Done processing grid");
	}

	/**
	 * 
	 * @param wb
	 * @param targetSheet
	 * @param sheetId
	 * @param panelId
	 */
	private void insertImage(Workbook wb, XSSFSheet targetSheet, String sheetId, String panelId) {
		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.insight.getLiveURL();
		String panelAppender = "&panel=" + panelId;
		String sheetAppender = "&sheet=" + sheetId;
		
		String prefixName = Utility.getRandomString(8);
		String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "png");
		String imageLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + exportName;

		if(driver == null) {
			driver = ChromeDriverUtility.makeChromeDriver(baseUrl, imageUrl + sheetAppender + panelAppender, sessionId, 800, 600);
		}
		// download this file
		ChromeDriverUtility.captureImagePersistent(driver, baseUrl, imageUrl + sheetAppender + panelAppender, imageLocation, sessionId);
		
		driver.quit();
		driver = null;

		// download this file
		//ChromeDriverUtility.captureImage(baseUrl, imageUrl + sheetAppender + panelAppender, fileLocation, sessionId, 800, 600, true);
		// write this to the sheet now
		int sheetLastRow = 0; 
		if(exportMap.containsKey(targetSheet.getSheetName() + "ROW_COUNT"))
			sheetLastRow = Integer.parseInt(exportMap.get(targetSheet.getSheetName() + "ROW_COUNT") + "");

		//1920 x 936
		//FileInputStream obtains input bytes from the image file
		try {
			InputStream inputStream = new FileInputStream(imageLocation);
			//Get the contents of an InputStream as a byte[].
			byte[] bytes = IOUtils.toByteArray(inputStream);
			//Adds a picture to the workbook
			int pictureIdx = wb.addPicture(bytes, Workbook.PICTURE_TYPE_PNG);
			//close the input stream
			inputStream.close();

			FileUtils.forceDelete(new File(imageLocation));

			//Returns an object that handles instantiating concrete classes
			CreationHelper helper = wb.getCreationHelper();
			//Creates the top-level drawing patriarch.
			Drawing drawing = targetSheet.createDrawingPatriarch();

			//Create an anchor that is attached to the worksheet
			ClientAnchor anchor = helper.createClientAnchor();

			
			//create an anchor with upper left cell _and_ bottom right cell
			anchor.setCol1(startColumn); //Column B
			anchor.setRow1(sheetLastRow); //Row 3
			anchor.setCol2(startColumn + width); //Column C // doesnt matter
			anchor.setRow2(sheetLastRow+height); //Row 4

			//Creates a picture
			Picture pict = drawing.createPicture(anchor, pictureIdx);
			//pict.resize();

			//Reset the image to the original size
			//pict.resize(); //don't do that. Let the anchor resize the image!
			//Create the Cell B3
			Cell cell = targetSheet.createRow(2).createCell(1);
		} catch (IOException e) {
			e.printStackTrace();
		}

		sheetLastRow = sheetLastRow + height + rowGutter;
		exportMap.put(targetSheet.getSheetName() + "ROW_COUNT", sheetLastRow);		
	}
	
}
