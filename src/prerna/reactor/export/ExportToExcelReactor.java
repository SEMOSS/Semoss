package prerna.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLbls;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPieChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTScatterChart;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.ColorByValueRule;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.poi.main.helper.excel.ExcelUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
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
	private static final String CHART_TITLE = "tools.shared.chartTitle.text";
	
	// for exporting grid
	private static final String GRIDSPANROWS = "tools.shared.gridSpanRows";
	private static final String TABLE_STYLE = "table-layout: fixed;border-collapse: collapse; border: 1px solid #d9d9d9; font-family: Arial, Helvetica, sans-serif; width: 100%; max-width: 600px;";
	private static final String THEAD_STYLE = "background: #f5f5f5; color: #5c5c5c;";
	private static final String TH_STYLE = "border: 1px solid #d9d9d9; padding: 8px;width: 200px; background-color: #00A8C1;color: #FFFFFF;font-size: .875em;";
	private static final String TD_STYLE = "border: 1px solid #d9d9d9; padding: 8px;font-size: .875em;";

	
	protected String fileLocation = null;
	Map <String, List<String>> orderOfPanelsMap = null;
	
	protected Logger logger;

	Object driver = null;
	private Map<String, Map<String, Object>> chartPanelLayout = new HashMap<>();
	int height = 10;
	int width = 10;
	
	ChromeDriverUtility util = null;
	
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
				ReactorKeysEnum.PANEL_ORDER_IDS.getKey(),
				ReactorKeysEnum.EXPORT_AUDIT.getKey(),
				ReactorKeysEnum.PLACE_HOLDER_DATA.getKey(),
				ReactorKeysEnum.PROJECT.getKey()
			};
		this.keyRequired = new int[] {0,0,0,0,0,0,0,0,0,0,0,0,0};
		this.keyMulti = new int[] {0,0,0,0,0,0,1,0,0,0,0,0,0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		// get the map also
		getMap(insight.getInsightId());
		processPayload();
		this.logger = getLogger(CLASS_NAME);

		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(exportMap.get("FILE_NAME") + ""); 
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "xlsx");
		// grab file path to write the file
		String fileLocation =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		
		boolean exportAudit = false;
		// check if the export audit has been selected for export
		if (keyValue.containsKey(ReactorKeysEnum.EXPORT_AUDIT.getKey())) {
			String auditParam = (String) keyValue.get(ReactorKeysEnum.EXPORT_AUDIT.getKey());
			exportAudit = auditParam.equalsIgnoreCase("yes") || auditParam.equalsIgnoreCase("true");
		}
		
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(fileLocation);
		exportMap.put("FILE_LOCATION", fileLocation);

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
		if(grs != null) {
			orderOfPanels = new ArrayList<String>(); 
			for(int idx = 0;idx < grs.size();idx++) {
				orderOfPanels.add(grs.get(idx) + "");
			}
		} else {
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
			TaskOptions taskOptions = panel.getLastTaskOptions();
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
			String plotType = taskOptions.getLayout(panelId);
			//condition for stack bar chart its expecting different data format for stack
			if (plotType != null && (plotType.equals("Stack") || plotType.equals("MultiLine"))) {
				writeChartCategoryData(workbook, task, sheetId, panelId, panel.getPanelFormatValues());
			} else {
				writeData(workbook, task, sheetId, panelId, panel.getPanelFormatValues());
			}
		}

		// now build charts
		for (String panelId : orderOfPanels) {
			InsightPanel panel = panelMap.get(panelId);
			// for each panel get the task and task options
			SelectQueryStruct qs = panel.getLastQs();
			TaskOptions taskOptions = panel.getLastTaskOptions();
			if (qs == null || taskOptions == null) {
				continue;
			}
			ITask task = InsightUtility.constructTaskFromQs(this.insight, qs);
			task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
			task.setTaskOptions(taskOptions);
			// add chart
			processTask(user, workbook, task, panel);
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
		
		if(exportMap.containsKey("para1")) {
			para1 = (String)exportMap.get("para1");
		}
		if(exportMap.containsKey("para2")) {
			para2 = (String)exportMap.get("para2");
		}
		
		// process and apply the audit param sheet if the export Audit has been opted
		// exportMap stores all the export related properties
		if (exportAudit) {
			makeParamSheet(workbook, this.insight,true, exportMap);
		}
		//moved the footer and header logic under export audit to apply headers and disclaimers
		if(para1 != null || para2 != null) {
			fillHeader(workbook, exportMap, para1, para2);
		}
		
		// fill the footers
		if(exportMap.containsKey("footer")) {
			fillFooter(workbook, exportMap, (String)exportMap.get("footer"));
		}
		
		// fill the place holders
		if(exportMap.containsKey("placeholders") && null!= exportMap.get("placeholders")) {
			fillPlaceholders(workbook, exportMap, (Map<String, List<String>>) exportMap.get("placeholders"));
		}
		
		// remove the sheets after processing from the resulted export file
		removeSheet(workbook);
		
		// close the driver
		insight.getChromeDriver().quit(driver);
		/*
		if(driver != null && driver instanceof ChromeDriver) {
			((ChromeDriver)driver).quit();
		}*/

		// write / encrypt file
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if (password != null) {
			ExcelUtility.encrypt(workbook, fileLocation, password);
		} else {
			ExcelUtility.writeToFile(workbook, fileLocation);
		}
		
		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);
		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
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

	private void processTask(User user, XSSFWorkbook workbook, ITask task, InsightPanel panel) {
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
			if (plotType.equals("Line")||plotType.equals("MultiLine")) {
				insertLineChart(sheet, dataSheet, options, panel);
			} else if (plotType.equals("Scatter")) {
				insertScatterChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Area")) {
				insertAreaChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Column") || plotType.equals("Stack")) {
				insertBarChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Pie")) {
				insertPieChart(sheet, dataSheet,options, panel);
			} else if (plotType.equals("Radar")) {
				insertRadarChart(sheet, dataSheet,options, panel);
			} else if(!plotType.equals("Grid") && !plotType.equals("PivotTable")) { // do it only for non grid.. for grid we still need to do something else
				insertImage(workbook, user, sheet, sheetId, panelId);
			} else if (plotType.equals("Grid")) {
				insertGrid(sheet.getSheetName(), task, panel);
			} else if(plotType.equals("PivotTable")) { // do it only for non grid.. for grid we still need to do something else
				
				String pivRoutine = DIHelper.getInstance().getProperty("OPTIMIZE_PIVOT_EXPORT");
				if(pivRoutine != null && pivRoutine.equalsIgnoreCase("True"))
				{
					prerna.reactor.frame.py.CollectPivotReactor cpr = new prerna.reactor.frame.py.CollectPivotReactor();
					cpr.setNounStore(tOptions.getCollectStore());
					cpr.setInsight(insight);
					List rowObject = tOptions.getCollectStore().getNoun(cpr.keysToGet[0]).getAllValues();
					//cpr.
					insertPivot2(sheet.getSheetName(), cpr, rowObject);
				}
				else // old routine
					insertPivot(user, sheet.getSheetName(), panelId, sheetId);
			}
			
		} catch(Exception ex) {
			ex.printStackTrace();
			insight.getChromeDriver().quit(driver);

//			if(driver != null && driver instanceof ChromeDriver) {
//				((ChromeDriver)driver).quit();
//			}
			driver = null;
		} finally {
			insight.getChromeDriver().quit(driver);
//			if(driver != null && driver instanceof ChromeDriver) {
//				((ChromeDriver)driver).quit();
//			}
//			driver = null;
		}
	}

	private void writeData(XSSFWorkbook workbook, ITask task, String sheetId, String panelId, Map<String , Map<String, String>> panelFormatting) {
		CreationHelper createHelper = workbook.getCreationHelper();
		String sheetName = sheetAlias.get(sheetId);
		XSSFSheet sheet = workbook.getSheet(sheetName);
		if (sheet != null) {
			// create the data sheet as well
			sheet = workbook.getSheet(sheetName + "_Data");
		}
		if (sheet == null) {
			sheet = getSheet(workbook, sheetName);
			// also create a data sheet
			// also hide the sheet
			// no need to worry about creating template for data sheet
			sheet = workbook.createSheet(sheetName + "_Data");
			workbook.setSheetHidden(workbook.getSheetIndex(sheet), true);
		} 
		// since we write veriticlaly
		// shouldn't be doing this anymore
		// freeze the first row
//		sheet.createFreezePane(0, 1);

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
				getFormatedCellWithValue(cell, headers[i], SemossDataType.STRING, headerCellStyle);
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
				getFormatedCellWithValue(cell, value, typesArr[i], stylingArr[i]);
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
				getFormatedCellWithValue(cell, value, typesArr[i], stylingArr[i]);
			}
		}
		
		// add an additional empty row at the end
		// so we have some spacing between
		excelRow = sheet.createRow(excelRowCounter++);

		// Update col and row bounds for sheet
		// this keeps track at the sheet level where to add next task
		sheetMap.put("colIndex", 0);
		// add the offset of rows
		sheetMap.put("rowIndex", endRow + (excelRowCounter - endRow));		

		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);

		// this map defines the start and end for each column
		if (headers != null && headers.length > 0) {
			List<String> headerList = Arrays.asList(headers);
			for (String header : headers) {
				Map<String, Object> columnMap = new HashMap<>();
				columnMap.put("startRow", endRow + 1);
				// -2 for the extra line break between the data
				columnMap.put("endRow", excelRowCounter - 2);
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
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
        String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
 		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
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
		
		//get the options data
		Map<String, Object> optionData	=(Map<String, Object>) options.get(panelId);

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
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				if ("MultiLine".equals(optionData.get("layout"))) {
					leftAxis.setTitle(String.join(", ", (List<String>) ((Map) optionData.get("alignment")).get("value")).replace("_",
							" "));
				} else {
					leftAxis.setTitle(String.join(", ", yColumnNames).replace("_", " "));
				}
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replace("_", " "));
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
			chartSeries.setTitle(yColumnNames.get(i).replace("_", " "), null);
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

		// add the title
		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
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
		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
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
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replace("_", " "));
			}
		}
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replace("_", " "));
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
			chartSeries.setTitle(yColumnName.replace("_", " "), null);
			chartSeries.setSmooth(false);
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			CTScatterChart scatterSeries = chart.getCTChart().getPlotArea().getScatterChartArray(0);
			scatterSeries.getSerArray(i).addNewSpPr().addNewLn().addNewNoFill();
			scatterSeries.addNewVaryColors().setVal(false);
		}
		
		// add the title
		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
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
		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
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
		//get the options data
		Map<String, Object> optionData	=(Map<String, Object>) options.get(panelId);

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
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				if ("Stack".equals(optionData.get("layout"))) {
					leftAxis.setTitle(String.join(", ", (List<String>) ((Map) optionData.get("alignment")).get("value")).replace("_",
							" "));
				} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replace("_", " "));
			}
		}}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replace("_", " "));
			}
		}

		XDDFBarChartData data = (XDDFBarChartData) chart.createData(chartType, bottomAxis, leftAxis);

		if (flipAxis) {
			data.setBarDirection(BarDirection.BAR);
		} else {
			data.setBarDirection(BarDirection.COL);
		}
		if("Stack".equals(optionData.get("layout"))){
			toggleStack= !toggleStack;
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
			chartSeries.setTitle(yColumnName!=null ? yColumnName.toString().replace("_", " "):null, null);
			yCounter++;
		}

		// add the title
		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
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
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
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
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replace("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replace("_", " "));
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
			chartSeries.setTitle(yColumnName.replace("_", " "), null);
			yCounter++;
		}

		// add the title
		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
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
		List<String> pieCustomDisplayValues = (List<String>) panel.getMapInput(panel.getOrnaments(), "tools.shared.customizePieLabel.dimension");
		
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
			chartSeries.setTitle(yColumnName.replace("_", " "), null);
			chartSeries.setExplosion((long) 0);
		}

		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
		}
		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			CTPieChart ctPieChart = chart.getCTChart().getPlotArea().getPieChartArray(0);
            ctPieChart.addNewDLbls();
            CTDLbls dLbls = ctPieChart.getDLbls();
            if(dLbls != null) {
            	if(pieCustomDisplayValues.contains("Percentage")) {
                    dLbls.addNewShowPercent().setVal(true);
            	} else {
                    dLbls.addNewShowPercent().setVal(false);
            	}
            	if(pieCustomDisplayValues.contains("Value")) {
                    dLbls.addNewShowVal().setVal(true);
            	} else {
                    dLbls.addNewShowVal().setVal(false);
            	}
            	if(pieCustomDisplayValues.contains("Name")) {
                    dLbls.addNewShowCatName().setVal(true);
            	} else {
                    dLbls.addNewShowCatName().setVal(false);
            	}
            	dLbls.addNewShowBubbleSize().setVal(false);
                dLbls.addNewShowLegendKey().setVal(false);
                dLbls.addNewShowSerName().setVal(false);
    			POIExportUtility.positionDisplayValues(ChartTypes.PIE, dLbls, displayValuesPosition);
            }
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
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
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
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replace("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replace("_", " "));
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
			chartSeries.setTitle(yColumnName.replace("_", " "), null);
		}

		// add the title
		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
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
	
	private void insertPivot(User user, String excelSheetName, String panelId, String sheetId) {
		//http://localhost:9090/semoss/#!/html?engine=95079463-9643-474a-be55-cca8bf91b358&id=735f32dd-4ec0-46ce-b2fa-4194cc270c7a&panel=0 
		//http://localhost:9090/semoss/#!/html?insightId=95079463-9643-474a-be55-cca8bf91b358&panel=0  
		// http://localhost:8080/appui/#!/html?insightId=d08a5e71-af2f-43d8-89e1-f806ff0527ea&panel=5 - this worked
		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String htmlUrl = baseUrl + "html?insightId=" + insight.getInsightId() + "&sheet=" + sheetId + "&panel=" + panelId;
		logger.info("Generating pivot at " + htmlUrl);
		if(driver == null) {
			//driver = ChromeDriverUtility.makeChromeDriver(baseUrl, htmlUrl,  800, 600);
			driver = insight.getChromeDriver().makeChromeDriver(baseUrl, htmlUrl,  800, 600);
		}
		logger.info("Generating pivot view");
		
		String exportName = AbstractExportTxtReactor.getExportFileName(user, "ABCD", "png");
		String imageLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + exportName;

		//this.insight.getChromeDriver().captureImagePersistent(driver, baseUrl, htmlUrl, imageLocation, sessionId, 10_000);
		
		String html2 = insight.getChromeDriver().captureDataPersistent(driver, baseUrl, htmlUrl, sessionId, 10_000);
		
		insight.getChromeDriver().quit(driver);
		driver = null;

		//logger.info(" HTML from Capture " + html2);
		//html2 = insight.getChromeDriver().getHTML(driver, "//html/body//table");
		//logger.info(" HTML from getHTML " + html2);
		//WebElement we = driver.findElement(By.xpath("//html/body//table"));
		//String html2 = driver.executeScript("return arguments[0].outerHTML;", we) + "";
		
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
		txl.sheetName = excelSheetName;
		String fileName = (String)exportMap.get("FILE_NAME");
		
		txl.processTable(excelSheetName, html2, fileName);
		logger.info("Done processing pivot");
	}
	
	private void insertPivot2(String excelSheetName, prerna.reactor.frame.py.CollectPivotReactor cpr, List rows) {
		//http://localhost:9090/semoss/#!/html?engine=95079463-9643-474a-be55-cca8bf91b358&id=735f32dd-4ec0-46ce-b2fa-4194cc270c7a&panel=0 
		//http://localhost:9090/semoss/#!/html?insightId=95079463-9643-474a-be55-cca8bf91b358&panel=0  
		// http://localhost:8080/appui/#!/html?insightId=d08a5e71-af2f-43d8-89e1-f806ff0527ea&panel=5 - this worked

		//logger.info(" HTML from Capture " + html2);
		//html2 = insight.getChromeDriver().getHTML(driver, "//html/body//table");
		//logger.info(" HTML from getHTML " + html2);
		//WebElement we = driver.findElement(By.xpath("//html/body//table"));
		//String html2 = driver.executeScript("return arguments[0].outerHTML;", we) + "";
		
		//WebElement elem1 = new WebDriverWait(driver, 10)
		//        .until(ExpectedConditions.elementToBeClickable(By.xpath("//html/body//table")));
		//html = driver.executeScript("return document.documentElement.outerHTML;") + "";
		//System.out.println(html);
		//System.out.println(html2);
		//driver.quit();
		//driver = null; 
		NounMetadata retData = cpr.execute();
		
		// you have what you need now..
		ConstantDataTask cdt = (ConstantDataTask)retData.getValue();
		// json is sitting in the cdt
		String json = (String)((Map)cdt.getOutputData()).get("values");

		JSONArray array = new JSONArray(json);
		StringBuffer html2 = new StringBuffer();
		for(int secIndex = 1;secIndex < array.length();secIndex++)
		{
			JSONObject obj = array.getJSONArray(secIndex).getJSONObject(0);
			html2.append(cpr.getJson2HTML(obj, rows));
		}
		
		TableToXLSXReactor txl = new TableToXLSXReactor();
		txl.exportMap = exportMap;
		txl.html = html2.toString();
		txl.sheetName = excelSheetName;
		String fileName = (String)exportMap.get("FILE_NAME");
		
		txl.processTable(excelSheetName, html2.toString(), fileName);
		logger.info("Done processing pivot");
	}

	
	/**
	 * Creating Html content and passed TableToXLSXReactor
	 * @param excelSheetName  
	 * @param task complete data 
	 * @param panel insight panel information
	 */
	private void insertGrid(String excelSheetName, ITask task, InsightPanel panel) {
		//get the string HTML from task
		TableToXLSXReactor txl = new TableToXLSXReactor();
		//it contains the all the param info
		txl.exportMap = exportMap;
		txl.sheetName = excelSheetName;
		String fileName = (String) exportMap.get("FILE_NAME");
		Boolean gridSpanRows = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRIDSPANROWS) + "");

		String[] headers = task.getHeaderInfo().stream().map(hdr->{
			return (String)hdr.get("header");
		}).collect(Collectors.toList()).stream().toArray(String[]::new);
		
		List<Map<String, Object>> headerInfo = task.getHeaderInfo();
		SemossDataType[] typesArr = new SemossDataType[headers.length];
		for (int i = 0; i < headers.length; i++) {
			typesArr[i] = SemossDataType.convertStringToDataType(headerInfo.get(i).get("type") + "");
		}

		//Putting the datatypes array of grid headers into export map
		exportMap.put(DATA_TYPES_ARRAY_KEY, typesArr);
		txl.isGrid = true;
		txl.processTable(excelSheetName, generateGridHtml(task, panel), fileName);
		if(gridSpanRows) {
			txl.mergeAreas();
		}
		logger.info("Done processing grid");
	}


	/**
	 * 
	 * @param wb
	 * @param targetSheet
	 * @param sheetId
	 * @param panelId
	 */
	private void insertImage(Workbook wb, User user, XSSFSheet targetSheet, String sheetId, String panelId) {
		String baseUrl = this.insight.getBaseURL();
		String sessionId = ThreadStore.getSessionId();
		String imageUrl = this.insight.getLiveURL();
		String panelAppender = "&panel=" + panelId;
		String sheetAppender = "&sheet=" + sheetId;
		
		String prefixName = Utility.getRandomString(8);
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "png");
		String imageLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + exportName;

		if(driver == null) {
			//driver = ChromeDriverUtility.makeChromeDriver(baseUrl, imageUrl + sheetAppender + panelAppender, 800, 600);
			driver = this.insight.getChromeDriver().makeChromeDriver(baseUrl, imageUrl + sheetAppender + panelAppender, 800, 600);
		}
		// download this file
		//ChromeDriverUtility.captureImagePersistent(driver, baseUrl, imageUrl + sheetAppender + panelAppender, imageLocation, sessionId);
		this.insight.getChromeDriver().captureImagePersistent(driver, baseUrl, imageUrl + sheetAppender + panelAppender, imageLocation, sessionId, 10_000);

		
		insight.getChromeDriver().quit(driver);
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
			InputStream inputStream = new FileInputStream(Utility.normalizePath(imageLocation));
			//Get the contents of an InputStream as a byte[].
			byte[] bytes = IOUtils.toByteArray(inputStream);
			//Adds a picture to the workbook
			int pictureIdx = wb.addPicture(bytes, Workbook.PICTURE_TYPE_PNG);
			//close the input stream
			inputStream.close();

			FileUtils.forceDelete(new File(Utility.normalizePath(imageLocation)));

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
	
	/*
	 * generate HTML string from the grid data
	 * 
	 * task contains the dataframe
	 * panel contains the all the ornaments and panel formats and color by values
	 */
	private String generateGridHtml(ITask task, InsightPanel panel) {
        // get the gridSpanRows param from the ornaments for the grid rowspan
		Boolean gridSpanRows = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRIDSPANROWS) + "");
		Map<String, Map<String, String>> panelFormatting = panel.getPanelFormatValues();
		// get the color by value details like on which  data we have to apply the color
		Map<ColorByValueRule, List<Object>> colorByValueMap = getColorByValueData(panel);

		StringBuilder html = new StringBuilder();
		String[] headers = null;
		List<Map<String, Object>> headerInfo = null;
		List<Object[]> rowList = new ArrayList<>();

		logger.info("Generating html view");
		// now we have the data, create the table
		html.append("<table style=\"" + TABLE_STYLE + "\">");
		// create the header
		if (task.hasNext()) {
			IHeadersDataRow row = task.next();
			headerInfo = task.getHeaderInfo();
			headers = row.getHeaders();
			
			html.append("<thead style=\"" + THEAD_STYLE + "\">");
			html.append("<tr>");
			// creating header row
			for (String header : headers) {
				html.append("<th style=\"" + TH_STYLE.toString() + "\">" + header + "</th>");
			}
			html.append("</tr>");
			html.append("</thead>");
			
			// creating body
			html.append("<tbody>");
			// add the first row to the list
			rowList.add(row.getValues());
			// adding the rest of the rows
			while (task.hasNext()) {
				row = task.next();
				rowList.add(row.getValues());
			}

			int[] rowSpan = new int[headers.length];
			for (int rowIdx = 0, rowLen = rowList.size(); rowIdx < rowLen; rowIdx++) {
				Object[] rowData = rowList.get(rowIdx);
				// creating data rows
				html.append("<tr>");
				for (int colIdx = 0, colLen = rowData.length; colIdx < colLen; colIdx++) {
					Object cell = rowData[colIdx];
					// check for row span flag
					if (gridSpanRows) {
						if (rowSpan[colIdx] > 1) {
							rowSpan[colIdx]--;
							continue;
						}
						// restart the spanning
						rowSpan[colIdx] = 1;
						// look at the next row
						for (int nextIdx = rowIdx + 1; nextIdx < rowLen; nextIdx++) {
							Object next = rowList.get(nextIdx)[colIdx];
							
							// Check if the current cell is equal to the next cell
							if (cell == next || (cell != null && next != null && cell.equals(next))) {
								// increment
								rowSpan[colIdx]++;								
							}
							//break the loop if current row cell and next row cell are not equal.
							else {
								break;
							}
						}
					}
					// get the background color of each cell or rows
					Object cellColor = FormattingUtility.getBackgroundColor(colorByValueMap, headers, rowData, colIdx);
					//get the formatted data values based on formatdatavalue tool applied 
					Object formattedDataValue = FormattingUtility.formatDataValues(
							cell,
							(String) headerInfo.get(colIdx).get("dataType"),
							(String) headerInfo.get(colIdx).get("additionalDataType"),
							panelFormatting.get(headers[colIdx]));
					//getting rowspancount as String
				   String rowSpanCount= rowSpan[colIdx] > 1 ? String.valueOf(rowSpan[colIdx]):""; 

					// creating td with data and styles (rowspan if applicable)
					html.append("<td style=\"" + TD_STYLE +
								cellColor.toString()+ 
							    "\"+ rowspan=" + rowSpanCount + ">" +
							    formattedDataValue+
							   "</td>");
				}
				html.append("</tr>");
			}
			html.append("</tbody>");
			html.append("</table>");
		}
		// write html to your C drive for testing purpose
		// WriteToFile(html.toString(), "test.html");		
		return html.toString();
	}

	/**
	 * This method returns the map with color by value rules and the raw values to
	 * apply color on
	 * @param panel
	 * @return
	 */
	private Map<ColorByValueRule, List<Object>> getColorByValueData(InsightPanel panel) {
		// Using LinkedHashMap - To Maintain insertion order
		Map<ColorByValueRule, List<Object>> colorByValueMap = new LinkedHashMap<>();
		for (ColorByValueRule cbv : panel.getColorByValue()) {
			// you can grab the query struct
			SelectQueryStruct cbvQS = cbv.getQueryStruct();
			// turn the query struct to a task
			// you can iterator through to know which values to paint
			ITask cbvTask = InsightUtility.constructTaskFromQs(this.insight, cbvQS);
			cbvTask.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
			List<Object> colorByValues = new ArrayList<>();

			while (cbvTask.hasNext()) {
				colorByValues.add(cbvTask.next().getRawValues()[0]);
			}
			colorByValueMap.put(cbv, colorByValues);
		}
		return colorByValueMap;
	}
	
	//transform the data for stackbar and multiline chart
	private void writeChartCategoryData(XSSFWorkbook workbook, ITask task, String sheetId, String panelId, Map<String, Map<String, String>> panelFormatting) {
		CreationHelper createHelper = workbook.getCreationHelper();
		String sheetName = sheetAlias.get(sheetId);
		XSSFSheet sheet = workbook.getSheet(sheetName);
		if (sheet != null) {
			// create the data sheet as well
			sheet = workbook.getSheet(sheetName + "_Data");
		}
		if (sheet == null) {
			sheet = getSheet(workbook, sheetName);
			// also create a data sheet
			// also hide the sheet
			// no need to worry about creating template for data sheet
			sheet = workbook.createSheet(sheetName + "_Data");
			workbook.setSheetHidden(workbook.getSheetIndex(sheet), true);
		} 
		// since we write veriticlaly
		// shouldn't be doing this anymore
		// freeze the first row
		//			sheet.createFreezePane(0, 1);

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

		Row headerRow = null;
		Map<String, Row> xAxisToRow = new HashMap<>();
		List<String> currentHeaderValues = new ArrayList<>();
		CellStyle headerCellStyle = null;
		int xAxisIndex = 0;
		int yAxisIndex = 1;
		int instanceColumnIndex = 2;
		// we need to iterate and write the headers during the first time
		if (task.hasNext()) {
			IHeadersDataRow row = task.next();
			List<Map<String, Object>> headerInfo = task.getHeaderInfo();

			// create the header row
			headerRow = sheet.createRow(excelRowCounter++);

			// create a Font for styling header cells
			Font headerFont = workbook.createFont();
			headerFont.setBold(true);
			// create a CellStyle with the font
			headerCellStyle = workbook.createCellStyle();
			headerCellStyle.setFont(headerFont);
			headerCellStyle.setAlignment(HorizontalAlignment.CENTER);
			headerCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);

			// generate the header row
			// and define constants used throughout like size, and types
			headers = row.getHeaders();
			size = headers.length;
			typesArr = new SemossDataType[size];
			additionalDataTypeArr = new String[size];
			stylingArr = new CellStyle[size];
			for (int i = 0; i < size; i++) {
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
			
			// we are creating our own table
			// where the data will come as
			// x-axis, y-axis value, split-for-columns
			// but we generate
			// x-axis, split-for-columns 1, split-for-columns 2
			// and the values will be what we get
			{
				currentHeaderValues.add(headers[0]);
				Cell cell = headerRow.createCell(0);
				cell.setCellValue(headers[0]);
				cell.setCellStyle(headerCellStyle);
			}
			
			// generate the first data row
			excelRow = sheet.createRow(excelRowCounter++);
			Object[] dataRow = row.getValues();
			// now we transpose this row to put it properly in the excel
			Object xAxisValue = dataRow[xAxisIndex];
			xAxisToRow.put(xAxisValue + "", excelRow);
			Cell cell = excelRow.createCell(0);
			getFormatedCellWithValue(cell, xAxisValue, typesArr[xAxisIndex],
					stylingArr[xAxisIndex]);
			
			Object instanceValue = dataRow[yAxisIndex];
			Object instanceColumn = dataRow[instanceColumnIndex];
			if(!currentHeaderValues.contains(instanceColumn + "")) {
				currentHeaderValues.add(instanceColumn + "");
				cell = headerRow.createCell(currentHeaderValues.size()-1);
				getFormatedCellWithValue(cell, instanceColumn, typesArr[instanceColumnIndex], headerCellStyle);
			}
			
			cell = excelRow.createCell(currentHeaderValues.indexOf(instanceColumn + ""));
			getFormatedCellWithValue(cell, instanceValue, typesArr[yAxisIndex],
					stylingArr[yAxisIndex]);
		}

		// now iterate through all the data
		while (task.hasNext()) {
			IHeadersDataRow row = task.next();
			Object[] dataRow = row.getValues();
			Cell cell = null;
			Object xAxisValue = dataRow[xAxisIndex];
			if(xAxisToRow.containsKey(xAxisValue + "")) {
				excelRow = xAxisToRow.get(xAxisValue + "");
			} else {
				excelRow = sheet.createRow(excelRowCounter++);
				xAxisToRow.put(xAxisValue + "", excelRow);
				
				// set the x axis value once
				cell = excelRow.createCell(0);
				getFormatedCellWithValue(cell, xAxisValue, typesArr[xAxisIndex],
						stylingArr[xAxisIndex]);
			}
			
			Object instanceValue = dataRow[yAxisIndex];
			Object instanceColumn = dataRow[instanceColumnIndex];
			if(!currentHeaderValues.contains(instanceColumn + "")) {
				currentHeaderValues.add(instanceColumn + "");
				cell = headerRow.createCell(currentHeaderValues.size()-1);
				getFormatedCellWithValue(cell, instanceColumn, typesArr[instanceColumnIndex], headerCellStyle);
			}
			
			cell = excelRow.createCell(currentHeaderValues.indexOf(instanceColumn + ""));
			getFormatedCellWithValue(cell, instanceValue, typesArr[yAxisIndex],
					stylingArr[yAxisIndex]);
		}

		// add an additional empty row at the end
		// so we have some spacing between
		excelRow = sheet.createRow(excelRowCounter++);

		// Update col and row bounds for sheet
		// this keeps track at the sheet level where to add next task
		sheetMap.put("colIndex", 0);
		// add the offset of rows
		sheetMap.put("rowIndex", endRow + (excelRowCounter - endRow));		

		Map<String, Object> panelMap = (Map<String, Object>) sheetMap.get(panelId);

		// this map defines the start and end for each column
		if (headers != null && headers.length > 0) {
			List<String> headerList = Arrays.asList(headers);
			for (String header : headers) {
				Map<String, Object> columnMap = new HashMap<>();
				columnMap.put("startRow", endRow + 1);
				// -2 for the extra line break between the data
				columnMap.put("endRow", excelRowCounter - 2);
				int headerIndex = headerList.indexOf(header);
				columnMap.put("startCol", headerIndex);
				columnMap.put("endCol", headerIndex);
				panelMap.put(header, columnMap);
			}
		}

		// add an additional empty row at the end
		// so we have some spacing between
		excelRow = sheet.createRow(excelRowCounter++);
		// Update col and row bounds for sheet
		// this keeps track at the sheet level where to add next task
		sheetMap.put("colIndex", 0);
		// add the offset of rows
		sheetMap.put("rowIndex", endRow + (excelRowCounter - endRow));
		// set x axis and y axis data
		panelMap.put("x-axis", Arrays.asList(currentHeaderValues.get(0)));
		panelMap.put("y-axis", currentHeaderValues.subList(1, currentHeaderValues.size()));

		// this map defines the start and end for each column
		for (String header : currentHeaderValues) {
			Map<String, Object> columnMap = new HashMap<>();
			columnMap.put("startRow", endRow + 1);
			// -3 for the extra line break between the data
			columnMap.put("endRow", excelRowCounter - 3);
			int headerIndex = currentHeaderValues.indexOf(header);
			columnMap.put("startCol", headerIndex);
			columnMap.put("endCol", headerIndex);
			panelMap.put(header, columnMap);
		}
	}

	// sets the format and value to cell
	private Cell getFormatedCellWithValue(Cell cell, Object value, SemossDataType dataType, CellStyle cellStyle) {
		if (value == null || value.toString().length() == 0) {
			cell.setCellValue("");
		} else {
			if (dataType == SemossDataType.STRING || value instanceof String) {
				cell.setCellValue(value + "");
			} else if (dataType == SemossDataType.INT || dataType == SemossDataType.DOUBLE) {
				cell.setCellValue(((Number) value).doubleValue());
			} else if (dataType == SemossDataType.DATE) {
				if (value instanceof SemossDate) {
					cell.setCellValue(((SemossDate) value).getDate());
				} else {
					cell.setCellValue(value + "");
				}
			} else if (dataType == SemossDataType.TIMESTAMP) {
				if (value instanceof SemossDate) {
					cell.setCellValue(((SemossDate) value).getDate());
				} else {
					cell.setCellValue(value + "");
				}
			} else if (dataType == SemossDataType.BOOLEAN) {
				cell.setCellValue((boolean) value);
			} else {
				cell.setCellValue(value + "");
			}

			if (cellStyle != null) {
				cell.setCellStyle(cellStyle);
			}
		}
		return cell;
	}
	
	/*
	 * write file to the local for testing purpose
	 * 
	 * public static void WriteToFile(String fileContent, String fileName) { try{
	 * String projectPath = "C:\\workspace"; String tempFile = projectPath +
	 * File.separator+fileName; File file = new File(tempFile); // if file does
	 * exists, then delete and create a new file //write to file with
	 * OutputStreamWriter OutputStream outputStream = new
	 * FileOutputStream(file.getAbsoluteFile()); Writer writer=new
	 * OutputStreamWriter(outputStream); writer.write(fileContent); writer.close();}
	 * catch(Exception e){ System.out.println(e); }
	 * 
	 * }
	 */

	
}
