package prerna.reactor.export;

import java.awt.Color;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.hslf.usermodel.HSLFPictureData;
import org.apache.poi.sl.usermodel.PictureData.PictureType;
import org.apache.poi.sl.usermodel.StrokeStyle;
import org.apache.poi.sl.usermodel.TableCell.BorderEdge;
import org.apache.poi.util.Units;
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
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFChart;
import org.apache.poi.xslf.usermodel.XSLFPictureData;
import org.apache.poi.xslf.usermodel.XSLFPictureShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTable;
import org.apache.poi.xslf.usermodel.XSLFTableCell;
import org.apache.poi.xslf.usermodel.XSLFTableRow;
import org.apache.poi.xslf.usermodel.XSLFTextParagraph;
import org.apache.poi.xslf.usermodel.XSLFTextRun;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLbls;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPieChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTScatterChart;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.InsightFile;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.ChromeDriverUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class ExportToPPTReactor extends AbstractReactor {

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

	Object driver = null;
	ChromeDriverUtility util = null;
	
	public ExportToPPTReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		util = this.insight.getChromeDriver();
		
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "pptx");
		// grab file path to write the file
		String fileLocation =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()));
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		if (fileLocation == null) {
			String insightFolder = this.insight.getInsightFolder();
			File f = new File(insightFolder);
			if(!f.exists()) {
				f.mkdirs();
			}
			fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(true);
		} else {
			fileLocation += DIR_SEPARATOR + exportName;
			insightFile.setDeleteOnInsightClose(false);
		}
		insightFile.setFilePath(fileLocation);
		
		Map<String, InsightPanel> panelMap = this.insight.getInsightPanels();
		Map<String, InsightSheet> sheetMap = this.insight.getInsightSheets();
		// create Powerpoint slideshow
		XMLSlideShow slideshow = new XMLSlideShow();
		// Map sheet to the list of panels it contains so we can group panels in
		// the same sheet together
		HashMap<String, List<InsightPanel>> sheetToPanelMap = new HashMap<String, List<InsightPanel>>();
		for (String panelId : panelMap.keySet()) {
			InsightPanel panel = panelMap.get(panelId);
			String sheetId = panel.getSheetId();
			if (!sheetToPanelMap.containsKey(sheetId)) {
				sheetToPanelMap.put(sheetId, new ArrayList<InsightPanel>());
			}
			List<InsightPanel> panelList = (List<InsightPanel>) sheetToPanelMap.get(sheetId);
			panelList.add(panel);
		}
		// Process each panel and try to plot each chart
		for (String sheetId : sheetToPanelMap.keySet()) {
			List<InsightPanel> panelList = (List<InsightPanel>) sheetToPanelMap.get(sheetId);
			for (InsightPanel panel : panelList) {
				// for each panel get the task and task options
				SelectQueryStruct qs = panel.getLastQs();
				TaskOptions taskOptions = panel.getLastTaskOptions();
				if(qs == null || taskOptions == null) {
					continue;
				}
				IQuerySelector firstSelector = qs.getSelectors().get(0);
				if (firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
					qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
				} else {
					qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
				}
				ITask task = InsightUtility.constructTaskFromQs(this.insight, qs);
				task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
				task.setTaskOptions(taskOptions);
				
				processTask(user, slideshow, task, panel);
			}
		}
		
		// Add Semoss Logo to bottom right corner of each slide
		addLogo(slideshow);
		
		writeToFile(slideshow, fileLocation);
		
		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the ppt file"));
		return retNoun;
	}

	private void addLogo(XMLSlideShow slideshow) {
		String semossLogoPath = DIHelper.getInstance().getProperty("EXPORT_SEMOSS_LOGO");
		if (semossLogoPath != null) {
			File logo = new File(semossLogoPath);
			if (logo.exists()) {
				byte[] picture = null;
				try {
					picture = IOUtils.toByteArray(new FileInputStream(semossLogoPath));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				XSLFPictureData pictureData = slideshow.addPicture(picture, PictureType.PNG);
				Rectangle lowerRightCornerBounds = createStandardPowerPointSemossLogoBounds();
				for (XSLFSlide slide : slideshow.getSlides()) {
					XSLFPictureShape pictureShape = slide.createPicture(pictureData);
					pictureShape.setAnchor(lowerRightCornerBounds);
				}
			}
		}		
	}

	private void processTask(User user, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		String panelId = panel.getPanelId();
		TaskOptions tOptions = task.getTaskOptions();
		Map<String, Object> options = tOptions.getOptions();
		// Insert chart if supported
		try {
			String plotType = (String) tOptions.getLayout(panelId);
			if (plotType.equals("Line")) {
				insertLineChart(slideshow, task, options, panel);
			} else if (plotType.equals("Scatter")) {
				insertScatterChart(options, slideshow, task, panel);
			} else if (plotType.equals("Area")) {
				insertAreaChart(options, slideshow, task, panel);
			} else if (plotType.equals("Column")) {
				insertBarChart(options, slideshow, task, panel);
			} else if (plotType.equals("Pie")) {
				insertPieChart(options, slideshow, task, panel);
			} else if (plotType.equals("Radar")) {
				insertRadarChart(options, slideshow, task, panel);
			} else if (plotType.equals("Grid")) {
				insertGridChart(options, slideshow, task, panel);
			} else if(!plotType.equals("PivotTable")) { 
				insertImage(user, options, slideshow, task, panel);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			if(driver != null && driver instanceof ChromeDriver) {
				
				((ChromeDriver)driver).quit();
			}
			driver = null;
		}
	}

	private void insertLineChart(XMLSlideShow slideshow, ITask task, Map<String, Object> options, InsightPanel panel) {
		// Grab data for chart
		PPTDataHandler dataHandler = new PPTDataHandler();
		dataHandler.setData(task);

		// retrieve ornaments
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.editGrid.x") + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.editGrid.y") + "");
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.displayValues") + "");
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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (List<String>) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		// Add Y Axis Title
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		// Add X Axis Title
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}
		XDDFLineChartData data = (XDDFLineChartData) chart.createData(ChartTypes.LINE, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSourceByType(xColumnName,0);

		// Add in y vals
		int yCounter = 0;
		for (int i = 0; i < yColumnNames.size(); i++) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnNames.get(i));
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFLineChartData.Series chartSeries = (XDDFLineChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnNames.get(i), null);
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

		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
		}
		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			POIExportUtility.displayValues(ChartTypes.LINE, chart);
		}

		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
	}

	private void insertScatterChart(Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		// Grab data for chart
		PPTDataHandler dataHandler = new PPTDataHandler();
		dataHandler.setData(task);

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


		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		String xColumnName = ((List<String>) alignmentMap.get("x")).get(0).toString();
		List<String> yColumnNames = (List<String>) alignmentMap.get("y");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
//		XDDFChartLegend legend = chart.getOrAddLegend();
//		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFValueAxis bottomAxis = chart.createValueAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}
		XDDFScatterChartData data = (XDDFScatterChartData) chart.createData(ChartTypes.SCATTER, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);

		// Add in y vals
		for (int i = 0; i < yColumnNames.size(); i++) {
			String yColumnName = yColumnNames.get(i);
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFScatterChartData.Series chartSeries = (XDDFScatterChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			// Standardize markers
			chartSeries.setSmooth(false);
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			CTScatterChart scatterSeries = chart.getCTChart().getPlotArea().getScatterChartArray(0);
			scatterSeries.getSerArray(i).addNewSpPr().addNewLn().addNewNoFill();
			scatterSeries.addNewVaryColors().setVal(false);
		}

		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
		}
		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			POIExportUtility.displayValues(ChartTypes.SCATTER, chart);
		}

		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
	}

	private void insertBarChart(Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		// Grab data for chart
		PPTDataHandler dataHandler = new PPTDataHandler();
		dataHandler.setData(task);

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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (List<String>) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}
		XDDFBarChartData data = (XDDFBarChartData) chart.createData(ChartTypes.BAR, bottomAxis, leftAxis);
		leftAxis.setCrossBetween(AxisCrossBetween.BETWEEN);
		
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

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSourceByType(xColumnName,0);

		// Add in y vals
		int yCounter = 0;
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFBarChartData.Series chartSeries = (XDDFBarChartData.Series) data.addSeries(xs, ys);
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(POIExportUtility.hex2Rgb(colorArray[yCounter%colorArray.length])));
            chartSeries.setFillProperties(fillProperties);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			yCounter++;
		}

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

		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
	}

	private void insertAreaChart(Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		// Grab data for chart
		PPTDataHandler dataHandler = new PPTDataHandler();
		dataHandler.setData(task);

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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (List<String>) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}
		XDDFAreaChartData data = (XDDFAreaChartData) chart.createData(ChartTypes.AREA, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);

		// Add in y vals
		int yCounter = 0;
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFAreaChartData.Series chartSeries = (XDDFAreaChartData.Series) data.addSeries(xs, ys);
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(POIExportUtility.hex2Rgb(colorArray[yCounter%colorArray.length])));
			chartSeries.setFillProperties(fillProperties);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
			yCounter++;
		}

		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
		}
		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			POIExportUtility.displayValues(ChartTypes.AREA, chart);
		}

		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
	}

	private void insertPieChart(Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		// Grab data for chart
		PPTDataHandler dataHandler = new PPTDataHandler();
		dataHandler.setData(task);

		// retrieve ornaments
		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
		String displayValuesPosition = panel.getMapInput(panel.getOrnaments(), "tools.shared.customizePieLabel.position") + "";
		List<String> pieCustomDisplayValues = (List<String>) panel.getMapInput(panel.getOrnaments(), "tools.shared.customizePieLabel.dimension");

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (List<String>) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFPieChartData data = (XDDFPieChartData) chart.createData(ChartTypes.PIE, null, null);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);
		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFPieChartData.Series chartSeries = (XDDFPieChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
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

		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
	}

	private void insertRadarChart(Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		// Grab data for chart
		PPTDataHandler dataHandler = new PPTDataHandler();
		dataHandler.setData(task);

		// retrieve ornaments
		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
        String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
 		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
 		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
 		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
 		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
 		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";


		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (List<String>) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (List<String>) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		if(showYAxisTitle) {
			if(!yAxisTitleName.isEmpty() && !yAxisTitleName.equals("null")) {
				leftAxis.setTitle(yAxisTitleName);
			} else {
				leftAxis.setTitle(String.join(", ", yColumnNames).replaceAll("_", " "));
			}
		}
		if(showXAxisTitle) {
			if(!xAxisTitleName.isEmpty() && !xAxisTitleName.equals("null")) {
				bottomAxis.setTitle(xAxisTitleName);
			} else {
				bottomAxis.setTitle(xColumnName.replaceAll("_", " "));
			}
		}
		XDDFRadarChartData data = (XDDFRadarChartData) chart.createData(ChartTypes.RADAR, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFRadarChartData.Series chartSeries = (XDDFRadarChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName.replaceAll("_", " "), null);
		}

		Object chartTitle = panel.getMapInput(panel.getOrnaments(), CHART_TITLE);
		if(chartTitle != null) {
			chart.setTitleText(chartTitle + "");
		}
		chart.plot(data);
		
		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
	}
	
	private void insertGridChart(Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {

//		// retrieve ornaments
//		Boolean toggleStack = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.toggleStack") + "");
//		Boolean flipAxis = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), "tools.shared.rotateAxis") + "");
//		Boolean gridOnX = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_X) + "");
//		Boolean gridOnY = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), GRID_ON_Y) + "");
//		Boolean displayValues = Boolean.parseBoolean(panel.getMapInput(panel.getOrnaments(), DISPLAY_VALUES) + "");
//		String displayValuesPosition = panel.getMapInput(panel.getOrnaments(), "tools.shared.customizeBarLabel.position") + "";
//		String yAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_Y_AXIS_TITLE) + "";
//		String xAxisFlag = panel.getMapInput(panel.getOrnaments(), SHOW_X_AXIS_TITLE) + "";
//		Boolean showYAxisTitle = !panel.getOrnaments().isEmpty() && !yAxisFlag.isEmpty() && !yAxisFlag.equals("null") ? Boolean.parseBoolean(yAxisFlag) : true;
//		Boolean showXAxisTitle = !panel.getOrnaments().isEmpty() && !xAxisFlag.isEmpty() && !xAxisFlag.equals("null") ? Boolean.parseBoolean(xAxisFlag) : true;
//		String yAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), Y_AXIS_TITLE_NAME) + "" : "";
//		String xAxisTitleName = !panel.getOrnaments().isEmpty() ? panel.getMapInput(panel.getOrnaments(), X_AXIS_TITLE_NAME) + "" : "";

		XSLFSlide slide = slideshow.createSlide();
		XSLFTable table = slide.createTable();
	    table.setAnchor(new Rectangle(50, 50, 800, 800));
	    
	    int maxRows = 50;
	    int counter = 0;
		boolean first = true;
		MAX_TABLE_SIZE : while(task.hasNext()) {
			if(counter++ > maxRows) {
				try {
					task.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break MAX_TABLE_SIZE;
			}
			IHeadersDataRow headerRow = task.next();
			Object[] dataValues = headerRow.getValues();

			// add the headers in first iteration
			if(first) {
				String[] headers = headerRow.getHeaders();
				XSLFTableRow tableHeader = table.addRow();
				for(int i = 0; i < headers.length; i++) {
					XSLFTableCell cell = tableHeader.addCell();
					cell.setFillColor(new Color(52, 152, 219));
					XSLFTextParagraph paragraph = cell.addNewTextParagraph();
					XSLFTextRun textRun = paragraph.addNewTextRun();
					textRun.setText(headers[i]);
					textRun.setFontColor(Color.WHITE);
					textRun.setFontSize(14.0);
					// some styling
					cell.setBorderColor(BorderEdge.top, Color.BLACK);
					cell.setBorderCompound(BorderEdge.top, StrokeStyle.LineCompound.SINGLE);
					cell.setBorderColor(BorderEdge.bottom, Color.BLACK);
					cell.setBorderCompound(BorderEdge.bottom, StrokeStyle.LineCompound.SINGLE);
					cell.setBorderColor(BorderEdge.left, Color.BLACK);
					cell.setBorderCompound(BorderEdge.left, StrokeStyle.LineCompound.SINGLE);
					cell.setBorderColor(BorderEdge.right, Color.BLACK);
					cell.setBorderCompound(BorderEdge.right, StrokeStyle.LineCompound.SINGLE);
				}
				first = false;
			}

			XSLFTableRow tableRow = table.addRow();
			for(int i = 0; i < dataValues.length; i++) {
				XSLFTableCell cell = tableRow.addCell();
				cell.setFillColor(Color.WHITE);
				XSLFTextParagraph paragraph = cell.addNewTextParagraph();
				XSLFTextRun textRun = paragraph.addNewTextRun();
				textRun.setText(dataValues[i] + "");
				textRun.setFontSize(12.0);

				// some styling
				cell.setBorderColor(BorderEdge.top, Color.BLACK);
				cell.setBorderCompound(BorderEdge.top, StrokeStyle.LineCompound.SINGLE);
				cell.setBorderColor(BorderEdge.bottom, Color.BLACK);
				cell.setBorderCompound(BorderEdge.bottom, StrokeStyle.LineCompound.SINGLE);
				cell.setBorderColor(BorderEdge.left, Color.BLACK);
				cell.setBorderCompound(BorderEdge.left, StrokeStyle.LineCompound.SINGLE);
				cell.setBorderColor(BorderEdge.right, Color.BLACK);
				cell.setBorderCompound(BorderEdge.right, StrokeStyle.LineCompound.SINGLE);
			}
		}
	}


	private Rectangle createStandardPowerPointChartBounds() {
		double leftOffsetInches = 0.05;
		double rightOffsetInches = 0.05;
		double topOffsetInches = 0.05;
		// Leave space for Semoss logo in the bottom corner
		double bottomOffsetInches = 0.2;
		double slideWidthInches = 10;
		double slideHeightInches = 7.5;
		
		double boundWidthInches = slideWidthInches - leftOffsetInches - rightOffsetInches;
		double boundHeightInches = slideHeightInches - topOffsetInches - bottomOffsetInches;
		double boundWidthOffsetInches = leftOffsetInches;
		double boundHeightOffsetinches = topOffsetInches;
		
		double emuPerInch = Units.EMU_PER_CENTIMETER * 2.54;
		int boundWidthEMU = (int) (boundWidthInches * emuPerInch);
		int boundheightEMU = (int) (boundHeightInches * emuPerInch);
		int boundWidthOffsetEMU = (int) (boundWidthOffsetInches * emuPerInch);
		int boundHeightOffsetEMU = (int) (boundHeightOffsetinches * emuPerInch);
		
		Rectangle bounds = new java.awt.Rectangle(boundWidthOffsetEMU, boundHeightOffsetEMU, boundWidthEMU, boundheightEMU);
		return bounds;
	}
	
	private Rectangle createStandardPowerPointSemossLogoBounds() {
		// PNG Powered by Semoss logo is 1478 x 214 pixels
		// Let's put the image in the bottom right corner and maintain aspect ratio
		// Point DPI = 72 = 1 inch
		double dpiPerInch = (double) Units.POINT_DPI;
		double slideWidthInches = 10;
		double slideHeightInches = 7.5;
		double imageHeightInches = .2;
		double imageWidthInches = imageHeightInches * (1478.0 / 214.0);
		
		double widthOffsetInches = slideWidthInches - imageWidthInches;
		double heightOffsetInches = slideHeightInches - imageHeightInches;
		
		double widthOffsetDPI = widthOffsetInches * dpiPerInch;
		double heightOffsetDPI = heightOffsetInches * dpiPerInch;
		double imageHeightDPI = imageHeightInches * dpiPerInch;
		double imageWidthDPI = imageWidthInches * dpiPerInch;
		
		// Cast coordinates to int so that they can be ingested by Rectangle
		int widthOffsetDPIInt = (int) widthOffsetDPI;
		int heightOffsetDPIInt = (int) heightOffsetDPI;
		int imageHeightDPIInt = (int) imageHeightDPI;
		int imageWidthDPIInt = (int) imageWidthDPI;
		Rectangle bounds = new java.awt.Rectangle(widthOffsetDPIInt, heightOffsetDPIInt, imageWidthDPIInt, imageHeightDPIInt);
		
		return bounds;
	}

	private void writeToFile(XMLSlideShow slideshow, String path) {
		try {
			OutputStream out = new FileOutputStream(path);
			slideshow.write(out);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * 
	 * @param options
	 * @param slideshow
	 * @param task
	 * @param panel
	 */
	private void insertImage(User user, Map<String, Object> options, XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		String baseUrl = this.insight.getBaseURL();
		String imageUrl = this.insight.getLiveURL();
		String panelAppender = "&panel=" + panel.getPanelId();
		String sheetAppender = "&sheet=" + panel.getSheetId();
		String sessionId = ThreadStore.getSessionId();
		String exportName = AbstractExportTxtReactor.getExportFileName(user, Utility.getRandomString(8), "png");
		String imageLocation = this.insight.getInsightFolder() + DIR_SEPARATOR + exportName;
		if(driver == null) {
			driver = util.makeChromeDriver(baseUrl, imageUrl + sheetAppender + panelAppender, 800, 600);
		}
		util.captureImagePersistent(driver, baseUrl, imageUrl + sheetAppender + panelAppender, imageLocation, sessionId, 800);
		
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(imageLocation);
			byte[] bytes = IOUtils.toByteArray(inputStream);
			inputStream.close();
			FileUtils.forceDelete(new File(imageLocation));
			XSLFPictureData hslfPictureData = slideshow.addPicture(bytes, HSLFPictureData.PictureType.PNG);
			XSLFSlide blankSlide = slideshow.createSlide();
			XSLFPictureShape pic = blankSlide.createPicture(hslfPictureData);
			pic.setAnchor(new Rectangle(0, 0, 800, 600));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(driver != null && driver instanceof ChromeDriver) {
				((ChromeDriver)driver).quit();
			}
		}
	}

}
