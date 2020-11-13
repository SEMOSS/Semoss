package prerna.sablecc2.reactor.export;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.poi.sl.usermodel.PictureData.PictureType;
import org.apache.poi.util.Units;
import org.apache.poi.xddf.usermodel.PresetColor;
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
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLbls;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTScatterChart;

import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
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
import prerna.util.DIHelper;

public class ExportToPPTReactor extends AbstractReactor {

	private static final String GRID_ON_X = "tools.shared.editGrid.x";
	private static final String GRID_ON_Y = "tools.shared.editGrid.y";
	private static final String DISPLAY_VALUES = "tools.shared.displayValues";

	public ExportToPPTReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		NounMetadata retNoun = null;
		// get a random file name
		String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
		String exportName = AbstractExportTxtReactor.getExportFileName(prefixName, "pptx");
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
				TaskOptions taskOptions = panel.getTaskOptions();
				if(qs == null || taskOptions == null) {
					continue;
				}
				IQuerySelector firstSelector = qs.getSelectors().get(0);
				if (firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
					qs.addOrderBy(firstSelector.getQueryStructName(), "ASC");
				} else {
					qs.addOrderBy(firstSelector.getAlias(), null, "ASC");
				}
				ITask task = new BasicIteratorTask(qs);
				task.setLogger(this.getLogger(ExportToExcelReactor.class.getName()));
				task.setTaskOptions(taskOptions);
				
				processTask(slideshow, task, panel);
			}
		}
		
		// Add Semoss Logo to bottom right corner of each slide
		addLogo(slideshow);
		
		writeToFile(slideshow, fileLocation);
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

	private void processTask(XMLSlideShow slideshow, ITask task, InsightPanel panel) {
		String panelId = panel.getPanelId();
		TaskOptions tOptions = task.getTaskOptions();
		Map<String, Object> options = tOptions.getOptions();
		// Insert chart if supported
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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (Vector) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		XDDFLineChartData data = (XDDFLineChartData) chart.createData(ChartTypes.LINE, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSourceByType(xColumnName,0);

		// Add in y vals
		for (int i = 0; i < yColumnNames.size(); i++) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnNames.get(i));
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFLineChartData.Series chartSeries = (XDDFLineChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnNames.get(i), null);
			// Standardize markers
			XDDFSolidFillProperties fillProperties = new XDDFSolidFillProperties();
			fillProperties.setColor(XDDFColor.from(PresetColor.ROYAL_BLUE));
			chartSeries.setMarkerStyle(MarkerStyle.CIRCLE);
			XDDFShapeProperties propertiesMarker = new XDDFShapeProperties();
			propertiesMarker.setFillProperties(fillProperties);
			chart.getCTChart().getPlotArea().getLineChartArray(0).getSerArray(i).getMarker().addNewSpPr()
					.set(propertiesMarker.getXmlObject());
			// Standardize line
			XDDFLineProperties lineProperties = new XDDFLineProperties();
			lineProperties.setFillProperties(fillProperties);
			chartSeries.setLineProperties(lineProperties);
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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		String xColumnName = ((Vector) alignmentMap.get("x")).firstElement().toString();
		List<String> yColumnNames = (Vector) alignmentMap.get("y");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
//		XDDFChartLegend legend = chart.getOrAddLegend();
//		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFValueAxis bottomAxis = chart.createValueAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		XDDFScatterChartData data = (XDDFScatterChartData) chart.createData(ChartTypes.SCATTER, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);

		// Add in y vals
		for (int i = 0; i < yColumnNames.size(); i++) {
			String yColumnName = yColumnNames.get(i);
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFScatterChartData.Series chartSeries = (XDDFScatterChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
			// Standardize markers
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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (Vector) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
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
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFBarChartData.Series chartSeries = (XDDFBarChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
		}

		chart.plot(data);
		Rectangle bounds = createStandardPowerPointChartBounds();

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			CTDLbls dLbls = POIExportUtility.displayValues(ChartTypes.BAR, chart);
			POIExportUtility.positionDisplayValues(ChartTypes.BAR, dLbls, displayValuesPosition);
		}

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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (Vector) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		XDDFAreaChartData data = (XDDFAreaChartData) chart.createData(ChartTypes.AREA, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFAreaChartData.Series chartSeries = (XDDFAreaChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (Vector) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) alignmentMap.get("value");

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
			chartSeries.setTitle(yColumnName, null);
			chartSeries.setExplosion((long) 0);
		}

		chart.plot(data);

		// if true, display data labels on chart
		if (displayValues.booleanValue()) {
			CTDLbls dLbls = POIExportUtility.displayValues(ChartTypes.PIE, chart);
			POIExportUtility.positionDisplayValues(ChartTypes.PIE, dLbls, displayValuesPosition);
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

		// Parse input data
		// options is guaranteed to be of length 1 so just grab the only value
		Map<String, Object> optionsSubMap = (Map<String, Object>) options.values().toArray()[0];
		Map<String, Object> alignmentMap = (Map<String, Object>) optionsSubMap.get("alignment");
		List<String> label = (Vector) alignmentMap.get("label");
		String xColumnName = label.get(0);
		List<String> yColumnNames = (Vector) alignmentMap.get("value");

		XSLFSlide slide = slideshow.createSlide();
		XSLFChart chart = slideshow.createChart(slide);
		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		POIExportUtility.addGridLines(gridOnX, gridOnY, chart);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);
		XDDFRadarChartData data = (XDDFRadarChartData) chart.createData(ChartTypes.RADAR, bottomAxis, leftAxis);

		// Add in x vals
		XDDFDataSource<?> xs = dataHandler.getColumnAsXDDFDataSource(xColumnName);

		// Add in y vals
		for (String yColumnName : yColumnNames) {
			Number[] yNumberArray = dataHandler.getColumnAsNumberArray(yColumnName);
			XDDFNumericalDataSource<? extends Number> ys = XDDFDataSourcesFactory.fromArray(yNumberArray);
			XDDFRadarChartData.Series chartSeries = (XDDFRadarChartData.Series) data.addSeries(xs, ys);
			chartSeries.setTitle(yColumnName, null);
		}

		chart.plot(data);
		Rectangle bounds = createStandardPowerPointChartBounds();
		slide.addChart(chart, bounds);
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

}
