package prerna.sablecc2.reactor.export;

import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.XDDFChart;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFDataFormat;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTAreaChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTBarChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLblPos;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLbls;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTLineChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPieChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPlotArea;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTScatterChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.STDLblPos;

public class ExportUtility {

	public static void addGridLines(Boolean gridOnX, Boolean gridOnY, XDDFChart chart) {
		CTPlotArea plotArea = null;
		plotArea = chart.getCTChart().getPlotArea();

		// if true add grid lines on x/y axis
		if (gridOnX.booleanValue() && gridOnY.booleanValue()) {
			plotArea.getCatAxArray()[0].addNewMajorGridlines();
			plotArea.getValAxArray()[0].addNewMajorGridlines();
		} else if (gridOnX.booleanValue()) {
			plotArea.getCatAxArray()[0].addNewMajorGridlines();
		} else if (gridOnY.booleanValue()) {
			plotArea.getValAxArray()[0].addNewMajorGridlines();
		}
	}

	public static CTDLbls displayValues(ChartTypes chartType, XDDFChart chart) {
		CTDLbls dLbls = null;
		switch (chartType) {
			case LINE:
				CTLineChart ctLineChart = chart.getCTChart().getPlotArea().getLineChartArray(0);
				ctLineChart.addNewDLbls();
				dLbls = ctLineChart.getDLbls();
				break;
			case SCATTER:
				CTScatterChart ctScatterChart = chart.getCTChart().getPlotArea().getScatterChartArray(0);
				ctScatterChart.addNewDLbls();
				dLbls = ctScatterChart.getDLbls();
				break;
			case BAR:
				CTBarChart ctBarChart = chart.getCTChart().getPlotArea().getBarChartArray(0);
				ctBarChart.addNewDLbls();
				dLbls = ctBarChart.getDLbls();
				break;
			case AREA:
				CTAreaChart ctAreaChart = chart.getCTChart().getPlotArea().getAreaChartArray(0);
				ctAreaChart.addNewDLbls();
				dLbls = ctAreaChart.getDLbls();
				break;
			case PIE:
				CTPieChart ctPieChart = chart.getCTChart().getPlotArea().getPieChartArray(0);
				ctPieChart.addNewDLbls();
				dLbls = ctPieChart.getDLbls();
				break;
			default:
				break;
		}

		if (dLbls != null) {
			dLbls.addNewShowBubbleSize().setVal(false);
			dLbls.addNewShowLegendKey().setVal(false);
			dLbls.addNewShowCatName().setVal(false);
			dLbls.addNewShowSerName().setVal(false);
			dLbls.addNewShowPercent().setVal(false);
			dLbls.addNewShowVal().setVal(true);
		}

		return dLbls;
	}

	public static void positionDisplayValues(ChartTypes chartType, CTDLbls dLbls, String displayValuesPosition) {
		// if available, set positioning of data labels
		if (!displayValuesPosition.equals("{}")) {
			CTDLblPos ctdLblPos = null;
			ctdLblPos = CTDLblPos.Factory.newInstance();

			if (chartType.equals(ChartTypes.BAR)) {
				if (displayValuesPosition.equalsIgnoreCase("inside")
						|| displayValuesPosition.equalsIgnoreCase("insideLeft")
						|| displayValuesPosition.equalsIgnoreCase("insideRight")) {
					ctdLblPos.setVal(STDLblPos.CTR);
				} else if (displayValuesPosition.equalsIgnoreCase("insideBottom")
						|| displayValuesPosition.equalsIgnoreCase("bottom")) {
					ctdLblPos.setVal(STDLblPos.IN_BASE);
				} else if (displayValuesPosition.equalsIgnoreCase("insideTop")) {
					ctdLblPos.setVal(STDLblPos.IN_END);
				} else if (displayValuesPosition.equalsIgnoreCase("top")) {
					ctdLblPos.setVal(STDLblPos.OUT_END);
				}
			} else if (chartType.equals(ChartTypes.PIE)) {
				if (displayValuesPosition.equalsIgnoreCase("inside")) {
					ctdLblPos.setVal(STDLblPos.CTR);
				} else if (displayValuesPosition.equalsIgnoreCase("outside")) {
					ctdLblPos.setVal(STDLblPos.OUT_END);
				}
			}

			dLbls.setDLblPos(ctdLblPos);
		}
	}

	public static XSSFCellStyle getCurrentStyle(XSSFWorkbook workbook, String additionalDataType) {
		XSSFCellStyle curStyle = workbook.createCellStyle();
		XSSFDataFormat df = workbook.createDataFormat();

		if (additionalDataType.contains("currency")) {
			curStyle.setDataFormat(df.getFormat("$#,#0.00"));
		}

		return curStyle;
	}
}
