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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataConsolidateFunction;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.XDDFChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTAreaChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTBarChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLblPos;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTDLbls;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTLineChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPieChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPlotArea;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTScatterChart;
import org.openxmlformats.schemas.drawingml.x2006.chart.STDLblPos;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import prerna.util.Constants;

public class POIExportUtility {

	private static final Logger classLogger = LogManager.getLogger(POIExportUtility.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private POIExportUtility() {

	}

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

	/**
	 * Get the cell style from either the panel formatting or the metamodel
	 * formatting
	 * 
	 * @param workbook
	 * @param metamodelAdditionalDataType
	 * @param panelFormatting
	 * @return
	 */
	public static CellStyle getCurrentStyle(Workbook workbook, String metamodelAdditionalDataType,
			Map<String, String> panelFormatting) {
		CellStyle curStyle = workbook.createCellStyle();
		DataFormat df = workbook.createDataFormat();
		// panel formatting goes first
		// then formatting at the additional type level
		String format = null;

		String prepend = null;
		String append = null;
		Object round = null;

		if (panelFormatting != null && panelFormatting.containsKey("type")) {
			boolean isDateFormatting = false;
			String panelAdditionalDataType = panelFormatting.get("type");
			// if there is a date type
			// we will use that as the additionalDataType
			if (panelAdditionalDataType.equalsIgnoreCase("Default")) {
				// for date formatting, it is stored in a bit different
				String dateAdditionalDataType = panelFormatting.get("dateType");
				if (dateAdditionalDataType != null && !dateAdditionalDataType.isEmpty()) {
					panelAdditionalDataType = dateAdditionalDataType;
					isDateFormatting = true;
				}
			}
			// aside from dates, we can lowercase the additional data type
			if (!isDateFormatting) {
				panelAdditionalDataType = panelAdditionalDataType.toLowerCase();
			}

			// grab the format from the additional data type
			format = getBaseExcelFormat(panelAdditionalDataType);

			prepend = panelFormatting.get("prepend");
			append = panelFormatting.get("append");
			round = panelFormatting.get("round");

			boolean hasPrepend = prepend != null && !prepend.isEmpty();
			boolean hasAppend = append != null && !append.isEmpty();

			// special cases
			try {
				if (format == null && round != null) {
					String rep = Strings.repeat("0", Integer.parseInt(round + ""));
					if (rep != null && !rep.isEmpty()) {
						format = "#." + rep;
					} else {
						format = "#";
					}
				}
			} catch (Exception e) {
				// ignore
			}
			if (format == null && (hasPrepend || hasAppend)) {
				format = "General";
			}

			// apply prepend/append
			if (prepend != null && !prepend.isEmpty()) {
				format = "\"" + prepend + "\"" + format;
			}
			if (append != null && !append.isEmpty()) {
				format = format + "\"" + append + "\"";
			}

			// return the panel format if not null
			if (format != null) {
				curStyle.setDataFormat(df.getFormat(format));
				return curStyle;
			}
		}

		// now try the metamodel additional type
		if (metamodelAdditionalDataType != null) {
			// first check if this is a map
			Map<String, String> customDataFormat = null;
			try {
				customDataFormat = GSON.fromJson(metamodelAdditionalDataType, Map.class);
				if (customDataFormat != null && !customDataFormat.isEmpty()) {
					metamodelAdditionalDataType = customDataFormat.get("type").toLowerCase();
				}
			} catch (JsonSyntaxException e) {
				// ignore
			} catch (Exception e) {
				classLogger.error("Failed to parse metamodel additional data type formatting {}",
						metamodelAdditionalDataType, e);
			}

			format = getBaseExcelFormat(metamodelAdditionalDataType);
			if (customDataFormat != null) {
				prepend = customDataFormat.get("prepend");
				append = customDataFormat.get("append");
				round = customDataFormat.get("round");
			}

			boolean hasPrepend = prepend != null && !prepend.isEmpty();
			boolean hasAppend = append != null && !append.isEmpty();

			// special cases
			try {
				if (format == null && round != null) {
					String rep = Strings.repeat("0", Integer.parseInt(round + ""));
					if (rep != null && !rep.isEmpty()) {
						format = "#." + rep;
					} else {
						format = "#";
					}
				}
			} catch (Exception e) {
				// ignore
			}
			if (format == null && (hasPrepend || hasAppend)) {
				format = "General";
			}

			// apply prepend/append
			if (prepend != null && !prepend.isEmpty()) {
				format = "\"" + prepend + "\"" + format;
			}
			if (append != null && !append.isEmpty()) {
				format = format + "\"" + append + "\"";
			}

			if (format != null) {
				curStyle.setDataFormat(df.getFormat(format));
				return curStyle;
			}
		}

		// nothing worked
		return null;
	}

	/**
	 * Convert hex color code into byte array
	 * 
	 * @param colorStr
	 * @return
	 */
	public static byte[] hex2Rgb(String colorStr) {
		int r = Integer.valueOf(colorStr.substring(1, 3), 16);
		int g = Integer.valueOf(colorStr.substring(3, 5), 16);
		int b = Integer.valueOf(colorStr.substring(5, 7), 16);
		return new byte[] { (byte) r, (byte) g, (byte) b };
	}

	/**
	 * Get hex color code array
	 * 
	 * @param colorName
	 * @param customColors
	 * @param colorObject
	 * @return colorHexArray
	 */
	public static String[] getHexColorCode(String colorName, Object customColors, Object colorObject) {
		if (colorObject == null) {
			return Constants.COLOR_SEMOSS;
		}

		String[] colorHexArray = Constants.COLOR_SEMOSS;
		if (!(colorObject instanceof HashMap)) {
			Vector colorsArray = (Vector) colorObject;
			Object[] objectArray = colorsArray.toArray();
			colorHexArray = Arrays.copyOf(objectArray, objectArray.length, String[].class);
		}
		if (customColors instanceof Map) {
			Map cc = (Map) customColors;
			if (cc.containsKey(colorName)) {
				Object[] result = cc.values().toArray();
				String[] r = Arrays.copyOf(result, result.length, String[].class);
				String dd = result.toString();
				String ee = (String) result[0];
				ee = ee.replace("[", "").replace("]", "");
				ee = ee.replace("\"", "");
				colorHexArray = ee.split(",");
			}
		}
		return colorHexArray;
	}

	/**
	 * Convert to get the base format
	 * 
	 * @param additionalDataType
	 * @return
	 */
	private static String getBaseExcelFormat(String additionalDataType) {
		String format;
		switch (additionalDataType) {
		case "int_comma":
			format = "#,###";
			break;
		case "int_currency":
			format = "$#";
			break;
		case "int_currency_comma":
			format = "$#,###";
			break;
		case "int_percent":
			format = "#%";
			break;
		case "double_round1":
			format = "0.0";
			break;
		case "double_round2":
			format = "0.00";
			break;
		case "double_round3":
			format = "0.000";
			break;
		case "double_comma_round1":
			format = "#,###.0";
			break;
		case "double_comma_round2":
			format = "#,###.00";
			break;
		case "double_currency_round2":
			format = "$#,###.00";
			break;
		case "double_percent_round1":
			format = "0.0\"%\"";
			break;
		case "double_percent_round2":
			format = "0.00\"%\"";
			break;
		case "thousand":
			format = "0.00,\"K\"";
			break;
		case "million":
			format = "0.00,,\"M\"";
			break;
		case "billion":
			format = "0.00,,,\"B\"";
			break;
		case "trillion":
			format = "0.00,,,,\"T\"";
			break;
		case "accounting":
			format = "_($* #,##0.0_);_($* (#,##0.0);_($* \"-\"?_);_(@_)";
			break;
		case "scientific":
			format = "0.00E+00";
			break;
		case "percentage":
			format = "#%";
			break;
		case "MMMMM d, yyyy":
			format = "MMMM d, yyyy";
			break;
		case "EEEEE, MMMMM d, yyyy":
			format = "dddd, MMMM d, yyyy";
			break;
		case "M/d/yy hh:mm a":
			format = "M/dd/yy hh:mm AM/PM";
			break;
		case "M/d/yy hh:mm":
			format = "M/d/yy hh:mm";
			break;
		case "M/d/yy HH:mm":
			format = "M/dd/yy HH:mm";
			break;
		case "M/d/yy HH:mm:ss":
			format = "M/d/yy HH:mm:ss";
			break;
		case "M/d/yyyy":
			format = "M/dd/yyyy";
			break;
		case "MM/dd/yyyy":
			format = "MM/dd/yyyy";
			break;
		case "yyyy-MM-dd":
			format = "yyyy-MM-dd";
			break;
		case "MM/dd":
			format = "MM/dd";
			break;
		case "dd-MMM":
			format = "dd-MMM";
			break;
		case "dd-MMM-yy":
			format = "dd-MMM-yy";
			break;
		case "dd-MMM-yyyy":
			format = "dd-MMM-yyyy";
			break;
		case "MMM-yy":
			format = "MMM-yy";
			break;
		default:
			format = null;
		}

		return format;
	}

	/**
	 * 
	 * @param functionName
	 * @return
	 */
	public static DataConsolidateFunction convertToExcelFunction(String functionName) {
		DataConsolidateFunction retFunction = null;

		switch (functionName.toUpperCase()) {
		case "SUM":
			retFunction = DataConsolidateFunction.SUM;
			break;
		case "COUNT":
			retFunction = DataConsolidateFunction.COUNT;
			break;
		case "MIN":
			retFunction = DataConsolidateFunction.MIN;
			break;
		case "MAX":
			retFunction = DataConsolidateFunction.MAX;
			break;
		case "MEDIAN": // cheating here
			retFunction = DataConsolidateFunction.AVERAGE;
			break;
		case "STDDEV": // need to see the actual name
			retFunction = DataConsolidateFunction.STD_DEV;
			break;
		case "AVERAGE":
			retFunction = DataConsolidateFunction.AVERAGE;
			break;
		case "MEAN":
			retFunction = DataConsolidateFunction.AVERAGE;
			break;
		default:
			retFunction = DataConsolidateFunction.SUM;
			break;
		}
		return retFunction;
	}
}
