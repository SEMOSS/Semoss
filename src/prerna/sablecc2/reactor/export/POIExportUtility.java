package prerna.sablecc2.reactor.export;

import java.util.Map;

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
import org.python.google.common.base.Strings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class POIExportUtility {

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	
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
     * Get the cell style from either the panel formatting or the metamodel formatting
     * @param workbook
     * @param metamodelAdditionalDataType
     * @param panelFormatting
     * @return
     */
    public static CellStyle getCurrentStyle(Workbook workbook, String metamodelAdditionalDataType, Map<String,String> panelFormatting) {
    	CellStyle curStyle = workbook.createCellStyle();
    	DataFormat df = workbook.createDataFormat();
    	// panel formatting goes first
    	// then formatting at the additional type level
    	String format = null;
    	
    	String prepend = null;
    	String append = null;
    	Object round = null;
    	
    	if(panelFormatting != null && panelFormatting.containsKey("type")) {
    		String panelAdditionalDataType = panelFormatting.get("type").toLowerCase();
    		format = getBaseExcelFormat(panelAdditionalDataType);

    		prepend = panelFormatting.get("prepend");
    		append = panelFormatting.get("append");
    		round = panelFormatting.get("round");
    		
    		boolean hasPrepend = prepend != null && !prepend.isEmpty();
    		boolean hasAppend = append != null && !append .isEmpty();

    		// special cases
    		try {
	    		if(format == null && round != null) {
	    			format = "#." + Strings.repeat("0", Integer.parseInt(round + ""));
	    		}
    		} catch(Exception e) {
    			// ignore
    		}
    		if(format == null && (hasPrepend || hasAppend) ) {
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
    		if(format != null) {
    			curStyle.setDataFormat(df.getFormat(format));
    			return curStyle;
    		}
    	}

    	// now try the metamodel additional type
    	if(metamodelAdditionalDataType != null) {
    		// first check if this is a map
    		Map<String, String> customDataFormat = null;
    		try {
    			customDataFormat = gson.fromJson(metamodelAdditionalDataType, Map.class);
    			if(customDataFormat != null && !customDataFormat.isEmpty()) {
    				metamodelAdditionalDataType = customDataFormat.get("type").toLowerCase();
    			}
    		} catch(JsonSyntaxException e) {
    			// ignore
    		} catch(Exception e) {
    			e.printStackTrace();
    		}

    		format = getBaseExcelFormat(metamodelAdditionalDataType);
    		if(customDataFormat != null) {
    			prepend = customDataFormat.get("prepend");
    			append = customDataFormat.get("append");
        		round = customDataFormat.get("round");
    		}

    		boolean hasPrepend = prepend != null && !prepend.isEmpty();
    		boolean hasAppend = append != null && !append .isEmpty();

    		// special cases
    		try {
	    		if(format == null && round != null) {
	    			format = "#." + Strings.repeat("0", Integer.parseInt(round + ""));
	    		}
    		} catch(Exception e) {
    			// ignore
    		}
    		if(format == null && (hasPrepend || hasAppend) ) {
    			format = "General";
    		}

    		// apply prepend/append
    		if (prepend != null && !prepend.isEmpty()) {
    			format = "\"" + prepend + "\"" + format;
    		} 
    		if (append != null && !append.isEmpty()) {
    			format = format + "\"" + append + "\"";
    		}

    		if(format != null) {
    			curStyle.setDataFormat(df.getFormat(format));
    			return curStyle;
    		}
    	}

    	// nothing worked
    	return null;
    }
    
    /**
     * Convert to get the base format
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
              case "MMMMM d, yyyy":
                  format = "MMMM d, yyyy";
                  break;
              case "EEEEE, MMMMM d, yyyy":
                  format = "dddd, MMMM d, yyyy";
                  break;
              case "M/d/yy hh:mm a":
                  format = "M/dd/yy hh:mm AM/PM";
                  break;
              case "M/d/yy HH:mm":
                  format = "M/dd/yy HH:mm";
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

		switch(functionName.toUpperCase()) {
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
