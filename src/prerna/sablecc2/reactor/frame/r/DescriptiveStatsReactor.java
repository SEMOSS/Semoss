package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;

public class DescriptiveStatsReactor extends AbstractRFrameReactor {

	/**
	 * This reactor gets statistics for a column
	 * 1) column to get stats on
	 * 2) panelId (defaults to zero if no panel id is entered)
	 */

	
	public DescriptiveStatsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		
		// initialize the rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		//get frame name
		String table = frame.getName();
				
		//get inputs
		String column = getColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		
		//need panel id to display
		String panelId = getPanelId();
		
		//create the object to store the output in
		Object [][] retOutput = new Object[8][2]; // name and the number of items
		String frameExpr = table + "$" + column;
		
		//get minimum
		String script = "min(as.numeric(na.omit(" + frameExpr + ")))";
		double min = this.rJavaTranslator.getDouble(script);
		retOutput[0][0] = "Minimum";
		retOutput[0][1] = min;

		//get quartiles
		script = "quantile(as.numeric(na.omit(" + frameExpr + ")), prob = c(0.25, 0.75))";
		double[] quartiles = this.rJavaTranslator.getDoubleArray(script);
		retOutput[1][0] = "Q1";
		retOutput[1][1] = quartiles[0];
		retOutput[2][0] = "Q3";
		retOutput[2][1] = quartiles[1];
		
		//get maximum
		script = "max(as.numeric(na.omit(" + frameExpr + ")))";
		double max = this.rJavaTranslator.getDouble(script);
		retOutput[3][0] = "Maximum";
		retOutput[3][1] = max;

		//get mean
		script = "mean(as.numeric(na.omit(" + frameExpr + ")))";
		double mean = this.rJavaTranslator.getDouble(script);
		retOutput[4][0] = "Mean";
		retOutput[4][1] = mean;

		//get median
		script = "median(as.numeric(na.omit(" + frameExpr + ")))";
		double median = this.rJavaTranslator.getDouble(script);
		retOutput[5][0] = "Median";
		retOutput[5][1] = median;

		//get sum
		script = "sum(as.numeric(na.omit(" + frameExpr + ")))";
		double sum = this.rJavaTranslator.getDouble(script);
		retOutput[6][0] = "Sum";
		retOutput[6][1] = sum;

		//get standard deviation
		script = "sd(as.numeric(na.omit(" + frameExpr + ")))";
		double sd = this.rJavaTranslator.getDouble(script);
		retOutput[7][0] = "Standard Deviation";
		retOutput[7][1] = sd;

		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "StatOutput", retOutput);
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(keysToGet[0]);
		if (columnGRS != null && !columnGRS.isEmpty()) {
			NounMetadata noun1 = columnGRS.getNoun(0);
			String column = noun1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to define column for descriptive statistics");
			}
			return column;
		}
		throw new IllegalArgumentException("Need to define column for descriptive statistics");
	}
	
	// get panel id using key "PANEL"
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}
}
