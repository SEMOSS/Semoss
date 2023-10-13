package prerna.reactor.frame.py;

import java.util.List;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class HistogramReactor extends AbstractFrameReactor {

	
	/**
	 * This reactor gets a histogram
	 * The inputs to the reactor are: 
	 * 1) the column to base the histogram on
	 * 2) the number of breaks
	 * 3) the panel id - defaults to zero if nothing is entered
	 */
	
	public HistogramReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.BREAKS.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		//get frame name
		String table = frame.getName();
		
		//get inputs
		String column = getColumn();
		//clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		//get number of breaks as an integer
		int numBreaks = getNumBreaks();
		
		//need to retrieve panel id to use in the task options
		String panelId = getPanelId();
		
		//build the py script to execute
		return getHistogram(frame, table, column, panelId, numBreaks);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// MAKE HISTOGRAM ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	//method to make the histogram - now we can easily use the histogram code from other reactors
	protected NounMetadata getHistogram(PandasFrame frame, String table, String column, String panelId, int numBreaks) {
		PyTranslator pyT = this.insight.getPyTranslator();
		StringBuilder script = new StringBuilder();
		script.append("hist,bins = ");
		StringBuilder formatHist = new StringBuilder();
		StringBuilder formatBins = new StringBuilder();
		boolean format = false;

		String colSelector = table + "['" + column + "']";
		if(numBreaks > 1) {
//			np.histogram( FRAME940921[~np.isnan(FRAME940921['MovieBudget'])]['MovieBudget'], bins=5)
			script.append("np.histogram(").append(table).append("[~np.isnan(").append(colSelector).append(")]['").append(column).append("'], bins=").append(numBreaks).append(")");
		} else {
//			np.histogram( FRAME940921[~np.isnan(FRAME940921['MovieBudget'])]['MovieBudget'], bins='auto')
			script.append("np.histogram(").append(table).append("[~np.isnan(").append(colSelector).append(")]['").append(column).append("'], bins='auto')");
			formatHist.append("hist = list(map(int, hist))");
			formatBins.append("bins = list(bins)");
			format = true;
		}
		
		insight.getPyTranslator().runPyAndReturnOutput("import numpy as np", script.toString());

		if (format) {
			insight.getPyTranslator().runPyAndReturnOutput(formatHist.toString(), formatBins.toString());
		}

		List<Object> counts = pyT.getList("hist");
		List<Object> breaks = pyT.getList("bins");

		//get the number of bins from the length of the counts
		int numBins;
		if (counts != null){
			numBins = counts.size();
		} else {
			numBins = 0;
		}
		Object[][] data = new Object[numBins][2];

		//add the data to the data object
		for(int i = 0; i < numBins; i++) {
			data[i][0] = breaks.get(i) + " - " + breaks.get(i + 1);
			data[i][1] = counts.get(i);
		}
		
		//task data includes task options
		ITask taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, column, "Frequency", data);
		
		//return metadata
		return new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	//get column using key "COLUMN"
	private String getColumn() {
		GenRowStruct columnGRS = this.store.getNoun(keysToGet[0]);
		if (columnGRS != null && !columnGRS.isEmpty()) {
			NounMetadata noun1 = columnGRS.getNoun(0);
			String column = noun1.getValue() + "";
			if (column.length() == 0) {
				throw new IllegalArgumentException("Need to define column to build histogram");
			}
			return column;
		}
		throw new IllegalArgumentException("Need to define column to build histogram");
	}
	
	//get number of breaks using key "BREAKS"
	private int getNumBreaks() {
		int numBreaks = 0;
		GenRowStruct breaksGRS = this.store.getNoun(keysToGet[1]);
		if (breaksGRS != null) {
			NounMetadata noun2 = breaksGRS.getNoun(0);
			if (noun2 != null) {
				numBreaks = (int) noun2.getValue();
			}
		}
		return numBreaks;
	}
	
	//get panel id using key "PANEL"
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}
}
