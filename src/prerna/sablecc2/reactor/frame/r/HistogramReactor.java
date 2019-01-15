package prerna.sablecc2.reactor.frame.r;

import java.util.Map;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;

public class HistogramReactor extends AbstractRFrameReactor {

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
		
		// initialize the rJavaTranslator
		init();
		AbstractRJavaTranslator rJavaTranslator = this.rJavaTranslator;
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
		//get number of breaks as an integer
		int numBreaks = getNumBreaks();
		
		//need to retrieve panel id to use in the task options
		String panelId = getPanelId();
		
		//build the r script to execute
		return getHistogram(rJavaTranslator, table, column, panelId, numBreaks);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// MAKE HISTOGRAM ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	//method to make the histogram - now we can easily use the histogram code from other reactors
	protected NounMetadata getHistogram(AbstractRJavaTranslator rJavaTranslator, String table, String column, String panelId, int numBreaks) {
	
		String script = null;
		if(numBreaks > 1) {
			script = "hist(" + table + "$" + column + ", breaks=" + numBreaks + ", plot=FALSE)";
		} else {
			script = "hist(" + table + "$" + column + ", plot=FALSE)";
		}
		
		// so we know a bit about the structure
		// we can get the following values
		// 1: breaks
		// 2: counts
		// 3: density
		// 4: mids
		// 5: xname
		// 6: equidist

		// we only need the breaks and counts
		// format each range to the count value

		//get r vector - this will be specific to reserve or JRI
		//then use the r vector to get the breaks (double array) and the counts (int array)
		Map<String, Object> hist = rJavaTranslator.getHistogramBreaksAndCounts(script);
		double[] breaks = (double[]) hist.get("breaks");
		int[] counts = (int[]) hist.get("counts");
		
		//get the number of bins from the length of the counts
		int numBins;
		if (counts != null){
			numBins = counts.length;
		} else {
			numBins = 0;
		}
		Object[][] data = new Object[numBins][2];

		//add the data to the data object
		for(int i = 0; i < numBins; i++) {
			data[i][0] = breaks[i] + " - " + breaks[i+1];
			data[i][1] = counts[i];
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
