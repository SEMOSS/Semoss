package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;

public class RNumericalColumnSimilarityReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RNumericalColumnSimilarityReactor.class.getName(); 
	
	// variable to determine size of sampled data for analysis, default is 100
	protected static final String SAMPLE_SIZE = "sampleSize";
	// significance level set to run analysis, default is 0.05
	protected static final String SIGNIFICANCE = "significance";
	// value to determine whether we also want script to retun non-similar columns, default is true
	protected static final String SHOW_ALL = "showAll";

	/**
	 * RunNumericalColumnSimilarity ( columns = [ "RottenTomatoes_Audience" , "RottenTomatoes_Critics", "Revenue_Domestic" , "Revenue_International", "MovieBudget" ] , panel = [ "0" ] , sampleSize = ["100"], significance = ["0.05"], showAll = ["TRUE"] );
	 */

	public RNumericalColumnSimilarityReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.PANEL.getKey(), SAMPLE_SIZE, SIGNIFICANCE, SHOW_ALL  };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[] { "data.table" };
		this.rJavaTranslator.checkPackages(packages);
		Logger logger = this.getLogger(CLASS_NAME);
		RDataTable dataFrame = (RDataTable) getFrame();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);
		
		// get inputs
		List<String> numericalCols = getColumns();
		
		String panelId = this.keyValue.get(this.keysToGet[1]);
		
		String sampleSize = this.keyValue.get(this.keysToGet[2]);
		if (sampleSize == null ) {
			sampleSize = "100";
		}
		String significance = this.keyValue.get(this.keysToGet[3]);
		if (significance == null) {
			significance = "0.05";
		}
		String showAll = this.keyValue.get(this.keysToGet[4]);
		if(showAll == null) {
			showAll = "true"; 
		}

		// get number of cols
		int numCols = numericalCols.size();
		
		// make sure that at least two columns were provided
		if(numCols < 2) {
			String errorString = "Please select two or more numerical columns to run this algorithm";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		
		
		// get the correlation data from the run r regression algorithm
		logger.info("Start iterating through data to determine similarity");
		String resultsList = runAlgorithm(frameName, numericalCols, sampleSize, significance, showAll);
		logger.info("Done iterating through data to determine similarity");
		
		// create the object to return to the FE
		// there will always be three new, existing, and similarity
		int numRows = this.rJavaTranslator.getInt("nrow(" + resultsList + ")");
		Object[][] retOutput = new Object[numRows][3];
		
		// need to fill in the object with the data values
		// retrieve data using getBulkDataRow
		String[] heatMapHeaders = new String[] { "New", "Existing", "Similarity" };
		String query = resultsList + "[" + 1 + ":" + numRows + "]";

		List<Object[]> bulkRows = this.rJavaTranslator.getBulkDataRow(query, heatMapHeaders);

		// each entry into the list is a row - we need to put this in the form of
		// Object[][]
		for (int i = 0; i < bulkRows.size(); i++) {
			retOutput[i] = bulkRows.get(i);
		}

		// create and return a task
		ITask taskData = ConstantTaskCreationHelper.getHeatMapData(panelId, "New","Existing","Similarity", retOutput);
		this.insight.getTaskStore().addTask(taskData);
		
		// variable cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + resultsList + "); gc();");
		
		// User Tracking
//		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, dataFrame, "ColSimilarity",
//				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// now return this object
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		
		// significance is defaulted to 0.05, if something other than 0.1, 0.05, 0.02, or 0.01 is chosen
		// so throw a notification to the user to let them know
		// currently though, we dont expose this to the UI, so this will never be hit
		
		if(!significance.equals("0.1") || !significance.equals("0.05") || !significance.equals("0.02") || !significance.equals("0.01") ) {
			noun.addAdditionalReturn(new NounMetadata("Significance level was reset to 0.05", PixelDataType.CONST_STRING,
					PixelOperationType.WARNING));
		}
		noun.addAdditionalReturn(new NounMetadata("Numerical Column Similarity ran successfully!", PixelDataType.CONST_STRING,
				PixelOperationType.SUCCESS));
		return noun;

	}


	private String runAlgorithm(String frameName, List<String> retHeaders, String sampleSize, String significance, String showAll) {
		// the name of the result for merging later
		String resultFrame = "resultFrame" + Utility.getRandomString(10);
		String wd = "wd" + Utility.getRandomString(10);
		
		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// source the r script that will run the numerical correlation routine
		String NumSimScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts";
		NumSimScriptFilePath = NumSimScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + NumSimScriptFilePath + "/NumColSimilarity.R"+  "\");\n");
		rsb.append(wd + " <- getwd();\n");
		rsb.append("setwd(\"" + NumSimScriptFilePath + "\");\n");
		
		// creating the frame of just the columns listed
		rsb.append(resultFrame + " <- " + frameName + "[,c(");
		String sep = "";
		for (String head : retHeaders) {
			rsb.append(sep);
			sep = " ,";
			rsb.append(" \"" + head + "\"");
		}
		rsb.append(" )];\n");
		
		
		// get random subset of column data
		rsb.append("if(nrow(" + resultFrame + ") > " + sampleSize + ") {");
		rsb.append(resultFrame + "<-" + resultFrame + "[sample(nrow(" + resultFrame + ")," + sampleSize + ")];}\n");
		
		// run function
		rsb.append(resultFrame + " <- find_match( as.data.frame(" + resultFrame + ") , as.data.frame(" + resultFrame
				+ ") , " + significance + ", " + showAll.toUpperCase() + ");");
		
		// combine "Identical" and "Close" columns
		// to determine "Similar" score
		rsb.append(resultFrame + "$Similarity <- (" + resultFrame + "$Identical_CDF + " + resultFrame + "$Close_CDF );");
		
		rsb.append("setwd(\"" + wd + "\");\n");
		
		this.rJavaTranslator.runR(rsb.toString());
		
		return resultFrame;
	}
	
	/*
	 * Get the input columns
	 */
	
	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(this.keysToGet[0]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for(Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}
	   return null;
	}

}