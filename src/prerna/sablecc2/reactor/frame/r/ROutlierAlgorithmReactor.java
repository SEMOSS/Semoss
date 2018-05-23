package prerna.sablecc2.reactor.frame.r;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;
import prerna.util.ga.GATracker;

/**
 * This reactor updates determines the similarity between values in an instance column based on the selected attribute columns
 * The result is a new column added to the data frame with values between 0 and 1 for each row
 * Higher values indicate stronger similarity
 * The inputs to the reactor are: 
 * 1) the instance column
 * 2) the attribute columns
 * 
 * TODO: 
 * tell the user if the subset size is too small for the sample size 
 * nas and missing values
 * validate data
 * write out formulas for this and for similarity
 * look through outlier algorithm to make sure that nothing was missed
 * 
 * 
 */

public class ROutlierAlgorithmReactor extends AbstractRFrameReactor {
	
	private static final String CLASS_NAME = ROutlierAlgorithmReactor.class.getName();
	
	private static final String NUMRUNS_KEY = "numRuns";
	private static final String SUBSETSIZE_KEY = "subsetSize";
	
	public ROutlierAlgorithmReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), SUBSETSIZE_KEY, NUMRUNS_KEY, ReactorKeysEnum.ATTRIBUTES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
		String frameName = dataFrame.getTableName();
		dataFrame.setLogger(logger);
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		
		// figure out inputs
		int numSubsetSize = getSubsetSize();
		int numRuns = getNumRuns();
		String instanceColumn = getInstanceCol();
		List<String> attributeNamesList = getAttributeNames(instanceColumn);
		String[] attributeNames = attributeNamesList.toArray(new String[]{});
		
		// determine the name for the new similarity column
		// to avoid adding columns with same name
		String[] allColNames = dataFrame.getColumnHeaders();
		int counter = 0;
		String newColName = instanceColumn + "_Outlier";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = instanceColumn + "_Outlier_" + counter;
		}
		
		// check to make sure that the length of the sample data is not longer than the number of rows of instance columns
		// check the length(unique(df$Genre))
		int instColLength = this.rJavaTranslator.getInt("length(unique(" + frameName + "$" + instanceColumn + "));" );
		if (instColLength < numSubsetSize) {
			throw new IllegalArgumentException("Subset size is larger than the number of instances.");
		}
		
		// get the correlation data from the run r correlation algorithm
		logger.info("Start iterating through data to determine outliers");
		runROutlierAlgorithm(frameName, numSubsetSize, numRuns, instanceColumn, attributeNames, newColName);
		logger.info("Done iterating through data to determine outliers");

		// track GA data
		GATracker.getInstance().trackAnalyticsPixel(this.insight, "OutliersAlgorithm");
		
		// create the new frame meta
		meta.addProperty(frameName, frameName + "__" + newColName);
		meta.setAliasToProperty(frameName + "__" + newColName, newColName);
		meta.setDataTypeToProperty(frameName + "__" + newColName, "DOUBLE");
		
		// now return this object
		// we are returning the name of our table that sits in R; it is structured as a list of entries: x,y,cor
		return new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	/**
	 * R the similarity algorithm using an r script
	 * @param frameName
	 * @param attributeNames
	 * @param instanceColumn
	 * @param newColName
	 */
	private void runROutlierAlgorithm(String frameName, int numSubsetSize, int numRuns, String instanceColumn, String[] attributeNames, String newColName) {
		
		// create a column vector to pass as an input into our R script
		String colVector = RSyntaxHelper.createStringRColVec(attributeNames);
		
		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);
		
		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();
		// source the r script that will run the numerical correlation routine
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Outliers.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");
        // R syntax for the routine: ResultsTableName <- getCorrelationTable(frameName, "Title", c("col1", "col2"), "simColumnName")
		rsb.append(resultsFrameName + "<- GetOutliers(" + frameName + ", " + numSubsetSize + ", " + numRuns + ", " + "\"" + instanceColumn + "\"" + ", " + colVector + ", " + "\"" + newColName + "\"" + ");");
		rsb.append(frameName +  " <- " + resultsFrameName);

		// run the script
		this.rJavaTranslator.runR(rsb.toString());
		// garbage collection
		this.rJavaTranslator.executeEmptyR("rm(" + resultsFrameName + "); gc();");
	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	
	/*
	 * Retrieving inputs
	 */
	private List<String> getAttributeNames(String instanceColumn) {
		// see if defined as individual key
		List<String> retList = new ArrayList<String>();
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				String attribute = noun.getValue().toString();
				if (!(attribute.equals(instanceColumn))) {
					retList.add(attribute);
				}
			}
		} else {
			int rowLength = this.curRow.size();
			for (int i = 1; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				String attribute = colNoun.getValue().toString();
				if (!(attribute.equals(instanceColumn))) {
					retList.add(attribute);
				}
			}
		}
		return retList;
	}

	private String getInstanceCol() {
		// check if the instance column is entered with a key
		GenRowStruct instanceIndexGrs = this.store.getNoun(keysToGet[0]);
		String instanceCol = "";
		NounMetadata instanceIndexNoun;
		if (instanceIndexGrs != null) {
			instanceIndexNoun = instanceIndexGrs.getNoun(0);
			instanceCol = (String) instanceIndexNoun.getValue();
		} else {
			// else, we assume it is the zero index in the current row -->
			// runOutlier(FRAME_COL, numSubsetSize, numRuns, [FRAME_COL1, ... ,
			// FRAME_COLN]);
			instanceIndexNoun = this.curRow.getNoun(0);
			instanceCol = (String) instanceIndexNoun.getValue();
		}

		return instanceCol;
	}

	private int getNumRuns() {
		GenRowStruct numRunsGrs = this.store.getNoun(NUMRUNS_KEY);
		int numRuns = -1;
		NounMetadata numRunsNoun;
		if (numRunsGrs != null) {
			numRunsNoun = numRunsGrs.getNoun(0);
			numRuns = ((Number) numRunsNoun.getValue()).intValue();
		} else {
			// else, we assume it is the second index in the current row -->
			// runOutlier(FRAME_COL, numSubsetSize, numRuns, [FRAME_COL1, ... ,
			// FRAME_COLN]);
			numRunsNoun = this.curRow.getNoun(2);
			numRuns = ((Number) numRunsNoun.getValue()).intValue();
		}
		return numRuns;

	}

	
	
	private int getSubsetSize() {
		GenRowStruct subsetSizeGrs = this.store.getNoun(SUBSETSIZE_KEY);
		int subsetSize = -1;
		NounMetadata subsetSizeNoun;
		if (subsetSizeGrs != null) {
			subsetSizeNoun = subsetSizeGrs.getNoun(0);
			subsetSize = ((Number) subsetSizeNoun.getValue()).intValue();
		} else {
			// else, we assume it is the first index in the current row -->
			// runOutlier(FRAME_COL, numSubsetSize, numRuns, [FRAME_COL1, ... ,
			// FRAME_COLN]);
			subsetSizeNoun = this.curRow.getNoun(1);
			subsetSize = ((Number) subsetSizeNoun.getValue()).intValue();
		}
		return subsetSize;
	}
	
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SUBSETSIZE_KEY)) {
			return "The subset size";
		} else if (key.equals(NUMRUNS_KEY)) {
			return "The number of runs";
		} else {
			return super.getDescriptionForKey(key);
		}
}
}
