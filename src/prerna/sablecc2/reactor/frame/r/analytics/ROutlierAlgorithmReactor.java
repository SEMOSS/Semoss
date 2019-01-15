package prerna.sablecc2.reactor.frame.r.analytics;

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
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

/**
 * This reactor updates determines the outliers for a set of values in an instance column based on the selected attribute columns
 * The result is a new column added to the data frame with values of TRUE (is an outlier) or FALSE (not an outlier)
 * The inputs to the reactor are: 
 * 1) the instance column
 * 2) the attribute columns
 * 3) whether each row should be treated as a unique instance (Yes or No)
 * 
 */

public class ROutlierAlgorithmReactor extends AbstractRFrameReactor {
	
	private static final String CLASS_NAME = ROutlierAlgorithmReactor.class.getName();
	
	private static final String ALPHA = "alpha";
	private static final String UNIQUE_INSTANCE_PER_ROW= "uniqInstPerRow";
	
	public ROutlierAlgorithmReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey(), UNIQUE_INSTANCE_PER_ROW};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] {"data.table", "HDoutliers", "FactoMineR"};
		this.rJavaTranslator.checkPackages(packages);
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		
		// figure out inputs
		String instanceColumn = getInstanceCol();
		List<String> attributeNamesList = getAttributeNames(instanceColumn);
		String[] attributeNames = attributeNamesList.toArray(new String[]{});
		String alpha = getAlpha();
		String uniqInstPerRowStr = getUniqInstPerRow();
		
		// determine the name for the new similarity column to avoid adding columns with same name
		String[] allColNames = dataFrame.getColumnHeaders();
		int counter = 0;
		String newColName = instanceColumn + "_Outlier";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = instanceColumn + "_Outlier_" + counter;
		}
		
		// get the data from the outlier algorithm
		logger.info("Start iterating through data to determine outliers");
		runROutlierAlgorithm(frameName, instanceColumn, attributeNames, newColName, alpha, uniqInstPerRowStr);
		logger.info("Done iterating through data to determine outliers");

		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "OutliersAlgorithm");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"OutliersAlgorithm", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// create the new frame meta
		meta.addProperty(frameName, frameName + "__" + newColName);
		meta.setAliasToProperty(frameName + "__" + newColName, newColName);
		meta.setDataTypeToProperty(frameName + "__" + newColName, "STRING");
		
		// now return this object
		NounMetadata noun = new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(
				new NounMetadata("Outlier ran succesfully! See new \"" + newColName + "\" column in the grid.", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	/**
	 * R the similarity algorithm using an r script
	 * @param frameName
	 * @param instanceColumn
	 * @param attributeNames
	 * @param newColName
	 */
	private void runROutlierAlgorithm(String frameName, String instanceColumn, String[] attributeNames, String newColName, String alpha, String uniqInstPerRowStr) {
		
		// create a column vector to pass as an input into our R script
		String colVector = RSyntaxHelper.createStringRColVec(attributeNames);
		
		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);
		
		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();
		// source the r script that will run the numerical correlation routine
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\HDoutliers.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");
        // R syntax for the routine: ResultsTableName <- DetermineHDOutliers(frameName, "Title", c("col1", "col2"), alpha, "outlierColumnName", TRUE)
		rsb.append(resultsFrameName + "<- DetermineHDoutliers(" + frameName + ", " + "\"" + instanceColumn + "\"" + ", " + colVector + ", " + alpha + ", " +  "\"" + newColName + "\"" + ", " + uniqInstPerRowStr + ");");
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
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
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
	
	private String getAlpha() {
		GenRowStruct alphaGrs = this.store.getNoun(ALPHA);
		if (alphaGrs != null) {
			if (alphaGrs.size() > 0) {
				return alphaGrs.get(0).toString();
			}
		}
		// default to .10
		return ".10";
	} 
	
	private String getUniqInstPerRow() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(UNIQUE_INSTANCE_PER_ROW);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				String value = columnGrs.get(0).toString().toUpperCase();
				if (value.equals("YES")) {
					return "TRUE";
				} else if (value.equals("NO")) {
					return "FALSE";
				}
			}
		} else {
			return "FALSE";
		}
		return null;
	}
	
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(UNIQUE_INSTANCE_PER_ROW)) {
			return "YES or NO indicator of whether each instance should be treated as unique or if non-unique instances should be grouped";
		} else {
			return super.getDescriptionForKey(key);
		}
}
}
