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
 * This reactor updates determines the similarity between values in an instance column based on the selected attribute columns
 * The result is a new column added to the data frame with values between 0 and 1 for each row
 * Higher values indicate stronger similarity
 * The inputs to the reactor are: 
 * 1) the instance column
 * 2) the attribute columns
 */

public class RSimilarityAlgorithmReactor extends AbstractRFrameReactor {
	
	private static final String CLASS_NAME = RSimilarityAlgorithmReactor.class.getName();
	
	private String[] attributeNames;
	private List<String> attributeNamesList;
	private String instanceColumn;
	
	public RSimilarityAlgorithmReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		
		// figure out inputs
		this.instanceColumn = getInstanceColumn();
		this.attributeNamesList = getAttributes(instanceColumn);
		this.attributeNames = this.attributeNamesList.toArray(new String[0]);
		
		// determine the name for the new similarity column
		// to avoid adding columns with same name
		String[] allColNames = dataFrame.getColumnHeaders();
		int counter = 0;
		String newColName = this.instanceColumn + "_SIMILARITY";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = this.instanceColumn + "_SIMILARITY_" + counter;
		}
		
		// get the correlation data from the run r correlation algorithm
		logger.info("Start iterating through data to determine similarity");
		runRSimilarityAlgorithm(frameName, attributeNames, instanceColumn, newColName);
		logger.info("Done iterating through data to determine similarity");

		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "SimilarityAlgorithm");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"SimilarityAlgorithm", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// create the new frame meta
		meta.addProperty(frameName, frameName + "__" + newColName);
		meta.setAliasToProperty(frameName + "__" + newColName, newColName);
		meta.setDataTypeToProperty(frameName + "__" + newColName, "DOUBLE");
		
		// now return this object
		// we are returning the name of our table that sits in R; it is structured as a list of entries: x,y,cor
		NounMetadata noun = new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(
				new NounMetadata("Similarity ran succesfully! See new \"" + newColName + "\" column in the grid.",
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	/**
	 * R the similarity algorithm using an r script
	 * @param frameName
	 * @param attributeNames
	 * @param instanceColumn
	 * @param newColName
	 */
	private void runRSimilarityAlgorithm(String frameName, String[] attributeNames, String instanceColumn, String newColName) {
		
		// create a column vector to pass as an input into our R script
		String colVector = RSyntaxHelper.createStringRColVec(attributeNames);
		
		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);
		
		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();
		// source the r script that will run the numerical correlation routine
		String correlationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Similarity.R";
		correlationScriptFilePath = correlationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + correlationScriptFilePath + "\");");
        // R syntax for the routine: ResultsTableName <- getCorrelationTable(frameName, "Title", c("col1", "col2"), "simColumnName")
		rsb.append(resultsFrameName + "<- GenerateSimilarityTable(" + frameName + ", " + "\"" + instanceColumn + "\"" + ", " + colVector + ", " + "\"" + newColName + "\"" + ");");
		rsb.append(RSyntaxHelper.asDataTable(frameName, resultsFrameName));
		// garbage collection
		rsb.append("rm(" + resultsFrameName + ",CalculateSimilarity, CSimilarity,DefineRatios,"
				+ "FindCentroids,GenerateCountTable,GenerateLookupDT, "
				+ "GenerateSimilarityTable,GenerateWeightsTable,ScaleUniqueData); gc();");
		// run the script
		this.rJavaTranslator.runR(rsb.toString());


	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	
	/*
	 * Retrieving inputs
	 */
	
	private String getInstanceColumn() {
		//check if instance column was input with the key 
		GenRowStruct instanceIndexGrs = this.store.getNoun(keysToGet[0]);
		String instanceColumn = "";
		NounMetadata instanceColumnNoun;
		if (instanceIndexGrs != null) {
			instanceColumnNoun = instanceIndexGrs.getNoun(0);
			instanceColumn = (String) instanceColumnNoun.getValue();
		} else {
			//else assume the column is the zero index noun in the curRow
			instanceColumnNoun = this.curRow.getNoun(0);
			instanceColumn = (String) instanceColumnNoun.getValue();
		}
		return instanceColumn;
	}

	private List<String> getAttributes(String instanceColumn) {
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
}
