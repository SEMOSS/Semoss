package prerna.reactor.frame.r.analytics;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

/**
 * This reactor updates determines the outlier rows based on the selected
 * attribute columns The result is a new column added to the data frame with
 * values of TRUE (is an outlier) or FALSE (not an outlier) The inputs to the
 * reactor are: 1) the attribute columns 2) alpha
 * 
 * RunOutlier(attributes=["Revenue_International","Revenue_Domestic","MovieBudget"],alpha=[0.05],nullHandleType=["As_Is"])
 * RunOutlier(attributes=["bp_1d","bp_1s","bp_2d","bp_2s"],alpha=[0.05],nullHandleType=["Impute"])
 * 
 */

public class RunOutlierReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RunOutlierReactor.class.getName();

	private static final String ALPHA = "alpha";
	private static final String NULL_HANDLING = "nullHandleType";

	public RunOutlierReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ATTRIBUTES.getKey(), ALPHA };
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "mclust", "flashClust", "leaps", "FNN", "FactoMineR",
				"HDoutliers", "missRanger" };
		this.rJavaTranslator.checkPackages(packages);
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		String frameName = dataFrame.getName();

		// figure out inputs
		List<String> attributeNamesList = getAttributeNames();
		String alpha = getAlpha();
		String nullHandlingType = getNullHandleType();

		// determine the name for the new outlier column to avoid adding columns with
		// same name
		String[] allColNames = dataFrame.getColumnHeaders();
		int counter = 0;
		String newColName = "Outlier";
		while (ArrayUtilityMethods.arrayContainsValue(allColNames, newColName)) {
			counter++;
			newColName = "Outlier_" + counter;
		}
		String[] attributeNames = attributeNamesList.toArray(new String[] {});

		// dont need to check the unique count of instances for each attribute
		// because of the new null handling options
		if (nullHandlingType.equals("as_is")) {
			for (String columnName : attributeNamesList) {
				// make sure there are no nulls if we are doing as_is
				int countNulls = this.rJavaTranslator.getInt("sum(is.na(" + frameName + "$" + columnName + "))");
				if (countNulls > 0) {
					String msg = columnName + " has " + countNulls
							+ " null values. Please select Impute or Drop to handle the null values.";
					throw new IllegalArgumentException(msg);
				}
			}
		}

		// get the data from the outlier algorithm
		logger.info("Start iterating through data to determine outliers");
		String newFrameName = runROutlierAlgorithm(frameName, attributeNames, alpha, nullHandlingType, newColName);
		logger.info("Done iterating through data to determine outliers");

		// check for error message
		String errorMessage = this.rJavaTranslator.getString(newFrameName);
		if (errorMessage != null) {
			String msg = errorMessage;
			throw new IllegalArgumentException(msg);
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, dataFrame, "OutliersAlgorithm",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// now create the frame and return the noun
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(frameName, newFrameName));
		RDataTable newTable = createNewFrameFromVariable(frameName);
		this.insight.setDataMaker(newTable);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(frameName, noun);

		return noun;
	}

	/**
	 * Run the outlier algorithm using an r script
	 * 
	 * @param frameName
	 * @param attributeNames
	 * @param alpha
	 * @param newColName
	 */
	private String runROutlierAlgorithm(String frameName, String[] attributeNames, String alpha,
			String nullHandlingType, String newColName) {

		// create a column vector to pass as an input into our R script
		String colVector = RSyntaxHelper.createStringRColVec(attributeNames);

		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// source the r script that will run the outlier and impute routine
		String outlierScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\HDoutliers.R";
		String imputeScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\ImputeData.R";
		outlierScriptFilePath = outlierScriptFilePath.replace("\\", "/");
		imputeScriptFilePath = imputeScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + outlierScriptFilePath + "\");");
		rsb.append("source(\"" + imputeScriptFilePath + "\");");

		// add function
		String frameAsDf = frameName + Utility.getRandomString(6);
		rsb.append(RSyntaxHelper.asDataFrame(frameAsDf, frameName));
		rsb.append(resultsFrameName + "<- find_outliers(" + frameAsDf + ", " + colVector + ", " + alpha + ", \""
				+ nullHandlingType + "\"" + ", \"" + newColName + "\")[[2]];");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// garbage collection
		this.rJavaTranslator.executeEmptyR("rm(" + frameAsDf + "); gc();");

		// return new frame
		return resultsFrameName;

	}

	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////

	/*
	 * Retrieving inputs
	 */

	private List<String> getAttributeNames() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(this.keysToGet[0]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}
		return null;
	}

	private String getAlpha() {
		GenRowStruct alphaGrs = this.store.getNoun(ALPHA);
		if (alphaGrs != null) {
			if (alphaGrs.size() > 0) {
				return alphaGrs.get(0).toString();
			}
		}
		// default to .05
		return "0.05";
	}

	private String getNullHandleType() {
		// an action specifying how to handle missing data (options:
		// "impute","drop","as_is")
		GenRowStruct alphaGrs = this.store.getNoun(NULL_HANDLING);
		if (alphaGrs != null) {
			if (alphaGrs.size() > 0) {
				return alphaGrs.get(0).toString().toLowerCase();
			}
		}
		// default to .05
		return "as_is";
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ALPHA)) {
			return "The statistical threshold to determine the cutoff point for outliers (Defaut = 0.05)";
		} else if (key.equals(NULL_HANDLING)) {
			return "An action specifying how to handle missing data (options: \"impute\",\"drop\",\"as_is\")";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
