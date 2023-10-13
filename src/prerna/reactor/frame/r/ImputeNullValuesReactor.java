package prerna.reactor.frame.r;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ImputeNullValuesReactor extends AbstractRFrameReactor {

	/**
	 * This reactor imputes the value of null values in numeric columns to fill in
	 * null values with best guesses
	 */

	// ImputeNullValues ( columns = [ "bp_1d" , "bp_1s" , "bp_2s" ] ) ;

	protected static final String CLASS_NAME = ImputeNullValuesReactor.class.getName();
	private static final String HANDLE_NULL = "handleNull";

	
	public ImputeNullValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() , HANDLE_NULL };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		String frameName = dataFrame.getName();

		// Check Packages
		logger.info(stepCounter + ". Checking R Packages");
		String[] packages = new String[] { "missRanger" };
		this.rJavaTranslator.checkPackages(packages);

		// figure out inputs
		List<String> colsToImpute = getColumns();

		// run the function
		String newFrameName = runImputeValues(frameName, colsToImpute);

		// now create the frame and return the noun
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(frameName, newFrameName));
		RDataTable newTable = createNewFrameFromVariable(frameName);
		this.insight.setDataMaker(newTable);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(frameName, noun);
				

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, dataFrame, "ImputeNullValues",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return noun;
	}

	private String runImputeValues(String frameName, List<String> colsToImpute) {
		// create a column vector to pass as an input into our R script
		String colVector = RSyntaxHelper.createStringRColVec(colsToImpute);

		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// source the r script that will run the numerical correlation routine
		String outlierScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\ImputeData.R";
		outlierScriptFilePath = outlierScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + outlierScriptFilePath + "\");");

		// add function
		String frameAsDf = frameName + Utility.getRandomString(6);
		rsb.append(RSyntaxHelper.asDataFrame(frameAsDf, frameName));
		rsb.append(resultsFrameName + "<- impute_data(" + frameAsDf + ", " + colVector + ",3,5,100);");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());
		this.addExecutedCode(rsb.toString());

		// garbage collection
		String cleanup = "rm(" + frameAsDf + "); gc();";
		this.rJavaTranslator.executeEmptyR(cleanup);
		this.addExecutedCode(cleanup);

		// return new frame
		return resultsFrameName;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private List<String> getColumns() {
		List<String> cols = new ArrayList<String>();

		// try its own key
		GenRowStruct colsGrs = this.store.getNoun(keysToGet[0]);
		if (colsGrs != null && !colsGrs.isEmpty()) {
			int size = colsGrs.size();
			for (int i = 0; i < size; i++) {
				cols.add(colsGrs.get(i).toString());
			}
			return cols;
		}

		int inputSize = this.getCurRow().size();
		if (inputSize > 0) {
			for (int i = 0; i < inputSize; i++) {
				cols.add(this.getCurRow().get(i).toString());
			}
			return cols;
		}

		throw new IllegalArgumentException("Need to define the columns to impute");
	}
}
