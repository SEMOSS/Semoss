package prerna.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class GetRFResultsReactor extends AbstractRFrameReactor{
	
	private static final String CLASS_NAME = GetRFResultsReactor.class.getName();
	private static final String SORTBY = "sortBy";
	private static final String REQUESTITEM = "requestItem";  //either 'VarImp' or 'ConfMatrix'
	/**
	 * GetRFResults(requestItem = [VarImp], panel = [99])
	 * GetRFResults(requestItem = [CONFMATRIX], panel = [99])
	 * 
	 * This reactor will only run if the RRandomForestAlgorithmReactor created a variable RF_VARIABLE_999988888877777 
	 * as it extracts/processes this variable.
	 * Input keys: 
	 * 		1. sortBy (optional) - for classification results, sorts the variable importance by either 
	 * 							   MeanDecreaseAccuracy (1) or MeanDecreaseGini (2). for regression results, 
	 * 							   sorts the variable importance by either %IncMSE (1) or IncNodePurity {2}. defaults = 1
	 * 		2. requestItem (required) - return object - must be either varimp (variable importance) or confmatrix (confusion matrix)
	 * 		3. panelID (required)
	 */
	public GetRFResultsReactor() {
		this.keysToGet = new String[] { SORTBY, REQUESTITEM, ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		String[] packages = new String[] { "data.table", "randomForest", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		String panelId = getPanelId();
		StringBuilder sb = new StringBuilder();

		// retrieve inputs
		String sortBy = getStringInput(SORTBY);
		if (sortBy == null) sortBy = "1";
		String requestItem = getStringInput(REQUESTITEM);
		if (!new ArrayList<String>(Arrays.asList("varimp", "confmatrix")).contains(requestItem.toLowerCase())){
			throw new IllegalArgumentException("Invalid requestItem - requestItem must be either 'varimp' or 'confmatrix'.");
		}
		
		// random forest r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\RandomForest.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");

		String temp_R = "tempVar" + Utility.getRandomString(8);
		ITask taskData = null;
		switch(requestItem.toLowerCase()) {
		case "varimp" :
			sb.append(temp_R + " <- getRFResults( RF_VARIABLE_999988888877777, 'varimp', sortBy=" + sortBy + ");");
			this.rJavaTranslator.runR(sb.toString());
			
			String[] varImpCols = this.rJavaTranslator.getColumns(temp_R + "$returnObject");
			List<Object[]> varImpData = this.rJavaTranslator.getBulkDataRow(temp_R + "$returnObject", varImpCols);
			//label,x,y,z,series
			String[] varImpAlignment= this.rJavaTranslator.getStringArray(temp_R + "$alignmentInfo");
			
			taskData = ConstantTaskCreationHelper.getScatterPlotData(panelId, varImpCols, varImpData, varImpAlignment[0], varImpAlignment[1], varImpAlignment[2], varImpAlignment[3], varImpAlignment[0], null);
			this.insight.getTaskStore().addTask(taskData);
			break;
		case "confmatrix":
			String rfType = this.rJavaTranslator.getString("RF_VARIABLE_999988888877777$type");
			if (rfType == "regression"){
				throw new IllegalArgumentException("Confusion matrix is unavailable for regression-type random forest model.");
			}
			
			sb.append(temp_R + " <- getRFResults( RF_VARIABLE_999988888877777, 'confmatrix', sortBy=" + sortBy + ");");
			this.rJavaTranslator.runR(sb.toString());
			
			String[] confMatrixCols = this.rJavaTranslator.getColumns(temp_R);
			List<Object[]> confMatrixData = this.rJavaTranslator.getBulkDataRow(temp_R, confMatrixCols);
			
			taskData = ConstantTaskCreationHelper.getGridData(panelId, confMatrixCols, confMatrixData);
			this.insight.getTaskStore().addTask(taskData);
			break;
		}

		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + temp_R + ",getRF,getRFResults);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun.addAdditionalReturn(
				new NounMetadata("Random Forest ran successfully!", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getStringInput(String keyName) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keyName);
		if (columnGrs != null) {
			if ( columnGrs.size() > 0 ) return columnGrs.get(0).toString();
		} else {
			if (keyName == REQUESTITEM) {
				throw new IllegalArgumentException("RequestItem of either 'varimp' or 'confmatrix' must be specified.");
			}
		}
		return null;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(this.keysToGet[2]);
		if (columnGrs != null) {
			if ( columnGrs.size() > 0 ) return columnGrs.get(0).toString();
		}
		return null;
	}

}
