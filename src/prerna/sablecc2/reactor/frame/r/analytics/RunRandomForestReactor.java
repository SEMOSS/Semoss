package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class RunRandomForestReactor extends AbstractRFrameReactor {
	/**
	 * FRAME | RunRandomForest ( classify = [ Nominated ] , attributes = [ "Director", "Genre" , "RottenTomatoesAudience" , "Studio" , "Title" ] , space = [ "insight" ] , panel = [ 1 ] , size = [ 1000 ] , blocks = [ 100 ] , mode = [ "build" ] , treeDepth = [0] ) ;
	 * FRAME | RunRandomForest ( classify = [ Nominated ] , attributes = [ "Director", "Genre" , "RottenTomatoesAudience" , "Studio" , "Title" ] , space = [ "insight" ] , panel = [ 1 ] , size = [ 1000 ] , blocks = [ 100 ] , mode = [ "predict" ] , treeDepth = [0] ) ;
	 */
	
	private static final String CLASS_NAME = RunRandomForestReactor.class.getName();
	private static final String RF_VARIABLE = "RF_VARIABLE_999988888877777";
	private static final String COLS = "RF_COLS_999988888877777";
	private static final String CLASSIFICATION_COLUMN = "classify";
	private static final String SIZE = "size";
	private static final String BLOCKS = "blocks";
	private static final String MODE = "mode";
	private static final String DEPTH = "depth";

	public RunRandomForestReactor() {
		this.keysToGet = new String[] { CLASSIFICATION_COLUMN, ReactorKeysEnum.ATTRIBUTES.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.PANEL.getKey(), SIZE, BLOCKS, MODE, DEPTH };
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		organizeKeys();
		
		// check for packages
		String[] packages = new String[] { "data.table", "ranger" };
		this.rJavaTranslator.checkPackages(packages);
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String dtName = frame.getName();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		StringBuilder sb = new StringBuilder();

		// retrieve inputs
		String dep_var = getClassificationColumn();		
		List<String> attributes = getInputList(1);
		attributes.remove(dep_var);
		String ind_vars = "c(";
		for (int i = 0; i < attributes.size(); i++) {
			if (i == attributes.size() - 1) {
				ind_vars += "\"" + attributes.get(i) + "\"";
			} else {
				ind_vars += "\"" + attributes.get(i) + "\", ";
			}
		}
		ind_vars += ")";
		String sample_size = this.keyValue.get(this.keysToGet[4]);
		String sample_blocks = this.keyValue.get(this.keysToGet[5]);
		String mode = this.keyValue.get(this.keysToGet[6]);
		String tree_depth = this.keyValue.get(this.keysToGet[7]);
		
		// check if there are filters on the frame. if so then need to run algorithm on subsetted data
		if(!frame.getFrameFilters().isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attributes);
			selectedCols.add(dep_var);
			for(String s : selectedCols) {
				qs.addSelector(new QueryColumnSelector(s));
			}
			qs.setImplicitFilters(frame.getFrameFilters());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dtName);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			this.rJavaTranslator.runR(dtNameIF + "<- {" + query + "}");
			implicitFilter = true;
			
			//cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}
		String targetDt = implicitFilter ? dtNameIF : dtName;
		
		// random forest r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\random_forest.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\"); ");

		// assets path
		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, AbstractSecurityUtils.securityEnabled()) + "/" + dep_var ;
		
		// initialize task
		ITask taskData = null;
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		logger.info("All inputs loaded.");
		
		if (mode.equals("build")) { // check if the model needs to be built
			logger.info("Building random forest model...");
			// set call to R function
			sb.append(RF_VARIABLE + " <- rfmodel_mgr( " + ind_vars + ", \"" + dep_var + "\", " + targetDt + ", \"" + assetFolder + "\", " + sample_size + ", " + sample_blocks + ", " + tree_depth + ");");
			sb.append(COLS + " <- colnames(" + RF_VARIABLE + "[[2]]);");
			// execute R
			this.rJavaTranslator.runR(sb.toString());
			
			// clean up r temp variables
			StringBuilder cleanUpScript = new StringBuilder();
			cleanUpScript.append("rm(" + dtNameIF + ",getRF,getRFResults);");
			cleanUpScript.append("gc();");
			this.rJavaTranslator.runR(cleanUpScript.toString());

			// construct map of model summary data to return to front end
			Map<String, Object> retMap = new HashMap<String, Object>();
			double[] predError = this.rJavaTranslator.getDoubleArray(RF_VARIABLE + "[[1]]");
			predError[0] = round(predError[0]*100, 2);
			retMap.put("predictionError", predError);
			double[][] confMatx = this.rJavaTranslator.getDoubleMatrix(RF_VARIABLE + "[[2]]");
			List<Object[]> mtx = new ArrayList<>();
			if (confMatx == null) {
				String[] mtxOrdering = {"Attributes", "Prediction_Error"};
				mtx = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE + "[[2]]", mtxOrdering);
				retMap.put("matrix", mtx);
			} else {
				retMap.put("matrix", confMatx);
				String[] confMatxHeaders = this.rJavaTranslator.getStringArray(COLS);
				retMap.put("headers", confMatxHeaders);
			}
			String[] headerOrdering = {"Variable", "Importance"};
			List<Object[]> retOutput = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE + "[[3]]", headerOrdering);
	
			// create and return a task
			String panelId = getPanelId();
			taskData = ConstantTaskCreationHelper.getBarChartInfo(panelId, "Variables", "Importance Values", retOutput);
			this.insight.getTaskStore().addTask(taskData);

			// construct noun to create visualization and return model summary data
			NounMetadata noun2 = new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
			noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
			noun.addAdditionalReturn(noun2);
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Random Forest model was constructed successfully!"));
		} else if (mode.equals("predict")) { // check if values need to be predicted from an existing model
			logger.info("Predicting from random forest model...");
			// set call to R function
			sb.append(RF_VARIABLE + " <- predict_rfmodel( " + targetDt + ", \"" + assetFolder + "\");");
			// execute R
			this.rJavaTranslator.runR(sb.toString());
			
			// clean up r temp variables
			StringBuilder cleanUpScript = new StringBuilder();
			cleanUpScript.append("rm(" + dtNameIF + ",getRF,getRFResults);");
			cleanUpScript.append("gc();");
			this.rJavaTranslator.runR(cleanUpScript.toString());
			
			// create and return grid task
			String panelId = getPanelId();
			String[] headerOrdering = getColumns(RF_VARIABLE);
			List<Object[]> retOutput = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE, headerOrdering);
			taskData = ConstantTaskCreationHelper.getGridData(panelId, headerOrdering, retOutput);
			this.insight.getTaskStore().addTask(taskData);
			
			// construct noun to update grid
			noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Prediction from Random Forest model successful!"));
		}
		return noun;
	}

	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private double round(double value, int places) { 
	    if (places < 0) throw new IllegalArgumentException();
	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

	private String getClassificationColumn() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(CLASSIFICATION_COLUMN);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}

		// else, we assume it is the first column
		if (this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find the column predict";
			throw new IllegalArgumentException(errorString);
		}
		return this.curRow.get(0).toString();
	}

	private List<String> getInputList(int index) {
		List<String> retList = new ArrayList<String>();
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[index]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				retList.add(noun.getValue().toString());
			}
		} else {
			if (index == 1) throw new IllegalArgumentException("Please specify attributes.");
		}
		return retList;
	}

}
