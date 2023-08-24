package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.options.TaskOptions;
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
	private static final String TREE = "RF_TREE_999988888877777";
	private static final String CLASSIFICATION_COLUMN = "classify";
	private static final String SETTINGS = "settings";
	private static final String TREE_SETTINGS = "tree_settings";
	private static final String MODE = "mode";
	private static final String FILENAME = "filename";
	private static final String NULL_HANDLING = "null_handler";

	public RunRandomForestReactor() {
		this.keysToGet = new String[] { CLASSIFICATION_COLUMN, 
										ReactorKeysEnum.ATTRIBUTES.getKey(), 
										ReactorKeysEnum.SPACE.getKey(),
										ReactorKeysEnum.PANEL.getKey(), 
										SETTINGS, MODE, FILENAME, TREE_SETTINGS, NULL_HANDLING };
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		organizeKeys();
		
		// check for packages
		String[] packages = new String[] { "data.table", "ranger", "plyr", "missRanger" };
		this.rJavaTranslator.checkPackages(packages);

		// retrieve mode of the reactor call
		String mode = this.keyValue.get(this.keysToGet[5]);
		
		// get file to save model to or predict from or get tree from
		String fileName = this.keyValue.get(this.keysToGet[6]);
		// get asset path
		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true) + "/" + fileName;
		
		// initialize vars
		String targetDt = null;
		String nullHandlerType = null;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		StringBuilder sb = new StringBuilder();
		// initialize task
		List<NounMetadata> tasks = null;
		ITask barAndGridTask = null;
		ITask dendrogramTask = null;
		List<String> treeSettings = null;
		NounMetadata noun = new NounMetadata(barAndGridTask, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		
		if (mode.equals("build")) { // check if the model needs to be built
			// get build inputs
			String depVar = getClassificationColumn();		
			List<String> attributes = getInputList(1);
			attributes.remove(depVar);
			String indVars = "c(";
			for (int i = 0; i < attributes.size(); i++) {
				if (i == attributes.size() - 1) {
					indVars += "\"" + attributes.get(i) + "\"";
				} else {
					indVars += "\"" + attributes.get(i) + "\", ";
				}
			}
			indVars += ")";
			List<String> advancedSettings = getInputList(4);
			nullHandlerType = getNullHandleType();
			String sampleSize = advancedSettings.get(0);
			String sampleBlocks = advancedSettings.get(1);
			String treeDepth = advancedSettings.get(2);
			
			// get frame
			RDataTable frame = (RDataTable) getFrame();
			String dtName = frame.getName();
			OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
			boolean implicitFilter = false;
			// check if there are filters on the frame. if so then need to run algorithm on subsetted data
			if(!frame.getFrameFilters().isEmpty()) {
				// create a new qs to retrieve filtered frame
				SelectQueryStruct qs = new SelectQueryStruct();
				List<String> selectedCols = new ArrayList<String>(attributes);
				selectedCols.add(depVar);
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
			targetDt = implicitFilter ? dtNameIF : dtName;

			// source r scripts
			String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\random_forest.R";
			scriptFilePath = scriptFilePath.replace("\\", "/");
			sb.append("source(\"" + scriptFilePath + "\"); ");
			String imputeFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\ImputeData.R";
			imputeFilePath= imputeFilePath.replace("\\", "/");
			sb.append("source(\"" + imputeFilePath + "\"); ");
			logger.info("All inputs loaded.");
			
			// build model
			logger.info("Building random forest model...");
			// set call to R function
			sb.append(RF_VARIABLE + " <- rfmodel_mgr( ind_vars=" + indVars + 
													", dep_var=\"" + depVar + 
													"\", trainFrame=" + targetDt + 
													", model_fn=\"" + assetFolder + 
													"\", mis_data=\"" + nullHandlerType + 
													"\", sample_size=" + sampleSize + 
													", sample_blocks=" + sampleBlocks + 
													", depth=" + treeDepth + ");");
			sb.append(COLS + " <- colnames(" + RF_VARIABLE + "[[2]]);");
			sb.append(TREE + " <- get_tree(\"" + assetFolder + "\", 1, 1);");
			// execute R
			this.rJavaTranslator.runR(sb.toString());

			// construct map of model summary data to return to front end
			Map<String, Object> retMap = new HashMap<>();
			double[] predError = this.rJavaTranslator.getDoubleArray(RF_VARIABLE + "[[1]]");
			if (predError != null) {
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
				double[] treeOptions = new double[2];
				treeOptions[0] = this.rJavaTranslator.getDouble(RF_VARIABLE + "[[4]]");
				treeOptions[1] = this.rJavaTranslator.getDouble(RF_VARIABLE + "[[5]]");
				retMap.put("tree_options", treeOptions);
				retMap.put("filename", fileName);
				String[] headerOrdering = {"Variables", "Importance"};
				List<Object[]> retBarPlotOutput = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE + "[[3]]", headerOrdering);
				
				String[] dendrogramHeaders = this.rJavaTranslator.getStringArray("colnames(" + 	TREE + ");");
				List<Object[]> retDendrogramOutput = new Vector<>(500);
				for (int i = 1; i < dendrogramHeaders.length+1; i++) {
					String[] values = this.rJavaTranslator.getStringArray(TREE + "[" + i + ",];");
					retDendrogramOutput.add(values);
				}
		
				// create and return a task
				String barChartPanelId = getPanelId();
				String dendrogramChartPanelId = String.valueOf(Integer.parseInt(barChartPanelId)+1);
				barAndGridTask = ConstantTaskCreationHelper.getBarChartInfo(barChartPanelId, "Variables", "Importance Values", retBarPlotOutput);
				this.insight.getTaskStore().addTask(barAndGridTask);
				
				dendrogramTask = getDendrogramInfo(dendrogramChartPanelId, dendrogramHeaders, retDendrogramOutput);
				this.insight.getTaskStore().addTask(dendrogramTask);

				// construct noun to create visualization and return model summary data
				noun = new NounMetadata(barAndGridTask, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
				noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Random Forest model was built and saved to APP assets"));
				NounMetadata noun2 = new NounMetadata(dendrogramTask, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
				NounMetadata noun3 = new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
				tasks = new Vector<>();
				tasks.add(noun);
				tasks.add(noun2);
				tasks.add(noun3);
			} else {
				logger.info("Model build unsuccessful.");
				SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("Model build was unsuccessful"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		} else if (mode.equals("predict")) { // check if values need to be predicted from an existing model
			// get predict inputs
			nullHandlerType = getNullHandleType();
			
			// get frame
			RDataTable frame = (RDataTable) getFrame();
			targetDt = frame.getName();
			
			// source r scripts
			String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\random_forest.R";
			scriptFilePath = scriptFilePath.replace("\\", "/");
			sb.append("source(\"" + scriptFilePath + "\"); ");
			String imputeFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\ImputeData.R";
			imputeFilePath= imputeFilePath.replace("\\", "/");
			sb.append("source(\"" + imputeFilePath + "\"); ");
			logger.info("All inputs loaded.");
			
			logger.info("Predicting from random forest model...");
			// set call to R function
			sb.append(RF_VARIABLE + " <- predict_rfmodel( newFrame=" + targetDt + ", model_fn=\"" + assetFolder + "\", mis_data=\"" + nullHandlerType + "\");");
			// execute R
			this.rJavaTranslator.runR(sb.toString());
			
			// create and return grid task if predict was successful
			double[] runResult = this.rJavaTranslator.getDoubleArray(RF_VARIABLE + "[[1]]");
			if (runResult[0] == 0) {
				logger.info("Updating grid...");
				String panelId = getPanelId();
				String[] headerOrdering = getColumns(RF_VARIABLE + "[[3]]");
				List<Object[]> retOutput = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE + "[[3]]", headerOrdering);
				barAndGridTask = ConstantTaskCreationHelper.getGridData(panelId, headerOrdering, retOutput);
				this.insight.getTaskStore().addTask(barAndGridTask);
				// construct noun to update grid
				noun = new NounMetadata(barAndGridTask, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
				noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Prediction from Random Forest model successful"));
			} else {
				logger.info("Prediction unsuccessful.");
				String[] columnsRequired = this.rJavaTranslator.getStringArray(RF_VARIABLE + "[[2]]");
				String[] columnsProvided = getColumns(RF_VARIABLE + "[[3]]");
				List<String> columnsNeeded = new ArrayList<>();
				for (int i = 0; i < columnsRequired.length; i++) {
					if (!Arrays.asList(columnsProvided).contains(columnsRequired[i])) {
						columnsNeeded.add(columnsRequired[i]);
					}
				}
				SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage("Missing data or columns are missing: " + Arrays.toString(columnsNeeded.toArray())));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		} else if (mode.equals("updateTree")) { // check if tree needs to be updated
			// get update tree inputs
			treeSettings = getInputList(7);
			
			logger.info("All inputs loaded.");
			
			// update tree
			logger.info("Updating tree...");
			String dendrogramChartPanelId = getPanelId();
			
			if (treeSettings == null || treeSettings.isEmpty()) {
				throw new IllegalArgumentException("Unable to generate tree to view"); 
			} else {
				sb.append(TREE + " <- get_tree(model_fn=\"" + assetFolder + "\", model_nbr=" + treeSettings.get(1) + ", tree_nbr="+ treeSettings.get(2) +");");
				// execute R
				this.rJavaTranslator.runR(sb.toString());
			}
			String[] dendrogramHeaders = this.rJavaTranslator.getStringArray("colnames(" + 	TREE + ");");
			List<Object[]> retDendrogramOutput = new Vector<>(500);
			for (int i = 1; i < dendrogramHeaders.length+1; i++) {
				String[] values = this.rJavaTranslator.getStringArray(TREE + "[" + i + ",];");
				retDendrogramOutput.add(values);
			}
			dendrogramTask = getDendrogramInfo(dendrogramChartPanelId, dendrogramHeaders, retDendrogramOutput);
			this.insight.getTaskStore().addTask(dendrogramTask);
			
			noun = new NounMetadata(dendrogramTask, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Dendrogram updated"));
		}
		
		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + dtNameIF + ",getRF,getRFResults);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		if (mode.equals("build")) {
			return new NounMetadata(tasks, PixelDataType.VECTOR, PixelOperationType.VECTOR);
		} else {
			return noun;
		}
	}


	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods//////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getNullHandleType() {
		// an action specifying how to handle missing data (options:
		// "impute","drop","as_is")
		GenRowStruct alphaGrs = this.store.getNoun(NULL_HANDLING);
		if (alphaGrs != null && alphaGrs.size() > 0) {
			return alphaGrs.get(0).toString().toLowerCase();
		}
		// default to .05
		return "as_is";
	}

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
		if(columnGrs != null && columnGrs.size() > 0) {
			return columnGrs.get(0).toString();
		}
		return null;
	}

	private String getClassificationColumn() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(CLASSIFICATION_COLUMN);
		if(columnGrs != null && columnGrs.size() > 0) {
			return columnGrs.get(0).toString();
		}

		// else, we assume it is the first column
		if (this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find the column predict";
			throw new IllegalArgumentException(errorString);
		}
		return this.curRow.get(0).toString();
	}

	private List<String> getInputList(int index) {
		List<String> retList = new ArrayList<>();
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
	
	private ITask getDendrogramInfo(String panelId, String[] headers, List<Object[]> dataValues) {
		// create the weird object the FE needs to paint a bar chart
				ConstantDataTask task = new ConstantDataTask();
				task.setId("TEMP_ID");
				Map<String, Object> returnData = new Hashtable<>();
				returnData.put("values", dataValues);
				returnData.put("headers", headers);
				task.setOutputData(returnData);
				
				//create maps to set the task options
				//main map that will be passed into the setTaskOptions method
				Map<String, Object> mapOptions = new HashMap<>();
				
				//this map (keyMap) comprises the mapping of both layout and alignment
				Map<String, Object> keyMap = new HashMap<>(); 
				keyMap.put("layout", "Dendrogram");
				
				//within keyMap, we need a map to store the maps that comprise alignment
				Map<String, Object> alignmentMap = new HashMap<>();
				alignmentMap.put("dimension",  headers);
				alignmentMap.put("facet", "[]");
				
				keyMap.put("alignment", alignmentMap);
				
				mapOptions.put(panelId, keyMap);
				//the final mapping looks like this:
				//taskOptions={0={layout=Column, alignment={tooltip=[], labels=[col1], value=[col2]}}}
				
				//set task options
				task.setTaskOptions(new TaskOptions(mapOptions));

				List<Map<String, Object>> vizHeaders = new Vector<>();
				for (int i = 0; i < headers.length; i++) {
					Map<String, Object> labelMap = new Hashtable<>();
					labelMap.put("alias", headers[i]);
					labelMap.put("derived", false);
					labelMap.put("header", headers[i]);
					labelMap.put("type", "STRING");
					vizHeaders.add(labelMap);
				}
				task.setHeaderInfo(vizHeaders);

				Map<String, Object> formatMap = new Hashtable<>();
				formatMap.put("type", "TABLE");
				task.setFormatMap(formatMap);
				
				return task;
	}

}
