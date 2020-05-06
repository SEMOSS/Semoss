package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
		List<String> advanced_settings = getInputList(4);
		String mode = this.keyValue.get(this.keysToGet[5]);
		String file_name = this.keyValue.get(this.keysToGet[6]);
		String null_handler_type = getNullHandleType();
		
		// initialize vars
		String targetDt = null;
		String assetFolder = null;
		String sample_size = null;
		String sample_blocks = null;
		String tree_depth = null;
		// initialize task
		ITask taskData1 = null;
		ITask taskData2 = null;
		List<String> tree_settings = null;
		NounMetadata noun = new NounMetadata(taskData1, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		
		// get asset path
		// assets path
		String space = this.keyValue.get(this.keysToGet[2]);
		assetFolder = AssetUtility.getAssetBasePath(this.insight, space, AbstractSecurityUtils.securityEnabled()) + "/" + file_name ;
		
		if (!mode.equals("updateTree")) {
			sample_size = advanced_settings.get(0);
			sample_blocks = advanced_settings.get(1);
			tree_depth = advanced_settings.get(2);
			
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
			targetDt = implicitFilter ? dtNameIF : dtName;
			
			// random forest r script
			String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\random_forest.R";
			scriptFilePath = scriptFilePath.replace("\\", "/");
			sb.append("source(\"" + scriptFilePath + "\"); ");
			String imputeFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\ImputeData.R";
			imputeFilePath= imputeFilePath.replace("\\", "/");
			sb.append("source(\"" + imputeFilePath + "\"); ");
			
			logger.info("All inputs loaded.");
		} else {
			tree_settings = getInputList(7);
		}
		
		if (mode.equals("build")) { // check if the model needs to be built
			logger.info("Building random forest model...");
			// set call to R function
			sb.append(RF_VARIABLE + " <- rfmodel_mgr( ind_vars=" + ind_vars + ", dep_var=\"" + dep_var + "\", trainFrame=" + targetDt + ", model_fn=\"" + assetFolder + "\", mis_data=\"" + null_handler_type + "\", sample_size=" + sample_size + ", sample_blocks=" + sample_blocks + ", depth=" + tree_depth + ");");
			sb.append(COLS + " <- colnames(" + RF_VARIABLE + "[[2]]);");
			sb.append(TREE + " <- get_tree(\"" + assetFolder + "\", 1, 1);");
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
			double[] tree_options = new double[2];
			tree_options[0] = this.rJavaTranslator.getDouble(RF_VARIABLE + "[[4]]");
			tree_options[1] = this.rJavaTranslator.getDouble(RF_VARIABLE + "[[5]]");
			retMap.put("tree_options", tree_options);
			retMap.put("filename", file_name);
			String[] headerOrdering = {"Variables", "Importance"};
			List<Object[]> retBarPlotOutput = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE + "[[3]]", headerOrdering);
			
			String[] dendrogramHeaders = this.rJavaTranslator.getStringArray("colnames(" + 	TREE + ");");
			List<Object[]> retDendrogramOutput = new Vector<Object[]>(500);
			for (int i = 1; i < dendrogramHeaders.length+1; i++) {
				String[] values = this.rJavaTranslator.getStringArray(TREE + "[" + i + ",];");
				retDendrogramOutput.add(values);
			}
	
			// create and return a task
			String barChartPanelId = getPanelId();
			String dendrogramChartPanelId = String.valueOf(Integer.parseInt(barChartPanelId)+1);
			taskData1 = ConstantTaskCreationHelper.getBarChartInfo(barChartPanelId, "Variables", "Importance Values", retBarPlotOutput);
			this.insight.getTaskStore().addTask(taskData1);
			
			taskData2 = getDendrogramInfo(dendrogramChartPanelId, dendrogramHeaders, retDendrogramOutput);
			this.insight.getTaskStore().addTask(taskData2);

			// construct noun to create visualization and return model summary data
			noun = new NounMetadata(taskData1, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Random Forest model was built and saved to APP assets"));
			NounMetadata noun2 = new NounMetadata(taskData2, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
			NounMetadata noun3 = new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
			List<NounMetadata> tasks = new Vector<NounMetadata>();
			tasks.add(noun);
			tasks.add(noun2);
			tasks.add(noun3);
			return new NounMetadata(tasks, PixelDataType.VECTOR, PixelOperationType.VECTOR);
		} else if (mode.equals("predict")) { // check if values need to be predicted from an existing model
			logger.info("Predicting from random forest model...");
			// set call to R function
			sb.append(RF_VARIABLE + " <- predict_rfmodel( newFrame=" + targetDt + ", model_fn=\"" + assetFolder + "\", mis_data=\"" + null_handler_type + "\");");
			// execute R
			this.rJavaTranslator.runR(sb.toString());
			
			// clean up r temp variables
			StringBuilder cleanUpScript = new StringBuilder();
			cleanUpScript.append("rm(" + dtNameIF + ",getRF,getRFResults);");
			cleanUpScript.append("gc();");
			this.rJavaTranslator.runR(cleanUpScript.toString());
			
			// create and return grid task if predict was successful
			double[] runResult = this.rJavaTranslator.getDoubleArray(RF_VARIABLE + "[[1]]");
			if (runResult[0] == 0) {
				logger.info("Updating grid...");
				String panelId = getPanelId();
				String[] headerOrdering = getColumns(RF_VARIABLE + "[[3]]");
				List<Object[]> retOutput = this.rJavaTranslator.getBulkDataRow(RF_VARIABLE + "[[3]]", headerOrdering);
				taskData1 = ConstantTaskCreationHelper.getGridData(panelId, headerOrdering, retOutput);
				this.insight.getTaskStore().addTask(taskData1);
				// construct noun to update grid
				noun = new NounMetadata(taskData1, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
				noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Prediction from Random Forest model successful"));
			} else {
				logger.info("Missing columns, run unsuccessful.");
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
			logger.info("Updating tree...");
			String dendrogramChartPanelId = getPanelId();
			sb.append(TREE + " <- get_tree(model_fn=\"" + assetFolder + "\", model_nbr=" + tree_settings.get(1) + ", tree_nbr="+ tree_settings.get(2) +");");
			// execute R
			this.rJavaTranslator.runR(sb.toString());
			
			String[] dendrogramHeaders = this.rJavaTranslator.getStringArray("colnames(" + 	TREE + ");");
			List<Object[]> retDendrogramOutput = new Vector<Object[]>(500);
			for (int i = 1; i < dendrogramHeaders.length+1; i++) {
				String[] values = this.rJavaTranslator.getStringArray(TREE + "[" + i + ",];");
				retDendrogramOutput.add(values);
			}
			
			taskData2 = getDendrogramInfo(dendrogramChartPanelId, dendrogramHeaders, retDendrogramOutput);
			this.insight.getTaskStore().addTask(taskData2);
			
			noun = new NounMetadata(taskData2, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Dendrogram updated"));
		}
		return noun;
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
		if (alphaGrs != null) {
			if (alphaGrs.size() > 0) {
				return alphaGrs.get(0).toString().toLowerCase();
			}
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
	
	private ITask getDendrogramInfo(String panelId, String[] headers, List<Object[]> dataValues) {
		// create the weird object the FE needs to paint a bar chart
				ConstantDataTask task = new ConstantDataTask();
				task.setId("TEMP_ID");
				Map<String, Object> returnData = new Hashtable<String, Object>();
				returnData.put("values", dataValues);
				returnData.put("headers", headers);
				task.setOutputData(returnData);
				
				//create maps to set the task options
				//main map that will be passed into the setTaskOptions method
				Map<String, Object> mapOptions = new HashMap<String, Object>();
				
				//this map (keyMap) comprises the mapping of both layout and alignment
				Map<String, Object> keyMap = new HashMap<String, Object>(); 
				keyMap.put("layout", "Dendrogram");
				
				//within keyMap, we need a map to store the maps that comprise alignment
				Map<String, Object> alignmentMap = new HashMap<String, Object>();
				alignmentMap.put("dimension",  headers);
				alignmentMap.put("facet", "[]");
				
				keyMap.put("alignment", alignmentMap);
				
				mapOptions.put(panelId, keyMap);
				//the final mapping looks like this:
				//taskOptions={0={layout=Column, alignment={tooltip=[], labels=[col1], value=[col2]}}}
				
				//set task options
				task.setTaskOptions(new TaskOptions(mapOptions));

				List<Map<String, Object>> vizHeaders = new Vector<Map<String, Object>>();
				for (int i = 0; i < headers.length; i++) {
					Map<String, Object> labelMap = new Hashtable<String, Object>();
					labelMap.put("alias", headers[i]);
					labelMap.put("derived", false);
					labelMap.put("header", headers[i]);
					labelMap.put("type", "STRING");
					vizHeaders.add(labelMap);
				}
				task.setHeaderInfo(vizHeaders);

				Map<String, Object> formatMap = new Hashtable<String, Object>();
				formatMap.put("type", "TABLE");
				task.setFormatMap(formatMap);
				
				return task;
	}

}
