package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RClassificationAlgorithmReactor extends AbstractRFrameReactor {
	/**
	 * RunClassification(classify=[Species],attributes=["PetalLength","PetalWidth","SepalLength","SepalWidth"], panel=[0])
	 * RunClassification(classify=[race],attributes=["age","workclass","education","marital_status","relationship","sex","capital_gain","capital_loss","income"], panel=[0])
	 */
	private static final String CLASS_NAME = RClassificationAlgorithmReactor.class.getName();

	private static final String CLASSIFICATION_COLUMN = "classify";

	public RClassificationAlgorithmReactor() {
		this.keysToGet = new String[] { CLASSIFICATION_COLUMN, ReactorKeysEnum.ATTRIBUTES.getKey(),
				ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		String[] packages = new String[] { "data.table", "partykit", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getName();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		StringBuilder rsb = new StringBuilder();

		// figure out inputs
		String predictionCol = getClassificationColumn();
		String predictionCol_R = "predictionCol" + Utility.getRandomString(8);
		rsb.append(predictionCol_R + "<- \"" + predictionCol + "\";");

		List<String> attributes = getColumns();
		if (attributes.contains(predictionCol)) {
			attributes.remove(predictionCol);
		}
		if(attributes.isEmpty()) {
			throw new IllegalArgumentException("Must define at least one attribute that is not the dimension to classify");
		}
		String attributes_R = "attributes" + Utility.getRandomString(8);
		rsb.append(attributes_R + "<- " + RSyntaxHelper.createStringRColVec(attributes.toArray()) + ";");
		
		// check if there are filters on the frame. if so then need to run algorithm on subsetted data
		if(!frame.getFrameFilters().isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attributes);
			selectedCols.add(predictionCol);
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
		
		//validate that the count of unique values in the instance column != number of rows in the frame
		int nrows  = frame.getNumRows(targetDt);
		int uniqInstCount = this.rJavaTranslator.getInt("if (is.factor(" + targetDt + "$" + predictionCol + ")) "
				+ "length(levels(" + targetDt + "$" + predictionCol + ")) else length(unique(" + targetDt + "$" + predictionCol + "));");
		if (nrows == uniqInstCount) {
			throw new IllegalArgumentException("Values in the column to classify are all unique; classification algorithm is not applicable.");
		}
				
		// clustering r script
		String classificationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Classification.R";
		classificationScriptFilePath = classificationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + classificationScriptFilePath + "\");");
		String outputList_R = "outputList" + Utility.getRandomString(8);
		
		// set call to R function
		rsb.append(outputList_R + " <- getCTree( " + targetDt + "," + predictionCol_R + "," + attributes_R + ");");
		
		// execute R
		this.rJavaTranslator.runR(rsb.toString());
		
		String[] predictors = this.rJavaTranslator.getStringArray(outputList_R + "$predictors;");
		String accuracy = this.rJavaTranslator.getString(outputList_R + "$accuracy;");
		String[] ctreeArray = this.rJavaTranslator.getStringArray(outputList_R + "$tree;");

		//// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + outputList_R + "," + predictionCol_R + "," + attributes_R + "," + dtNameIF +  ",getCTree,getUsefulPredictors);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		
		if (ctreeArray == null || ctreeArray.length == 0) {
			Map<String, Object> vizData = new HashMap<String, Object>();
			vizData.put("name", "Decision Tree For " + predictionCol);
			vizData.put("layout", "Dendrogram");
			vizData.put("panelId", getPanelId());
			// make an empty map
			Map<String, Map> classificationMap = new HashMap<String, Map>();
			classificationMap.put("No Tree Generated", new HashMap());
			vizData.put("children", classificationMap);
			NounMetadata noun = new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_OUTPUT);
			noun.addAdditionalReturn(
					new NounMetadata("A decision tree could not be constructed for the requested dataset. Please retry with different data points.", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}

		Map<String, Object> vizData = new HashMap<String, Object>();
		vizData.put("name", "Decision Tree For " + predictionCol);
		vizData.put("layout", "Dendrogram");
		vizData.put("panelId", getPanelId());
		// add the actual data
		Map<String, Map> classificationMap = processTreeString(ctreeArray);
		vizData.put("children", classificationMap);
		// add the accuracy and predictors
		List<Map<String, String>> statList = new ArrayList<Map<String, String>>();
		Map<String, String> statHash = new HashMap<String, String>();
		statHash.put("Accuracy", accuracy);
		statList.add(statHash);
		if (predictors != null && predictors.length > 0){
			statHash = new Hashtable<String, String>();
			statHash.put("Relevant Predictors", String.join(", ", predictors));
			statList.add(statHash);
		}
		vizData.put("stats", statList);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Classification", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// now return this object
		NounMetadata noun = new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_OUTPUT);
		noun.addAdditionalReturn(
				new NounMetadata("Classification ran successfully!", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;		
	}
	
	private Map<String, Map> processTreeString(String[] ctreeArray) {
		Map<String, Map> treeMap = new HashMap<String, Map>();
		int index = Arrays.asList(ctreeArray).indexOf("[1] root");
		if (index == -1 ){
			// single node case
			index = Arrays.asList(ctreeArray).indexOf("Fitted party:") + 1;
			generateNodeTreeWithParenthesis(treeMap, ctreeArray[index]);
		} else {
			// multi node case
			String[] treeStringArr = new String[ctreeArray.length - index - 4];
			System.arraycopy(ctreeArray, index + 1, treeStringArr, 0, treeStringArr.length);
			for (int i = 0; i < treeStringArr.length; i++ ){
				treeStringArr[i] = treeStringArr[i].replaceAll("\\|\\s*\\[[0-9]+\\]\\s","");
			}
			generateTreeEndingWithParenthesis(treeMap, "", 0, treeStringArr, new Integer(0));
		}
		
		return treeMap;
	}
	
	private void generateNodeTreeWithParenthesis(Map<String, Map> rootMap, String nodeValue) {
		String lastRegex = "\\(n\\s=.*\\)" ;

		String key = nodeValue.substring(10).replaceFirst(lastRegex, "").trim();
		rootMap.put(key, new HashMap<String, Map>());
	}
	
	private void generateTreeEndingWithParenthesis(Map<String, Map> rootMap, String startKey, int subTreeIndex, String[] treeStringArr, Integer index) {
		String endRegex = ".*\\(n\\s=.*\\)" ;
		String lastRegex = "\\(n\\s=.*\\)" ;

		Map<String, Map> currTree = new HashMap<String, Map>();
		if(!startKey.isEmpty()) {
			rootMap.put(startKey, currTree);
		}
		
		for(; index < treeStringArr.length; index++) {
			String row = "";
			boolean newRowNeeded = false;
			while(newRowNeeded == false) {
				String rowSubstring = treeStringArr[index].split(": ")[0].replaceAll("^(\\|\\s*)*", "");
				if (rootMap.toString().contains(rowSubstring)){
					index++;
					if (index >= treeStringArr.length) return;
				} else {
					newRowNeeded = true;
					row = treeStringArr[index];
				}
			}

			if(!row.startsWith("|")) {
				if(subTreeIndex > 0) {
					index--;
					return;
				} 
				if(row.matches(endRegex)) {
					String[] keyVal = row.replaceFirst(lastRegex, "").split(": ");
					Map<String, Map> endMap = new HashMap<String, Map>();
					endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
					rootMap.put(keyVal[0].trim(), endMap);
				} else {
					String newRow = row.trim();
					currTree = new HashMap<String, Map>();
					rootMap.put(newRow, currTree);
					startKey = newRow;
					subTreeIndex = 0;
				}
			} else if(row.lastIndexOf("| ") != subTreeIndex) {
				//either done with the currtree - need to pull back out and assess whether rootMap needs to be grown out more
				return;
			} else if(row.matches(endRegex)) {
				String[] keyVal = row.substring(row.lastIndexOf("| ")+1, row.length()).trim().replaceFirst(lastRegex, "").split(": ");
				Map<String, Map> endMap = new HashMap<String, Map>();
				endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
				currTree.put(keyVal[0].trim(), endMap);
			} else {
				index++;
				String newKey = row.substring(row.lastIndexOf("| ")+1, row.length()).trim();
				// for a subtree to exist, there must be a new row after
				int newSubTreeIndex = treeStringArr[index].lastIndexOf("| ");
				generateTreeEndingWithParenthesis(currTree, newKey, newSubTreeIndex, treeStringArr, index);
			}
		}
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

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

	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		} else {
			throw new IllegalArgumentException("Attribute columns must be specified.");
		}

		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for (Object obj : values) {
			strValues.add(obj.toString());
		}

		return strValues;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[2]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CLASSIFICATION_COLUMN)) {
			return "The classification column";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
