package prerna.sablecc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import cern.colt.Arrays;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlBuilder;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.node.PScript;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;

public class PKQLRunner {
	
	public enum STATUS {SUCCESS, ERROR, INPUT_NEEDED}
	
	private STATUS currentStatus = PKQLRunner.STATUS.SUCCESS;
	private String currentString = ""; //TODO: what is this used for?
	private Object response = "PKQL processing complete";
	private String explain = "";
	private HashMap<String,Object> allAdditionalInfo = new HashMap<>();
	private String additionalInfoString = "";
	private Object returnData = null;
	
	private Map<String,String> newColumns = new HashMap<String,String>();
	private Map<String, Map<String,Object>> masterFeMap = new HashMap<String, Map<String,Object>>(); // this holds all active front end data. in the form panelId --> prop --> value

	//	private Map<String, List<Map<String, Object>>> expiredFeMaps =  new HashMap<String, List<Map<String,Object>>>();
	
	private Map<String, Object> activeFeMap; // temporally grabbed out of master
	private Translation translation;
	private List<Map> responseArray = new Vector<Map>();
	private Map<String, Map<String, Object>> varMap = new HashMap<String, Map<String, Object>>();
	LinkedList<PScript> pkqlToRun = new LinkedList<PScript>();
	List<String> unassignedVars = new Vector<String>();
	
//	private Map<String, Object> dataMap = new HashMap<>();
	
	private List<IPkqlMetadata> metadataResponse = new Vector<IPkqlMetadata>();
	
	// there is a getter for this
	// but we never set this... so not used
//	private String newInsightID;
	
	private Object dashboardMap;
	
	/**
	 * Runs a given pkql expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pkql expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pkql expression on
	 */
	public void runPKQL(String expression, IDataMaker frame) {
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;
		if(translation == null){
			translation = new Translation(frame, this);
		}

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
			currentStatus = PKQLRunner.STATUS.ERROR;
			currentString = expression;
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				response = "ERROR : " + e.getMessage();
			} else {
				response = "Invalid PKQL Statement";
			}
			storeResponse();
		}
		return;
	}
	
	/**
	 * Runs a given pkql expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pkql expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pkql expression on
	 */
	public void runPKQL(String expression) {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;
		if(translation == null){
			translation = new Translation(this);
		}

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
			currentStatus = PKQLRunner.STATUS.ERROR;
			currentString = expression;
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				response = "ERROR : " + e.getMessage();
			} else {
				response = "Invalid PKQL Statement";
			}
			storeResponse();
		}
		return;
	}
	
	public List<Map> getResults(){
		return this.responseArray;
	}
	
	public void storeResponse(){
		HashMap<String, Object> result = new HashMap<String, Object>();
		if(response instanceof Object[]) {
			StringBuilder builder = new StringBuilder();
			String retResponse = getStringFromObjectArray( (Object[]) response, builder);
//			result.put("result", StringEscapeUtils.escapeHtml(retResponse));
			result.put("result", retResponse);
		} else if(response instanceof List) {
			StringBuilder builder = new StringBuilder();
			String retResponse = getStringFromList( (List) response, builder);
//			result.put("result", StringEscapeUtils.escapeHtml(retResponse));
			result.put("result", retResponse);
		} else if(response instanceof double[]) {
//			result.put("result", StringEscapeUtils.escapeHtml(Arrays.toString( (double[]) response)));
			result.put("result", Arrays.toString( (double[]) response));
		} else if(response instanceof int[]) {
//			result.put("result", StringEscapeUtils.escapeHtml(Arrays.toString( (int[]) response)));
			result.put("result", Arrays.toString( (int[]) response));
		} 
		//TODO: should extrapolate this to a generic kind of expression iterator
		else if(response instanceof SqlBuilder) {
			StringBuilder builder = new StringBuilder();
			H2SqlExpressionIterator it = new H2SqlExpressionIterator((SqlBuilder) response);
			List<Object[]> first500 = new Vector<Object[]>();
//			first500.add(it.getHeaders());
			int counter = 0;
			while(it.hasNext() && counter < 500) {
				first500.add(it.next());
				counter++;
			}
			// if we only show a subset
			if(counter == 500) {
				builder.append("Only showing first 500 rows\n");
				it.close();
			}
			
			String retResponse = getStringFromList( first500, builder);
			result.put("result", retResponse);
		} else {
//			result.put("result", StringEscapeUtils.escapeHtml(response + ""));
			result.put("result", response + "");
		}
		result.put("status", currentStatus);
		result.put("command", currentString);
		result.put("returnData", returnData);
		// this is what frontend uses to diplay this piece in the recipe
		// this will definitely have to be built out to be more encompassing
		// maybe throw the current string through an English Translation class to get the label
		// for now we will just parse out the beginning
//		if(currentString.contains("(")){
//			result.put("label", currentString.substring(0, currentString.indexOf("(")));
//		}
//		else {
//			result.put("label", currentString);
//		}
		result.put("label", this.explain);
		
		// this is additional info passed to front-end for further processing.
		result.put("additionalInfo", additionalInfoString);
		
		// add it to the data associated with this panel if this is a panel pkql
		// also add the panel id to the master
		if(this.activeFeMap != null){
			//addFeData("additionalInfo", allAdditionalInfo, true);
			List<Object> feResults = (List<Object>) this.activeFeMap.get("pkqlData");
			feResults.add(result);
			result.put("panelId", this.activeFeMap.get("panelId"));
			this.activeFeMap = null;
		}
		responseArray.add(result);
	}

	private String getStringFromList(List objList, StringBuilder builder) {
		for(int i = 0; i < objList.size(); i++) {
			Object obj = objList.get(i);
			if(obj instanceof Object[]) {
				getStringFromObjectArray( (Object[]) obj, builder);
			} else if(obj instanceof double[]) {
				builder.append(Arrays.toString((double[]) obj));
			} else if(obj instanceof int[]) {
				builder.append(Arrays.toString((int[]) obj));
			} else {
				builder.append(obj);
			}
			builder.append("\n");
		}
		
		return builder.toString();
	}
	
	private String getStringFromObjectArray(Object[] objArray, StringBuilder builder) {
		builder.append("[");
		for(int i = 0; i < objArray.length; i++) {
			Object obj = objArray[i];
			if(obj instanceof Object[]) {
				getStringFromObjectArray((Object[]) obj, builder);
			} else {
				if(i == objArray.length-1) {
					if(obj instanceof double[]) {
						builder.append(Arrays.toString((double[]) obj));
					} else if(obj instanceof int[]) {
						builder.append(Arrays.toString((int[]) obj));
					} else {
						builder.append(obj);
					}
				} else {
					// since primitive arrays are stupid in java
					if(obj instanceof double[]) {
						builder.append(Arrays.toString((double[]) obj)).append(", ");
						builder.append("\n");
					} else if(obj instanceof int[]) {
						builder.append(Arrays.toString((int[]) obj)).append(", ");
						builder.append("\n");
					} else {
						builder.append(obj).append(", ");
					}
				}
			}
		}
		builder.append("]");
		
		return builder.toString();
	}
	
	public void setResponse(Object response) {
		this.response = response;
	}
	
	public void setReturnData(Object retData) {
		this.returnData = retData;
	}
	
	public void setStatus(PKQLRunner.STATUS currentStatus) {
		this.currentStatus = currentStatus;
	}
	
	public void setCurrentString(String string){
		this.currentString = string;
	}
	
//	public void setExplain(String explain) {
//		this.explain = explain;
//	}
	
	// Front end data needs to be tracked on a panel by panel basis
	// Each PKQL component adding FE Data though might not know which panel its dealing with
	// Therefore, on inAPanel I will set the ID and "open" the transaction to every other fe calls will be tracked to that panel
	// Then on outAPanel I will close it, signaling we are no longer dealing with this panel id
	//
	// might these be nested...? for now we'll assume not. could have potential though
	public void openFeDataBlock(String panelId){
		Map<String, Object> feBlock = openBlock(panelId);
		this.activeFeMap = feBlock;
	}
	
	private Map<String, Object> openBlock(String blockName){
		Map<String, Object> block = null;
		if(masterFeMap.containsKey(blockName)){
			block = masterFeMap.get(blockName);
		}
		else {
			block = new HashMap<String, Object>();
			List<Map> feResponses = new Vector<Map>();
			block.put("pkqlData", feResponses);
			masterFeMap.put(blockName, block);
		}
		return block;
	}
	
	public void addFeData(String key, Object value, boolean shouldOverride){
		addData(key, value, shouldOverride, this.activeFeMap);
	}
	
	public void addData(String key, Object value, boolean shouldOverride, Map<String, Object> map){
		// if should not override, need to keep as list
		if(!shouldOverride){
			List<Object> values = new Vector<Object>();
			if(map.containsKey(key)){
				values = (List<Object>) map.get(key);
			}
			values.add(value);
			map.put(key, values);
		}
		// if should override, just put it in
		else{
			map.put(key, value);
		}
	}
	
	public void addBeData(String key, Object value, boolean shouldOverride){
		Map<String, Object> beBlock = openBlock("data");
		addData(key, value, shouldOverride, beBlock);
	}
	
	public Map<String, Map<String, Object>> getFeData(){
		return this.masterFeMap;
	}
	
	public IDataMaker getDataFrame() {
		if(translation != null) {
			return translation.getDataFrame();
		}
		return null;
	}

	public void aggregateMetadata(List<IPkqlMetadata> metadataResponses) {
		// grab the metadata produced
		if(metadataResponses != null && !metadataResponses.isEmpty()) {
			this.metadataResponse.addAll(metadataResponses);
			// create the explanation and populate additional info map using the metadata responses
			int size = this.metadataResponse.size();
			this.explain = this.metadataResponse.get(size-1).getExplanation();
			if(this.metadataResponse.get(size-1).getAdditionalInfo() != null)
				allAdditionalInfo.putAll(this.metadataResponse.get(size - 1).getAdditionalInfo());;
			for(int i = size-2; i >= 0; i--) {
				IPkqlMetadata innerMeta = this.metadataResponse.get(i);
				String innerMetaPkql = innerMeta.getPkqlStr();
				String innerExplanation = innerMeta.getExplanation();
				if(this.explain.contains(innerMetaPkql)) {
					this.explain = this.explain.replace(innerMetaPkql, innerExplanation);
				}
				if(innerMeta.getAdditionalInfo() != null)
					allAdditionalInfo.putAll(innerMeta.getAdditionalInfo());
			}
		}
		Gson gson = new GsonBuilder().create();
		additionalInfoString = gson.toJson(allAdditionalInfo);
//		addFeData("additionalInfo", additionalInfoString, true);
		System.out.println("EXPLANATION IS ::: " + this.explain);
	}
	
//	/**
//	 * This method stores the current fe state and opens a new hash to store next state
//	 * This is called when a new state comes in and we don't want to lose the old one
//	 */
//	public void storeFeState() {
//		// remove the activeFeMap from masterFeMap
//		// put activeFeMap into expiredFeMap
//		// open new activeFeMap
//		
//		String panelId = (String) this.activeFeMap.get("panelId");
//		this.masterFeMap.remove(panelId);
//		List<Map<String, Object>> expiredMaps = new Vector<Map<String, Object>>();
//		if(this.expiredFeMaps.containsKey(panelId)){
//			expiredMaps = this.expiredFeMaps.get(panelId);
//		}
//		expiredMaps.add(this.activeFeMap);
//		this.activeFeMap = null;
//		this.openFeDataBlock(panelId);
//	}

	/**
	 * This method is for putting the state of the current panel into a new panel
	 * Used mostly for clone (a clone takes the current state)
	 * @param newId
	 */
	public void copyFeData(String newId) {
		Map<String, Object> copiedActiveMap = new HashMap<String, Object>();
		copiedActiveMap.putAll(this.activeFeMap);
		copiedActiveMap.remove("panelId");
		copiedActiveMap.remove("newPanelId");
		openFeDataBlock(newId);

		// copy the objects stored
		Gson gson = new Gson();
		String propCopy = gson.toJson(copiedActiveMap);
		Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
		this.activeFeMap.putAll(newProps);
	}

	public Object getFeData(String key) {
		return this.activeFeMap.get(key);
	}

	public Map<String,String> getNewColumns() {
		return newColumns;
	}

	public void setNewColumns(Map<String, String> newColumns) {
		this.newColumns = newColumns;
	}
	
	/**
	 * Set new map with engine and col name, to be filled with user inputted value once that comes in.
	 */
	public void addNewVariable(String varName, String engine, String col) {
		Map<String, Object> thisVarData = new HashMap<String, Object>();
		thisVarData.put(Constants.ENGINE, engine);
		thisVarData.put(Constants.TYPE, col);
		this.varMap.put(varName, thisVarData);
	}

	/*
	 * Adds a variable to the var map so that it can be retrieved with other pkqls
	 */
	public void setVariableValue(String varName, String expr) {
		if(!this.varMap.containsKey(varName)) {
			this.varMap.put(varName, new HashMap<>());
		}
		this.varMap.get(varName).put(Constants.VALUE, expr);
	}
	
	/*
	 * Adds a variable to the var map so that it can be retrieved with other pkqls
	 */
	public void setVariableValue(String varName, Object expr) {
		if (!this.varMap.containsKey(varName)) {
			this.varMap.put(varName, new HashMap<>());
		}
		this.varMap.get(varName).put(Constants.VALUE, expr);
	}

	// need a removal too :)
	public void removeVariable(String varName) {
		this.varMap.remove(varName); //.put(Constants.VALUE, expr);
	}
	
	
	
	/*
	 * Sets a reference to the variable map into the runner so that Translation
	 * can access it The main object sits on the Insight
	 */
	public void setVarMap(Map<String, Map<String, Object>> varMap){
		this.varMap = varMap;
	}
	
	/**
	 * Get the varMap for the given runner/insight.
	 * 
	 * @return	varMap	Map of params/var info, including engine, col name, and user inputted value
	 */
	public Map<String, Map<String, Object>> getVarMap() {
		return this.varMap;
	}

	/*
	 * Retrieves a variable from the var map
	 */
	public Object getVariableValue(String varName) {
		if(this.varMap.get(varName) != null) {
			return this.varMap.get(varName).get(Constants.VALUE);
		} else {
			return null;
		}
	}
	
	/**
	 * Get the info for a given var/param from the varMap.
	 * 
	 * @return	Map<String, Object> holding data for a given param (engine, col name, user inputted value)
	 */
	public Map<String, Object> getVariableData(String varName) {
		return this.varMap.get(varName);
	}

	public void clearResponses() {
//		this.currentStatus = PKQLRunner.STATUS.SUCCESS;
		this.currentString = "";
		this.response = "PKQL processing complete";
		this.responseArray = new Vector<Map>();
		this.masterFeMap = new HashMap<String, Map<String,Object>>(); // this holds all active front end data. in the form panelId --> prop --> value
		this.activeFeMap = null; // temporally grabbed out of master
		this.translation = null;
		this.newColumns = new HashMap<String,String>();
		
//		this.expiredFeMaps =  new HashMap<String, List<Map<String,Object>>>();
	}

//	public String getNewInsightID() {
//		return newInsightID;
//	}
	
//	public void setNewInsightID(String id) {
//		this.newInsightID = id;
//	}
	
	public Object getDashboardData() {
		return this.dashboardMap;
	}
	
	public void setDashBoardData(Object dashboardData) {
		this.dashboardMap = dashboardData;
	}
	
//	public Object getDataMap() {
//		return this.dataMap;
//	}
	
//	public void setDataMap(Map<String, Object> dataMap) {
//		String insightID = (String)dataMap.get("insightID");
//		this.dataMap.put(insightID, dataMap);
//	}
	
	public List<IPkqlMetadata> getMetadataResponse() {
		return this.metadataResponse;
	}
	
}