package prerna.sablecc;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.lang.StringEscapeUtils;

import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;

public class PKQLRunner {
	
	enum Status {SUCCESS, ERROR}
	
	private String currentStatus = "success";
	private String currentString = "";
	private Object response = "PKQL processing complete";
	private Map<String,String> newColumns = new HashMap<String,String>();
	private Map<String, Map<String,Object>> masterFeMap = new HashMap<String, Map<String,Object>>(); // this holds all active front end data. in the form panelId --> prop --> value
	private Map<String, List<Map<String, Object>>> expiredFeMaps =  new HashMap<String, List<Map<String,Object>>>();
	private Map<String, Object> activeFeMap;
	private Translation2 translation;
	private List<Map> responseArray = new Vector<Map>();
	
	public void runPKQL(String expression, ITableDataFrame f) {
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;

		try {
			tree = p.parse();
			// Apply the translation.
			translation = new Translation2(f, this);
			tree.apply(translation);

		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
			currentStatus = "error";
			currentString = expression;
			response = "Invalid PKQL Statement";
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
			result.put("result", StringEscapeUtils.escapeHtml(retResponse));
		} else if(response instanceof List) {
			StringBuilder builder = new StringBuilder();
			String retResponse = getStringFromList( (List) response, builder);
			result.put("result", StringEscapeUtils.escapeHtml(retResponse));
		} else if(response instanceof double[]) {
			result.put("result", StringEscapeUtils.escapeHtml(Arrays.toString( (double[]) response)));
		} else if(response instanceof int[]) {
			result.put("result", StringEscapeUtils.escapeHtml(Arrays.toString( (int[]) response)));
		} else { 
			result.put("result", StringEscapeUtils.escapeHtml(response + ""));
		}
		result.put("status", currentStatus);
		result.put("command", currentString);
		
		// add it to the data associated with this panel if this is a panel pkql
		// also add the panel id to the master
		if(this.activeFeMap != null){
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
	
	public void setStatus(String currentStatus) {
		this.currentStatus = currentStatus;
	}
	
	public void setCurrentString(String string){
		this.currentString = string;
	}
	
	// Front end data needs to be tracked on a panel by panel basis
	// Each PKQL component adding FE Data though might not know which panel its dealing with
	// Therefore, on inAPanel I will set the ID and "open" the transaction to every other fe calls will be tracked to that panel
	// Then on outAPanel I will close it, signaling we are no longer dealing with this panel id
	//
	// might these be nested...? for now we'll assume not. could have potential though
	public void openFeDataBlock(String panelId){
		Map<String, Object> feBlock = null;
		if(masterFeMap.containsKey(panelId)){
			feBlock = masterFeMap.get(panelId);
		}
		else {
			feBlock = new HashMap<String, Object>();
			List<Map> feResponses = new Vector<Map>();
			feBlock.put("pkqlData", feResponses);
			masterFeMap.put(panelId, feBlock);
		}
		this.activeFeMap = feBlock;
	}
	
	public void addFeData(String key, Object value, boolean shouldOverride){
		// if should not override, need to keep as list
		if(!shouldOverride){
			List<Object> values = new Vector<Object>();
			if(this.activeFeMap.containsKey(key)){
				values = (List<Object>) activeFeMap.get(key);
			}
			values.add(value);
			this.activeFeMap.put(key, values);
		}
		// if should override, just put it in
		else{
			this.activeFeMap.put(key, value);
		}
	}
	
	public Map<String, Map<String, Object>> getFeData(){
		return this.masterFeMap;
	}
	
	public ITableDataFrame getDataFrame() {
		return translation.getDataFrame();
	}

	/**
	 * This method stores the current fe state and opens a new hash to store next state
	 * This is called when a new state comes in and we don't want to lose the old one
	 */
	public void storeFeState() {
		// remove the activeFeMap from masterFeMap
		// put activeFeMap into expiredFeMap
		// open new activeFeMap
		
		String panelId = (String) this.activeFeMap.get("panelId");
		this.masterFeMap.remove(panelId);
		List<Map<String, Object>> expiredMaps = new Vector<Map<String, Object>>();
		if(this.expiredFeMaps.containsKey(panelId)){
			expiredMaps = this.expiredFeMaps.get(panelId);
		}
		expiredMaps.add(this.activeFeMap);
		this.activeFeMap = null;
		this.openFeDataBlock(panelId);
	}

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
		this.activeFeMap.putAll(copiedActiveMap);
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
}