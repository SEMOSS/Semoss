package prerna.sablecc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.analysis.DepthFirstAdapter;
import prerna.sablecc.node.AAddColumn;
import prerna.sablecc.node.AAlphaWordOrNum;
import prerna.sablecc.node.AApiBlock;
import prerna.sablecc.node.AApiImportBlock;
import prerna.sablecc.node.AApiTerm;
import prerna.sablecc.node.AColCsv;
import prerna.sablecc.node.AColDef;
import prerna.sablecc.node.AColTable;
import prerna.sablecc.node.AColWhere;
import prerna.sablecc.node.AColopScript;
import prerna.sablecc.node.AConfiguration;
import prerna.sablecc.node.ACsvRow;
import prerna.sablecc.node.ACsvTable;
import prerna.sablecc.node.ACsvTableImportBlock;
import prerna.sablecc.node.ADashboardJoin;
import prerna.sablecc.node.ADataFrame;
import prerna.sablecc.node.ADataconnect;
import prerna.sablecc.node.ADataconnectdb;
import prerna.sablecc.node.ADatatype;
import prerna.sablecc.node.ADecimal;
import prerna.sablecc.node.ADivExpr;
import prerna.sablecc.node.AEExprExpr;
import prerna.sablecc.node.AExprGroup;
import prerna.sablecc.node.AExprInputOrExpr;
import prerna.sablecc.node.AExprRow;
import prerna.sablecc.node.AExprScript;
import prerna.sablecc.node.AFilterColumn;
import prerna.sablecc.node.AFlexSelectorRow;
import prerna.sablecc.node.AHelpScript;
import prerna.sablecc.node.AImportData;
import prerna.sablecc.node.AInputInputOrExpr;
import prerna.sablecc.node.AInsightidJoinParam;
import prerna.sablecc.node.AKeyvalue;
import prerna.sablecc.node.AMathFun;
import prerna.sablecc.node.AMathFunTerm;
import prerna.sablecc.node.AMathParam;
import prerna.sablecc.node.AMinusExpr;
import prerna.sablecc.node.AModExpr;
import prerna.sablecc.node.AMultExpr;
import prerna.sablecc.node.ANumWordOrNum;
import prerna.sablecc.node.ANumberTerm;
import prerna.sablecc.node.AOpenData;
import prerna.sablecc.node.AOpenDataInputOrExpr;
import prerna.sablecc.node.AOpenDataJoinParam;
import prerna.sablecc.node.APanelClone;
import prerna.sablecc.node.APanelClose;
import prerna.sablecc.node.APanelComment;
import prerna.sablecc.node.APanelCommentEdit;
import prerna.sablecc.node.APanelCommentRemove;
import prerna.sablecc.node.APanelConfig;
import prerna.sablecc.node.APanelLookAndFeel;
import prerna.sablecc.node.APanelTools;
import prerna.sablecc.node.APanelViz;
import prerna.sablecc.node.APanelopScript;
import prerna.sablecc.node.APastedData;
import prerna.sablecc.node.APastedDataBlock;
import prerna.sablecc.node.APastedDataImportBlock;
import prerna.sablecc.node.APlusExpr;
import prerna.sablecc.node.ARelationDef;
import prerna.sablecc.node.ARemoveData;
import prerna.sablecc.node.ASetColumn;
import prerna.sablecc.node.ASplitColumn;
import prerna.sablecc.node.ATermExpr;
import prerna.sablecc.node.ATermGroup;
import prerna.sablecc.node.AUnfilterColumn;
import prerna.sablecc.node.AUserInput;
import prerna.sablecc.node.AVarDef;
import prerna.sablecc.node.AVarTerm;
import prerna.sablecc.node.AVariableJoinParam;
import prerna.sablecc.node.AVarop;
import prerna.sablecc.node.AVaropScript;
import prerna.sablecc.node.Node;
import prerna.sablecc.node.PScript;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;

import com.google.gson.Gson;

public class Translation extends DepthFirstAdapter {
	// this is the third version of this shit I am building
	// I need some way of having logical points for me to know when to start another reactor
	// for instance I could have an expr term within a math function which itself could be within another expr term
	// the question then is do I have 2 expr terms etc. 
	// so I start the first expr term
	// I start assimilating
	// get to a point where I start a new one
	// my vector tells me that
	// the init and deinit should take care of it I bet ?
	// how do I know when I am done ?
	// it has to be invoked at the last level
	Hashtable <String, Object> reactorHash = null;
	IScriptReactor curReactor = null;

	// reactor vector
	Vector <Hashtable<String, Object>> reactorHashHistory = new Vector <Hashtable<String, Object>>(); 
	Vector <Hashtable <String, Object>> reactorStack = new Vector<Hashtable <String, Object>>();

	// set of reactors
	// which serves 2 purposes 
	// a. Where to initiate
	// b. What is the name of the reactor

	Map <String, String> reactorNames = new Hashtable<String, String>();
	IDataMaker frame = null;
	PKQLRunner runner = null;

	public Translation() { // Test Constructor
		frame = new TinkerFrame();
		this.reactorNames = frame.getScriptReactors();
		//		((TinkerFrame)frame).tryCustomGraph();
		this.runner = new PKQLRunner();
		//		fillReactors();
	}
	/**
	 * Constructor that takes in the dataframe that it will perform its calculations off of and the runner that invoked the translation
	 * @param frame IDataMaker
	 * @param runner PKQLRunner: holds response from PKQL script and the status of whether the script errored or not
	 */
	public Translation(IDataMaker frame, PKQLRunner runner) {
		// now get the data from tinker
		this.frame = frame;
		this.reactorNames = frame.getScriptReactors();
		this.runner = runner;
		//		fillReactors();
	}

	// MOVED TO THE DATA FRAMES SO THEY HAVE CONTROL
	//	private void fillReactors() { // TODO: use PKQLReactor enum
	//		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
	//		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
	//		reactorNames.put(PKQLReactor.MATH_FUN.toString(), "prerna.sablecc.MathReactor");
	//		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
	//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
	//		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
	//		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
	//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
	//		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
	//		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
	//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
	//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.ImportDataReactor");
	//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
	//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
	//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
	//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
	//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
	//	}

	@Override
	public void inAConfiguration(AConfiguration node){
		System.out.println(node.toString());
		runner.pkqlToRun.addAll(node.getScript());

		int index = 0;
		while(index < runner.pkqlToRun.size())
		{
			PScript script = runner.pkqlToRun.get(index);
			if(runner.unassignedVars.isEmpty() || script instanceof AVaropScript){ // if no vars are unassigned.. we are good. otherwise we only look for their assignment
				//        		PVarop varop = ((AVaropScript)script).getVarop();
				runner.pkqlToRun.remove(index).apply(this);
				index = 0;
			}
			else{
				System.out.println("Waiting for var(s) to be defined : " + runner.unassignedVars.toString());
				runner.setCurrentString("Waiting for var(s) to be defined : " + runner.unassignedVars.toString());
				runner.setStatus(STATUS.INPUT_NEEDED);
				postProcess(script);
				index++;
			}
		}
		outAConfiguration(node);
		// make sure we don't re-process everything on the node... set it empty
		node.setScript(new LinkedList());
	}

	public void initReactor(String myName) {
		String parentName = null;
		if(reactorHash != null)
			// I am not sure I need to add element here
			// I need 2 things in here
			// I need the name of a parent i.e. what is my name and my parent name
			// actually I just need my name
			parentName = (String)reactorHash.get("SELF");
		reactorHash = new Hashtable<String, Object>();
		if(parentName != null)
			reactorHash.put("PARENT_NAME", parentName);
		reactorHash.put("SELF", myName);
		reactorStack.addElement(reactorHash);

		// I should also possibly initialize the reactor here
		try {
			String reactorName = reactorNames.get(myName);
			curReactor = (IScriptReactor)Class.forName(reactorName).newInstance();
			curReactor.put(PKQLEnum.G, frame);
			// this is how I can get access to the parent when that happens
			reactorHash.put(myName, curReactor);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}

	public Hashtable <String, Object> deinitReactor(String myName, String input, String output, boolean put) {
		Hashtable <String, Object> thisReactorHash = reactorStack.lastElement();
		reactorStack.remove(thisReactorHash);
		IScriptReactor thisReactor = (IScriptReactor)thisReactorHash.get(myName);
		// this is still one level up
		thisReactor.process();
		Object value = 	thisReactor.getValue(input);
		System.out.println("Value is .. " + value);

		if(reactorStack.size() > 0) {
			reactorHash = reactorStack.lastElement();
			// also set the cur reactor
			String parent = (String)thisReactorHash.get("PARENT_NAME");

			// if the parent is not null
			if(parent != null && reactorHash.containsKey(parent)) {
				curReactor = (IScriptReactor)reactorHash.get(parent);
				if(put)
					curReactor.put(output, value);
				else
					curReactor.set(output, value);
			}
		}else if(reactorHash.size() > 0){ //if there is no parent reactor eg.: data.type
			//String self = (String) thisReactorHash.get("SELF");
			//if(self != null && reactorHash.containsKey(self)) {
			//curReactor = (IScriptReactor)reactorHash.get(self);
			if(put)
				curReactor.put(output, value);
			else
				curReactor.set(output, value);
			//}
		}
		return thisReactorHash;
	}		

	public Hashtable <String, Object> deinitReactor(String myName, String input, String output) {
		return deinitReactor(myName, input, output, true);
	}

	private void synchronizeValues(String input, String[] values2Sync, IScriptReactor thisReactor) {
		for(int valIndex = 0;valIndex < values2Sync.length;valIndex++) {
			Object value = thisReactor.getValue(values2Sync[valIndex]);
			if(value != null) {
				curReactor.put(input + "_" + values2Sync[valIndex], value);
			}
		}		
	}

	private String getCol(String colName) {
		colName = colName.substring(colName.indexOf(":") + 1).trim();

		return colName;
	}

	// the highest level above all commands
	// tracks the most basic things all pkql should have
	private void postProcess(Node node){
		runner.setCurrentString(node.toString());
		runner.storeResponse();
	}

	// highest level preprocessing
	private void preProcess(Node node){
		runner.setCurrentString(node.toString());
		runner.storeResponse();
	}

	@Override
	public void inAApiBlock(AApiBlock node) {
		if(reactorNames.containsKey(PKQLEnum.API)) {
			List myJoins = null;
			if(curReactor !=null && curReactor.getValue(PKQLEnum.JOINS)!=null){
				myJoins = (List) curReactor.getValue(PKQLEnum.JOINS);
			}
			initReactor(PKQLEnum.API);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.API, nodeStr);
			String engine = node.getEngineName().toString().trim();
			curReactor.put("ENGINE", engine);
			curReactor.put("INSIGHT", node.getInsight().toString());
			Map<String, Map<String, Object>> varMap = runner.getVarMap();
			Map<String, Map<String, Object>> varMapForReactor = new HashMap<String, Map<String, Object>>();
			// Grab any param data for the current engine so API reactor grabs those values as needed
			for(String var : varMap.keySet()) {
				Map<String, Object> paramValues = varMap.get(var);
				if(paramValues.get(Constants.ENGINE).equals(engine)) {
					varMapForReactor.put(var, varMap.get(var));
				}
			}
			curReactor.put("VARMAP", varMapForReactor);
			if(myJoins != null) {
				curReactor.put("TABLE_JOINS", myJoins);
			}		
		}
	}

	@Override
	public void outAApiBlock(AApiBlock node) {
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.API, nodeStr, PKQLEnum.API); // I need to make this into a string
		if(curReactor != null && node.parent() != null && (node.parent() instanceof AApiImportBlock || node.parent() instanceof AApiTerm) && !node.getEngineName().toString().equalsIgnoreCase("ImportIO")) {
			String [] values2Sync = curReactor.getValues2Sync(PKQLEnum.API);
			synchronizeValues(PKQLEnum.API, values2Sync, thisReactor);
		}
		runner.setResponse(thisReactor.getValue("RESPONSE"));
		runner.setStatus((STATUS) thisReactor.getValue("STATUS"));
	}


	//////////// HIGHEST LEVEL SCRIPT IN AND OUT //////////////////////////////
	@Override
	public void inAExprScript(AExprScript node) {
		if(reactorNames.containsKey(PKQLEnum.EXPR_SCRIPT)) {
			initReactor(PKQLEnum.EXPR_SCRIPT);
			String nodeExpr = node.getExpr().toString().trim();
			curReactor.put(PKQLEnum.EXPR_TERM, nodeExpr);
		}		
	}

	@Override
	public void outAExprScript(AExprScript node) {
		String nodeExpr = node.getExpr().toString().trim();
		String nodeStr = node.toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.EXPR_SCRIPT, nodeExpr, nodeStr);
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed expression
	@Override
	public void inAHelpScript(AHelpScript node) {
		//TODO: build out a String that explains PKQL and the commands
		runner.setResponse("Welcome to PKQL. Please look through documentation to find available functions.");
		runner.setStatus(STATUS.SUCCESS);
	}
	@Override
	public void outAHelpScript(AHelpScript node) {
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed expression
	@Override
	public void outAVaropScript(AVaropScript node) {
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed expression
	@Override
	public void outAColopScript(AColopScript node) {
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed expression
	@Override
	public void outAPanelopScript(APanelopScript node) {
		postProcess(node);
	}

	//**************************************** START PANEL OPERATIONS **********************************************//
	@Override 
	public void inAPanelopScript(APanelopScript node){
		String nodeString = node.toString();
		String id = "0";
		if(nodeString.startsWith("panel[")){
			nodeString = nodeString.substring(nodeString.indexOf("[")+1);
			id = nodeString.substring(0, nodeString.indexOf("]"));
		}
		runner.openFeDataBlock(id);
		runner.addFeData("panelId", id, true);
	}

	@Override
	public void inAPanelViz(APanelViz node) {
		System.out.println("in a viz change");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelViz(APanelViz node) {
		System.out.println("out a viz comment");
		String layout = node.getLayout().toString().trim();
		//		String alignment = node.getDatatablealign().toString().trim();
		Object alignment = curReactor.getValue("TERM");
		List<Object> alignTranslated = new Vector<Object>();
		if(alignment instanceof Vector){
			for(Object obj : (Vector)alignment){
				alignTranslated.add(curReactor.getValue((obj+"").trim()));
			}
		}
		else {
			alignTranslated.add(curReactor.getValue(alignment+""));
		}

		curReactor.put("VizTableData", alignTranslated);
		Map<String, Object> chartDataObj = new HashMap<String, Object>();
		chartDataObj.put("layout", layout);
		//		chartDataObj.put("dataTableKeys", alignTranslated);
		//		chartDataObj.put("dataTableValues", null);
		if(node.getUioptions()!=null){
			chartDataObj.put("uiOptions", node.getUioptions().toString().trim());
		}
		runner.addFeData("chartData", chartDataObj, true);
		runner.setResponse("Successfully set layout to " + layout + " with alignment " + alignment);//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
		deinitReactor(PKQLEnum.VIZ, "", "");

		chartDataObj.put("dataTableKeys", curReactor.getValue("VizTableKeys"));
		chartDataObj.put("dataTableValues", curReactor.getValue("VizTableValues"));
	}

	@Override 
	public void inAPanelComment(APanelComment node) {
		System.out.println("in a viz comment");
		initReactor(PKQLEnum.VIZ);
	}

	// this is just grabbing the comment information and storing it in the runner
	// should be split out into reactor
	@Override
	public void outAPanelComment(APanelComment node) {
		System.out.println("out a viz change");
		deinitReactor(PKQLEnum.VIZ, "", "");

		// get the comment id
		String nodeCommentString = node.getPanelcommentadd().toString();
		String cid = "0";
		if(nodeCommentString.contains(".comment[")){
			nodeCommentString = nodeCommentString.substring(nodeCommentString.indexOf(".comment[")+9);
			cid = nodeCommentString.substring(0, nodeCommentString.indexOf("]"));
		}

		// set the information
		Map<String, Object> commentMap = new HashMap<String, Object>();
		String textWithQuotes = node.getText().toString().trim();
		commentMap.put("text", textWithQuotes.substring(1, textWithQuotes.length()-1)); // remove the quotes
		commentMap.put("group", node.getGroup().toString().trim());
		commentMap.put("type", node.getType().toString().trim());
		commentMap.put("location", node.getLocation().toString().trim());
		commentMap.put("commentId", cid);
		Map comments = (Map) runner.getFeData("comments");
		if(comments == null){
			comments = new HashMap();
		}
		comments.put(cid, commentMap);
		runner.addFeData("comments", comments, true);
		runner.setResponse("Successfully commented : " + node.getText().toString().trim());//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelCommentEdit(APanelCommentEdit node) {
		System.out.println("in a viz comment edit");
		initReactor(PKQLEnum.VIZ);
	}

	// this is just grabbing the comment information and storing it in the runner
	// should be split out into reactor
	@Override
	public void outAPanelCommentEdit(APanelCommentEdit node) {
		System.out.println("out a viz comment edit");
		deinitReactor(PKQLEnum.VIZ, "", "");

		// get the comment id
		String nodeCommentString = node.getPanelcommentedit().toString();
		String cid = "0";
		if(nodeCommentString.contains(".comment[")){
			nodeCommentString = nodeCommentString.substring(nodeCommentString.indexOf(".comment[")+9);
			cid = nodeCommentString.substring(0, nodeCommentString.indexOf("]"));
		}

		//set the information
		Map<String, Object> commentMap = new HashMap<String, Object>();
		String textWithQuotes = node.getText().toString().trim();
		commentMap.put("text", textWithQuotes.substring(1, textWithQuotes.length()-1)); // remove the quotes
		commentMap.put("group", node.getGroup().toString().trim());
		commentMap.put("type", node.getType().toString().trim());
		commentMap.put("location", node.getLocation().toString().trim());
		commentMap.put("commentId", cid);
		Map comments = (Map) runner.getFeData("comments");
		if(comments == null){
			comments = new HashMap();
		}
		comments.put(cid, commentMap);
		runner.addFeData("comments", comments, true);
		runner.setResponse("Successfully edited comment " + cid + " : " + node.getText().toString().trim());//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelCommentRemove(APanelCommentRemove node) {
		System.out.println("in a viz comment remove");
		initReactor(PKQLEnum.VIZ);
	}

	// this sets a comment as closed
	@Override
	public void outAPanelCommentRemove(APanelCommentRemove node) {
		System.out.println("out a viz comment remove");
		deinitReactor(PKQLEnum.VIZ, "", "");

		// get the comment id
		String nodeCommentString = node.getPanelcommentremove().toString();
		String cid = "0";
		if(nodeCommentString.contains(".comment[")){
			nodeCommentString = nodeCommentString.substring(nodeCommentString.indexOf(".comment[")+9);
			cid = nodeCommentString.substring(0, nodeCommentString.indexOf("]"));
		}

		//set it as closed
		Map<String, Object> commentMap = new HashMap<String, Object>();
		commentMap.put("closed", true);

		Map comments = (Map) runner.getFeData("comments");
		if(comments == null){
			comments = new HashMap();
		}
		comments.put(cid, commentMap);
		runner.addFeData("comments", comments, true);
		runner.setResponse("Successfully removed comment " + cid);//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelClone(APanelClone node){
		System.out.println("in a panel clone");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelClone(APanelClone node){
		System.out.println("out a panel clone");
		String newId = node.getNewid().getText();
		// we add to the current fe data the new panel id
		runner.addFeData("newPanelId", newId, false);
		// also copy current state to this new panel id
		runner.copyFeData(newId);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully cloned! New panel id: " + newId);//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelLookAndFeel(APanelLookAndFeel node){
		System.out.println("in a panel laf");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelLookAndFeel(APanelLookAndFeel node){
		System.out.println("out a panel laf");
		Map laf = (Map) runner.getFeData("lookandfeel");
		if(laf == null){
			laf = new HashMap();
		}
		laf.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		runner.addFeData("lookandfeel", laf, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set look and feel");//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelTools(APanelTools node){
		System.out.println("in a panel tools");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelTools(APanelTools node){
		System.out.println("out a panel tools");
		Map tools = (Map) runner.getFeData("tools");
		if(tools == null){
			tools = new HashMap();
		}
		tools.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		runner.addFeData("tools", tools, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set tools");//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelConfig(APanelConfig node){
		System.out.println("in a panel config");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelConfig(APanelConfig node){
		System.out.println("out a panel config");
		Map config = (Map) runner.getFeData("config");
		if(config == null){
			config = new HashMap();
		}
		config.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		runner.addFeData("config", config, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set config");//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelClose(APanelClose node){
		System.out.println("in a panel close");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelClose(APanelClose node){
		System.out.println("out a panel close");
		runner.addFeData("closed", true, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully closed panel");//
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	//**************************************** END PANEL OPERATIONS **********************************************//

	//**************************************** START JOIN OPERATIONS **********************************************//
	@Override
	public void outAOpenDataJoinParam(AOpenDataJoinParam node)
	{
		String insightID = (String)curReactor.getValue(PKQLEnum.OPEN_DATA);
		curReactor.set(PKQLEnum.JOIN_PARAM, insightID);
	}
	
	@Override
	public void outAInsightidJoinParam(AInsightidJoinParam node)
	{
		String insightID = node.getWord().toString().trim();
		String cleanedInsightID = insightID.substring(1, insightID.length()-1);
		curReactor.set(PKQLEnum.JOIN_PARAM, cleanedInsightID);
	}
	
	@Override
	public void outAVariableJoinParam(AVariableJoinParam node)
	{
		String varName = ((AVarDef)node.getVarDef()).getValname().toString().trim();
		String insightID = (String)runner.getVariableValue(varName);
		curReactor.set(PKQLEnum.JOIN_PARAM, insightID);
	}
	
	public void inADashboardJoin(ADashboardJoin node)
	{
		System.out.println("Have dashboard join as " + node);
		if(reactorNames.containsKey(PKQLEnum.DASHBOARD_JOIN)) {
			initReactor(PKQLEnum.DASHBOARD_JOIN);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.DASHBOARD_JOIN, nodeStr.trim());
			// rel type is a token, not a production, so no in/out to add it to the reactor
			// need to add it here
			if(node.getRel() != null)
			curReactor.put(PKQLEnum.REL_TYPE, node.getRel().toString().trim());
		}
	}
	
	public void outADashboardJoin(ADashboardJoin node)
	{
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		curReactor.put("G", this.frame);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DASHBOARD_JOIN, nodeStr, PKQLEnum.DASHBOARD_JOIN);
		runner.setDashBoardData(thisReactor.getValue("DashboardData"));
	}

	//**************************************** END JOIN OPERATIONS **********************************************//

	//**************************************** START VAR OPERATIONS **********************************************//
	
    public void outAExprInputOrExpr(AExprInputOrExpr node) {
    	//need to get expr and set it to var_param
    	String value = node.getExpr().toString().trim();
    	curReactor.put(PKQLEnum.VAR_PARAM, value);
    }

    public void outAInputInputOrExpr(AInputInputOrExpr node) {
    }

    public void outAOpenDataInputOrExpr(AOpenDataInputOrExpr node) {
    	String insightID = ((List)curReactor.getValue(PKQLEnum.OPEN_DATA)).get(0).toString();
		curReactor.put(PKQLEnum.VAR_PARAM, insightID);
    }
	
    @Override
	public void inAVarop(AVarop node){
		if(reactorNames.containsKey(PKQLReactor.VAR.toString())) {
			String varName = (node.getVarDef() + "").trim();
			String expr = (node.getInputOrExpr() + "").trim();

			initReactor(PKQLReactor.VAR.toString());
			curReactor.put(PKQLReactor.VAR.toString(), varName);
			curReactor.put(PKQLEnum.EXPR_TERM, expr); // don't need once all algorithms have been refactored into Reactors
		}	
	}

	//this is only used for setting a var (aka v:test = 'true')
	//AVarTerm will be used in expressions (aka c:Budget + v:test)
	@Override
	public void outAVarop(AVarop node) {
		if(reactorNames.containsKey(PKQLReactor.VAR.toString())) {
			String nodeStr = PKQLReactor.VAR.toString();
			
			boolean updatingExistingVar = false;
			Map<String, Object> thisReactorHash = deinitReactor(PKQLReactor.VAR.toString(), nodeStr, nodeStr);
			IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLReactor.VAR.toString());
			String varName = previousReactor.getValue(PKQLReactor.VAR.toString()) + "";
			Object inputNeeded = previousReactor.getValue(PKQLEnum.INPUT);
			if(inputNeeded == null) // if no input needed for this var, set it and we are good
			{
				String varParam = curReactor.getValue(PKQLEnum.VAR_PARAM) + "";
				runner.unassignedVars.remove(varName);
				if(runner.getVariableData(varName) != null) {
					updatingExistingVar = true;
				}
				runner.setVariableValue(varName, varParam.replaceAll("^\'|\'$", "").trim());
				runner.setResponse("Set variable " + varName +" to " + varParam);
				runner.setStatus(STATUS.SUCCESS);
				curReactor.put(PKQLReactor.VAR.toString(), varName);
			}
			else {
				runner.unassignedVars.add(varName);
				if(runner.getVariableData(varName) == null) {
					runner.addNewVariable(varName, previousReactor.getValue(Constants.ENGINE).toString(), ((Vector)previousReactor.getValue(Constants.TYPE)).get(0).toString());
				}
				runner.setResponse("Need input on variable " + varName);
				Map<String, Object> paramMap = new HashMap<String,Object>();
				paramMap.put("varToSet", varName);
				paramMap.put("options", previousReactor.getValue("options"));
				paramMap.put("selectAmount", previousReactor.getValue("selectAmount"));
				runner.addBeData("var2define", paramMap, false);
				runner.setStatus(STATUS.INPUT_NEEDED);
			}
		}	
	}

	@Override
	public void inAVarTerm(AVarTerm node) {
		String varName = node.getVar().toString().trim();
		// get the value for the var from the runner
		Object varVal = runner.getVariableValue(varName);
		// adding to the reactor
		curReactor.set(PKQLEnum.VAR_TERM, varVal);
		curReactor.addReplacer((node + "").trim(), varVal);
	}

	@Override
	public void inAVarDef(AVarDef node) {
		String valName = node.getValname().toString().trim();
		// adding to the reactor
		curReactor.put(PKQLReactor.VAR.toString(), valName);
		curReactor.addReplacer((node + "").trim(), valName);
    }
	
	//**************************************** END VAR OPERATIONS **********************************************//
	
	
	@Override
	public void inADataFrame(ADataFrame node) {
		if(reactorNames.containsKey(PKQLEnum.DATA_FRAME)) {
			// get the appropriate reactor
			initReactor(PKQLEnum.DATA_FRAME);
			curReactor.put("G", frame);
		}	
	}

	@Override
	public void outADataFrame(ADataFrame node) {
		// then deinit
		//grab the new G from the reactor
		// set into this class

		deinitReactor(PKQLEnum.DATA_FRAME, node.getBuilder().toString().trim(),  node.toString().trim());

		this.frame = (ITableDataFrame) curReactor.getValue(PKQLEnum.G);

		// set the script reactors for this new frame
		this.reactorNames = frame.getScriptReactors();
	}

	@Override
	public void inATermExpr(ATermExpr node) {
		if(reactorNames.containsKey(PKQLEnum.EXPR_TERM)) {
			// get the appropriate reactor
			initReactor(PKQLEnum.EXPR_TERM);
			// get the name of reactor
			String nodeTerm = node.getTerm().toString().trim();
			curReactor.put("G", frame);
			curReactor.put(PKQLEnum.EXPR_TERM, nodeTerm);
		}	
	}

	@Override
	public void outATermExpr(ATermExpr node) {
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.EXPR_TERM, node.getTerm().toString().trim(),  node.toString().trim());

		if (thisReactorHash.get(PKQLEnum.EXPR_TERM) instanceof ExprReactor) {
			ExprReactor thisReactor = (ExprReactor)thisReactorHash.get(PKQLEnum.EXPR_TERM);
			String expr = (String)thisReactor.getValue(PKQLEnum.EXPR_TERM);
			Object objVal = thisReactor.getValue(PKQLEnum.COL_DEF);
			if(objVal != null) {
				if(objVal instanceof Collection) {
					Collection<? extends Object> values = (Collection<? extends Object>) objVal;
					for(Object obj : values) {
						curReactor.set(PKQLEnum.COL_DEF, obj);
					}
				} else {
					curReactor.set(PKQLEnum.COL_DEF, objVal);
				}
			}

			// this commented out code is part of the shift to getting derived calculation info
			//			objVal = thisReactor.getValue(PKQLEnum.COL_CSV);
			//			if(objVal != null) {
			//				curReactor.put(PKQLEnum.COL_CSV, objVal);
			//			}
			//			objVal = thisReactor.getValue(PKQLEnum.PROC_NAME);
			//			if(objVal != null) {
			//				curReactor.put(PKQLEnum.PROC_NAME, objVal);
			//			}
			curReactor.addReplacer(expr, thisReactor.getValue(expr));
			//			runner.setResponse(thisReactor.getValue(expr));
			//			runner.setStatus((String)thisReactor.getValue("STATUS"));
		}
	}

	@Override
	public void inAEExprExpr(AEExprExpr node) {
		System.out.println("In The EXPRESSION .. " + node);
	}

	@Override
	public void outAPlusExpr(APlusExpr node) {
		String leftKeyName = node.getLeft().toString().trim();
		String rightKeyName = node.getRight().toString().trim();

		Object leftObj = curReactor.getValue(leftKeyName);
		Object rightObj = curReactor.getValue(rightKeyName);
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double)(leftObj) + (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName.trim());
			curReactor.removeReplacer(rightKeyName.trim());
		}
	}

	@Override
	public void inAMinusExpr(AMinusExpr node) {
	}

	@Override
	public void outAMinusExpr(AMinusExpr node) {
		String leftKeyName = node.getLeft().toString().trim();
		String rightKeyName = node.getRight().toString().trim();

		Object leftObj = curReactor.getValue(leftKeyName);
		Object rightObj = curReactor.getValue(rightKeyName);
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double)(leftObj) - (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
	}

	@Override
	public void outAMultExpr(AMultExpr node) {
		String leftKeyName = node.getLeft().toString().trim();
		String rightKeyName = node.getRight().toString().trim();

		Object leftObj = curReactor.getValue(leftKeyName);
		Object rightObj = curReactor.getValue(rightKeyName);
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double)(leftObj) * (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
	}

	@Override
	public void outADivExpr(ADivExpr node) {
		String leftKeyName = node.getLeft().toString().trim();
		String rightKeyName = node.getRight().toString().trim();

		Object leftObj = curReactor.getValue(leftKeyName);
		Object rightObj = curReactor.getValue(rightKeyName);
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double)(leftObj) / (Double)(rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
	}

	@Override
	public void outAModExpr(AModExpr node) {

	}

	@Override
	public void inAColDef(AColDef node) {
		String colName = node.getColname().toString().trim();
		// adding to the reactor
		curReactor.set("COL_DEF", colName);
		curReactor.addReplacer((node + "").trim(), colName);
	}
	
	@Override
	public void inAFlexSelectorRow(AFlexSelectorRow node) {
		// adding to the reactor
		curReactor.set("TERM", node.getTerm()+"");
	}

	@Override
	public void inATermGroup(ATermGroup node) {
		// adding to the reactor
		curReactor.set("TERM", node.getTerm()+"");
	}

	@Override
	public void inAAddColumn(AAddColumn node) {
		if(reactorNames.containsKey(PKQLEnum.COL_ADD)) {
			initReactor(PKQLEnum.COL_ADD);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.COL_ADD, nodeStr);

			String nodeExpr = node.getExpr().toString().trim();
			curReactor.put(PKQLEnum.EXPR_TERM, nodeExpr);
		}		
	}

	@Override
	public void outAAddColumn(AAddColumn node) {
		String nodeExpr = node.getExpr().toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeExpr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.COL_ADD, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.COL_ADD.toString());
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		runner.setResponse((String)previousReactor.getValue("RESPONSE"));
	}

	@Override
	public void inAFilterColumn(AFilterColumn node) {
		if(reactorNames.containsKey(PKQLEnum.FILTER_DATA)) {
			initReactor(PKQLEnum.FILTER_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.FILTER_DATA, nodeStr.trim());
		}
	}
	//	
	@Override
	public void outAFilterColumn(AFilterColumn node) {
		String nodeExpr = node.getWhere().toString().trim();
		//		curReactor.put(PKQLEnum.WHERE, nodeExpr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.FILTER_DATA, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.FILTER_DATA.toString());
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		runner.setResponse((String)previousReactor.getValue("FILTER_RESPONSE"));
	}

	@Override
	public void inAUnfilterColumn(AUnfilterColumn node) {
		if(reactorNames.containsKey(PKQLEnum.UNFILTER_DATA)) {
			initReactor(PKQLEnum.UNFILTER_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.UNFILTER_DATA, nodeStr.trim());
		}
	}

	@Override
	public void outAUnfilterColumn(AUnfilterColumn node) {
		String nodeExpr = node.getColDef().toString().trim();
		//		curReactor.put(PKQLEnum.WHERE, nodeExpr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.UNFILTER_DATA, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.UNFILTER_DATA.toString());
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		runner.setResponse("Unfiltered Column: " + (String)previousReactor.getValue("FILTER_COLUMN"));
	}

	@Override
	public void inASplitColumn(ASplitColumn node) {

		if(reactorNames.containsKey(PKQLEnum.COL_SPLIT)) {
			initReactor(PKQLEnum.COL_SPLIT);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.COL_SPLIT, nodeStr.trim());
		}
	}

	@Override
	public void outASplitColumn(ASplitColumn node) {

		String nodeExpr = node.getColsplit().toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.COL_SPLIT, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.COL_SPLIT.toString());
		//		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		//		runner.setResponse("SplitColumn: " + (String)previousReactor.getValue("FILTER_COLUMN"));
	}

	@Override
	public void outAExprGroup(AExprGroup node) {

	}

	@Override
	public void inAImportData(AImportData node){
		if(reactorNames.containsKey(PKQLEnum.IMPORT_DATA)) {
			// make the determination to say if this is a frame.. yes it is
			/// if it is so change the reactor to the new reactor
			initReactor(PKQLEnum.IMPORT_DATA);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.IMPORT_DATA, nodeStr);

			//			String varName = node.getVar().toString().trim();
			//			// get the value for the var from the runner
			//			Object varVal = runner.getVariable(varName);
			//			// adding to the reactor
			//			curReactor.set(PKQLEnum.VAR_TERM, varVal);
			//			curReactor.addReplacer((node + "").trim(), varVal);

			if(node.getJoins()!=null){
				node.getJoins().apply(this); // need to process joins so that we can access them in the api block for preprocessing inner joins
			}		
		}
	}

	@Override
	public void outAImportData(AImportData node){
		String nodeImport = node.getImport().toString().trim();
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeImport);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.IMPORT_DATA, nodeImport, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLReactor.IMPORT_DATA.toString());
		runner.setNewColumns((Map<String, String>)previousReactor.getValue("logicalToValue"));
		runner.setResponse(previousReactor.getValue(nodeStr));
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
	}

	@Override
	public void inAOpenData(AOpenData node)
	{
		initReactor(PKQLEnum.OPEN_DATA);
	}

	@Override
	public void outAOpenData(AOpenData node)
	{
		String nodeOpen = node.getDataopentoken().toString().trim();
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeOpen);
		Object o = curReactor.getValue(PKQLEnum.OPEN_DATA);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.OPEN_DATA, nodeOpen, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.OPEN_DATA);
		curReactor.set(PKQLEnum.OPEN_DATA, previousReactor.getValue(PKQLEnum.OPEN_DATA));
	}

	@Override
	public void inARemoveData(ARemoveData node) {
		if(reactorNames.containsKey(PKQLEnum.REMOVE_DATA)) {
			// simplify baby simplify baby simplify
			initReactor(PKQLEnum.REMOVE_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.REMOVE_DATA, nodeStr.trim());
		}	
	}

	@Override
	public void outARemoveData(ARemoveData node) {
		String nodeStr = node.getApiBlock() + "";
		nodeStr = nodeStr.trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeStr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.REMOVE_DATA, nodeStr, (node + "").trim());
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.REMOVE_DATA);
		runner.setResponse(previousReactor.getValue(node.toString().trim()));//
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
	}

	@Override
	public void outASetColumn(ASetColumn node) {
	}

	
	@Override
	public void inAUserInput(AUserInput node) {
		if(reactorNames.containsKey(PKQLReactor.INPUT.toString())) {
			//			String options = node.getOptions().toString().trim();
			//			String selections = node.getSelections().toString().trim();

			initReactor(PKQLReactor.INPUT.toString());
			curReactor.put(PKQLReactor.INPUT.toString(), node.toString());
			//			curReactor.put(PKQLEnum.EXPR_TERM, expr); // don't need once all algorithms have been refactored into Reactors
		}	
	}

	@Override
	public void outAUserInput(AUserInput node) {
		if(reactorNames.containsKey(PKQLReactor.INPUT.toString())) {
			String nodeStr = PKQLReactor.INPUT.toString();
			Map<String, Object> thisReactorHash = deinitReactor(PKQLReactor.INPUT.toString(), nodeStr, nodeStr);
			IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLReactor.INPUT.toString());
			List options = (List) previousReactor.getValue("options");
			String selections = node.getSelections().toString().trim();
			// need to pause processing here........
			// a user defined value must be retrieved from the front end
			//		this.unassignedVars.add(node.toString());
			curReactor.put("options", options);
			curReactor.put("selectAmount", selections);
			curReactor.put(Constants.ENGINE, previousReactor.getValue(PKQLEnum.API + "_" + Constants.ENGINE));
			curReactor.put(Constants.TYPE, previousReactor.getValue(PKQLEnum.API + "_" + PKQLEnum.COL_CSV));
			//		node.replaceBy(null); // need to get out of finishing the processing of this pkql..... how do i just return out of this bad boy??
			// the plan is:
			// return out of this bad boy
			// allow for term = term which will be the way the front end sets it
			// in the config we look for that type of script and allow it if our missing piece is there

			// FOR NOW ASSUMING USER INPUT IS JUST FOR VAR ASSIGNMENT
			// DONT NEED TO WORRY ABOUT RETURNING OUT OF ANYTHING
			//		node.
		}
	}

	@Override
	public void inANumberTerm(ANumberTerm node) {
		String number = node.getDecimal().toString().trim();
	}

	@Override
	public void inADecimal(ADecimal node) {
		String fraction = node.getFraction() + "";
		String number = node.getWhole().toString().trim();
		if(node.getFraction() != null)
			number = number + "." + fraction;

		curReactor.addReplacer(node.toString().trim(), Double.parseDouble(number));
	}

	@Override
	public void inAAlphaWordOrNum(AAlphaWordOrNum node) {
	}

	@Override
	public void outAAlphaWordOrNum(AAlphaWordOrNum node) {
		String word = (node.getWord() + "").trim();
		String cleaned = word.substring(1, word.length()-1);// remove the quotes
		curReactor.set(PKQLEnum.WORD_OR_NUM, cleaned); 
		curReactor.addReplacer(word, cleaned);
	}

	@Override
	public void outANumWordOrNum(ANumWordOrNum node) {
		String number = node.getDecimal().toString().trim();
		if(node.getDecimal() instanceof ADecimal) {
			ADecimal dec = (ADecimal)node.getDecimal();
			String fraction = dec.getFraction() + "";
			number = dec.getWhole().toString().trim();
			if(dec.getFraction() != null)
				number = number + "." + fraction;
		}
		curReactor.set(PKQLEnum.WORD_OR_NUM, number);
	}

	@Override
	public void outAKeyvalue(AKeyvalue node){
		Object word1 = node.getWord1();
		if(curReactor.getValue(word1.toString().trim())!=null){
			word1 = curReactor.getValue(word1.toString().trim());
		}

		Object word2 = node.getWord2();
		if(curReactor.getValue(word2.toString().trim())!=null){
			word2 = curReactor.getValue(word2.toString().trim());
		}

		Map myMap = new HashMap();
		myMap.put(word1, word2);
		curReactor.set("KEY_VALUE", myMap); // remove the quotes
	}

	@Override
	public void inAExprRow(AExprRow node) {
	}

	@Override
	public void outAExprRow(AExprRow node) {
	}

	@Override
	public void inAMathFunTerm(AMathFunTerm node) {
	}

	@Override
	public void inAMathFun(AMathFun node) {
		if(reactorNames.containsKey(PKQLEnum.MATH_FUN)) {
			String procedureName = node.getId().toString().trim();
			String nodeStr = node.getExpr().toString().trim();

			String procedureAlgo = "";
			if(reactorNames.containsKey(procedureName.toUpperCase())) {
				// the frame has defined a specific reactor for this procedure
				procedureAlgo = reactorNames.get(procedureName.toUpperCase());
			} else {
				procedureAlgo = "prerna.algorithm.impl." + procedureName + "Reactor";
			}

			reactorNames.put(PKQLReactor.MATH_FUN.toString(), procedureAlgo);
			String expr = (String)curReactor.getValue(PKQLEnum.EXPR_TERM);

			initReactor(PKQLReactor.MATH_FUN.toString());
			curReactor.put(PKQLEnum.G, frame);
			curReactor.put(PKQLEnum.MATH_FUN, nodeStr.trim());

			//for panel.viz
			curReactor.put("MATH_EXPRESSION", node.toString().trim());

			curReactor.put(PKQLEnum.PROC_NAME, procedureName); // don't need once all algorithms have been refactored into Reactors
			if(expr != null)
				curReactor.put(PKQLEnum.EXPR_TERM, expr);
		}	
	}

	@Override
	public void outAMathFun(AMathFun node) {
		String nodeStr = node.toString().trim();
		String expr = node.getExpr().toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLReactor.MATH_FUN.toString(), expr, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLReactor.MATH_FUN.toString());
		curReactor.put(PKQLEnum.COL_DEF, previousReactor.getValue(PKQLEnum.COL_DEF)); //TODO: use syncronize instead
		curReactor.put(PKQLEnum.PROC_NAME, previousReactor.getValue(PKQLEnum.PROC_NAME));
		curReactor.put(PKQLEnum.COL_CSV, previousReactor.getValue(PKQLEnum.COL_CSV));

		//for panel.viz -- can't use the same, because i need set, not put...don't want to change because that might break something
		//TODO : combine with above
		curReactor.set("MATH_EXPRESSION", previousReactor.getValue("MATH_EXPRESSION"));
		curReactor.set(PKQLEnum.COL_DEF+"2", previousReactor.getValue(PKQLEnum.COL_DEF)); //TODO: use syncronize instead
		curReactor.set(PKQLEnum.PROC_NAME+"2", previousReactor.getValue(PKQLEnum.PROC_NAME));
		curReactor.set(PKQLEnum.COL_CSV+"2", previousReactor.getValue(PKQLEnum.COL_CSV));


		curReactor.addReplacer(nodeStr, previousReactor.getValue(expr));
		runner.setResponse(previousReactor.getValue(expr));
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
	}

	@Override
	public void inAMathParam(AMathParam node) {
		if(reactorNames.containsKey(PKQLEnum.MATH_PARAM)) {
			initReactor(PKQLEnum.MATH_PARAM);
		}	
	}

	@Override
	public void outAMathParam(AMathParam node) {
		String nodeStr = node.toString().trim();
		String expr = node.getMapObj().toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.MATH_PARAM, expr, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get((PKQLEnum.MATH_PARAM));
		curReactor.put(PKQLEnum.MATH_PARAM, previousReactor.getValue(PKQLEnum.MATH_PARAM));

	}

	@Override
	public void inAColWhere(AColWhere node) {
		if(reactorNames.containsKey(PKQLEnum.WHERE)) {
			initReactor(PKQLEnum.WHERE);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.WHERE, nodeStr.trim());
			curReactor.put(PKQLEnum.COMPARATOR, (node.getEqualOrCompare()+"").trim());
		}		
	}

	@Override
	public void outAColWhere(AColWhere node) {
		// I need to do some kind of action and pop out the last one on everything
		String nodeStr = node.toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.WHERE, nodeStr, PKQLEnum.FILTER, false);
	}

	@Override
	public void inARelationDef(ARelationDef node) {
		if(reactorNames.containsKey(PKQLEnum.REL_DEF)) {
			initReactor(PKQLEnum.REL_DEF);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.REL_DEF, nodeStr);
			curReactor.put(PKQLEnum.REL_TYPE, (node.getRelType().toString()).trim());
		}		
	}

	@Override
	public void outARelationDef(ARelationDef node) {
		String nodeStr = node.toString().trim();
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.REL_DEF, nodeStr, PKQLEnum.JOINS, false);
	}

	@Override
	public void inAColCsv(AColCsv node) {
		System.out.println("Directly lands into col csv " + node);
		if(reactorNames.containsKey(PKQLEnum.COL_CSV)) {
			initReactor(PKQLEnum.COL_CSV);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.COL_CSV, nodeStr);
		}
	}

	@Override
	public void outAColCsv(AColCsv node) {
		String thisNode = node.toString().trim();
		if(node.parent() != null && node.parent() instanceof AColTable) {
			deinitReactor(PKQLEnum.COL_CSV, thisNode, PKQLEnum.COL_CSV, false);
		} else {
			deinitReactor(PKQLEnum.COL_CSV, thisNode, PKQLEnum.COL_CSV);
		}

	}

	@Override
	public void inACsvRow(ACsvRow node) {
		System.out.println("Directly lands into col csv " + node);
		if(reactorNames.containsKey(PKQLEnum.ROW_CSV)) {
			initReactor(PKQLEnum.ROW_CSV);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.ROW_CSV, nodeStr);
		}
	}

	@Override
	public void outACsvRow(ACsvRow node) {
		// I need to do an action here
		// get the action
		// call to say this has happened and then reset it to null;
		String thisNode = node.toString().trim();

		if(node.parent() != null && node.parent() instanceof ACsvTable) {
			deinitReactor(PKQLEnum.ROW_CSV, thisNode, PKQLEnum.ROW_CSV, false);
		} else {
			deinitReactor(PKQLEnum.ROW_CSV, thisNode, PKQLEnum.ROW_CSV);
		}
	}

	@Override
	public void inACsvTable(ACsvTable node) {
		System.out.println("Directly lands into col table " + node);
		if(reactorNames.containsKey(PKQLEnum.CSV_TABLE)) {
			initReactor(PKQLEnum.CSV_TABLE);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.CSV_TABLE, nodeStr.trim());
		}
	}

	@Override
	public void outACsvTable(ACsvTable node) {
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		deinitReactor(PKQLEnum.CSV_TABLE, thisNode, PKQLEnum.CSV_TABLE);

		if(curReactor != null && node.parent() != null && node.parent() instanceof ACsvTableImportBlock) {
			String [] values2Sync = curReactor.getValues2Sync(PKQLEnum.CSV_TABLE);
			synchronizeValues(PKQLEnum.CSV_TABLE, values2Sync, thisReactor);
		}
	}

	public void inAPastedData(APastedData node)
	{
		System.out.println("Directly lands into col table " + node);
		if(reactorNames.containsKey(PKQLEnum.PASTED_DATA)) {
			initReactor(PKQLEnum.PASTED_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.PASTED_DATA, nodeStr.trim());
			// is theere a more appropriate way to pass information between siblings
			if(node.parent() != null && node.parent() instanceof APastedDataBlock) {
				String word = ((APastedDataBlock) node.parent()).getDelimitier().toString().trim();
				curReactor.set(PKQLEnum.WORD_OR_NUM, (word.substring(1, word.length()-1))); // remove the quotes
			}
		}
	}

	public void outAPastedData(APastedData node)
	{
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		deinitReactor(PKQLEnum.PASTED_DATA, thisNode, PKQLEnum.PASTED_DATA);

		if(curReactor != null && node.parent() != null && node.parent() instanceof APastedDataImportBlock) {
			String [] values2Sync = curReactor.getValues2Sync(PKQLEnum.PASTED_DATA);
			synchronizeValues(PKQLEnum.PASTED_DATA, values2Sync, thisReactor);
		}
	}

	public IDataMaker getDataFrame() {
		if(this.curReactor!=null){
			IDataMaker table = (IDataMaker)this.curReactor.getValue("G");
			if(table == null){
				return this.frame;
			}
			return table;
		}
		else {
			return null;
		}
	}

	//**************************************** START DATA OPERATIONS **********************************************//

	public void inADatatype(ADatatype node)
	{
		System.out.println("Translation.inADatatype() with node = "+ node );
		if(reactorNames.containsKey(PKQLEnum.DATA_TYPE)) {
			initReactor(PKQLEnum.DATA_TYPE);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.DATA_TYPE, nodeStr);
		}
	}

	public void outADatatype(ADatatype node)
	{
		System.out.println("Translation.outADatatype() with node = "+ node );
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_TYPE, thisNode, PKQLEnum.DATA_TYPE);
		runner.setResponse(thisReactor.getValue(PKQLEnum.DATA_TYPE));
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
		runner.setCurrentString(PKQLEnum.DATA_TYPE);
		runner.storeResponse();
		System.out.println("");
	}
	
	public void inADataconnect(ADataconnect node)
    {
    	System.out.println("Translation.inADataconnect() with node = "+ node );
    	if(reactorNames.containsKey(PKQLEnum.DATA_CONNECT)) {
			initReactor(PKQLEnum.DATA_CONNECT);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.DATA_CONNECT, nodeStr);
		}
    }

    public void outADataconnect(ADataconnect node)
    {
    	System.out.println("Translation.outADataconnect() with node = "+ node );
    	String thisNode = node.toString().trim();
    	IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_CONNECT, thisNode, PKQLEnum.DATA_CONNECT);
		runner.setResponse(thisReactor.getValue(PKQLEnum.DATA_CONNECT));
		runner.setStatus((STATUS) thisReactor.getValue("STATUS"));//
		runner.setCurrentString(PKQLEnum.DATA_CONNECT);
		runner.storeResponse();
    }
    
    public void inADataconnectdb(ADataconnectdb node)
    {
    	initReactor(PKQLEnum.DATA_CONNECTDB);
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.DATA_CONNECTDB, nodeStr);
    }

    public void outADataconnectdb(ADataconnectdb node)
    {
    	String thisNode = node.toString().trim();
    	IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_CONNECTDB, thisNode, PKQLEnum.DATA_CONNECTDB);
    }

}