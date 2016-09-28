package prerna.sablecc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.api.IScriptReactor;
import prerna.om.Dashboard;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.analysis.DepthFirstAdapter;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.node.AAddColumn;
import prerna.sablecc.node.AAlphaWordOrNum;
import prerna.sablecc.node.AApiBlock;
import prerna.sablecc.node.AApiImportBlock;
import prerna.sablecc.node.AApiTerm;
import prerna.sablecc.node.AColCsv;
import prerna.sablecc.node.AColDef;
import prerna.sablecc.node.AColGroup;
import prerna.sablecc.node.AColTerm;
import prerna.sablecc.node.AColWhere;
import prerna.sablecc.node.AColopScript;
import prerna.sablecc.node.AConfiguration;
import prerna.sablecc.node.ACsvRow;
import prerna.sablecc.node.ACsvTable;
import prerna.sablecc.node.ACsvTerm;
import prerna.sablecc.node.ADashboardAdd;
import prerna.sablecc.node.ADashboardConfig;
import prerna.sablecc.node.ADashboardJoin;
import prerna.sablecc.node.ADashboardopScript;
import prerna.sablecc.node.ADataFrame;
import prerna.sablecc.node.ADatabaseConcepts;
import prerna.sablecc.node.ADatabaseList;
import prerna.sablecc.node.ADataconnect;
import prerna.sablecc.node.ADataconnectdb;
import prerna.sablecc.node.ADatanetworkconnect;
import prerna.sablecc.node.ADatanetworkdisconnect;
import prerna.sablecc.node.ADatatype;
import prerna.sablecc.node.ADecimal;
import prerna.sablecc.node.ADivExpr;
import prerna.sablecc.node.AEExprExpr;
import prerna.sablecc.node.AExprGroup;
import prerna.sablecc.node.AExprInputOrExpr;
import prerna.sablecc.node.AExprRow;
import prerna.sablecc.node.AExprScript;
import prerna.sablecc.node.AExprWordOrNum;
import prerna.sablecc.node.AFilterColumn;
import prerna.sablecc.node.AFlexSelectorRow;
import prerna.sablecc.node.AHelpScript;
import prerna.sablecc.node.AImportData;
import prerna.sablecc.node.AInputInputOrExpr;
import prerna.sablecc.node.AInsightidJoinParam;
import prerna.sablecc.node.AJOp;
import prerna.sablecc.node.AKeyvalue;
import prerna.sablecc.node.AKeyvalueGroup;
import prerna.sablecc.node.AMapObj;
import prerna.sablecc.node.AMathFun;
import prerna.sablecc.node.AMathFunTerm;
import prerna.sablecc.node.AMathParam;
import prerna.sablecc.node.AMinusExpr;
import prerna.sablecc.node.AModExpr;
import prerna.sablecc.node.AMultExpr;
import prerna.sablecc.node.ANumWordOrNum;
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
import prerna.sablecc.node.PColGroup;
import prerna.sablecc.node.PCsvGroup;
import prerna.sablecc.node.PCsvRow;
import prerna.sablecc.node.PKeyvalueGroup;
import prerna.sablecc.node.PScript;
import prerna.sablecc.node.TRelType;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;

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

	// frame specific reactors are stored in reactorNames
	// used for strategy pattern of pkql input to specific reactor class
	Map<String, String> reactorNames = new Hashtable<String, String>();
	
	// used for strategy pattern of pkql input to specific api type
	// this is not a part of reactor names since we have not defined these are constants
	// within the postfix, but are passed as string values in the "engineName"
	// component of a data.import.  It has to be defined as a string since the engine names
	// that could exist should obviously not be stored within the postfix
	Map<String, String> apiReactorNames = new Hashtable<String, String>();
	
	// keep a list of pkql metadata that is useful outside of translation
	// currently only used for csv file imports since need this information to 
	// later create an engine and modify a pkql string upon saving of insight
	List<IPkqlMetadata> storePkqlMetadata = new Vector<IPkqlMetadata>();
	
	IDataMaker frame = null;
	PKQLRunner runner = null;

	public Translation() { // Test Constructor
		frame = new TinkerFrame();
		this.runner = new PKQLRunner();

		this.reactorNames = frame.getScriptReactors();
		fillApiReactors();
	}
	/**
	 * Constructor that takes in the dataframe that it will perform its calculations off of and the runner that invoked the translation
	 * @param frame IDataMaker
	 * @param runner PKQLRunner: holds response from PKQL script and the status of whether the script errored or not
	 */
	public Translation(IDataMaker frame, PKQLRunner runner) {
		// now get the data from tinker
		this.frame = frame;
		this.runner = runner;

		this.reactorNames = frame.getScriptReactors();
		fillApiReactors();
	}

	/**
	 * We want to use a strategy pattern for instantiating the different API reactors
	 */
	private void fillApiReactors() {
		//TODO: should move all of these in RDF_MAP so that its easily updated
		apiReactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
		apiReactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.CsvApiReactor");
		apiReactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
		apiReactorNames.put(PKQLEnum.R_API, "prerna.sablecc.RApiReactor");
	}
	
	
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT OPERATIONS //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT OPERATIONS //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT OPERATIONS //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT OPERATIONS //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT OPERATIONS //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT OPERATIONS //////////////////////////////////////
	
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
		deinitReactor(PKQLEnum.EXPR_SCRIPT, nodeExpr, nodeStr);
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

    public void outADashboardopScript(ADashboardopScript node) {
    	postProcess(node);
    }
    
	// the highest level above all commands
	// tracks the most basic things all pkql should have
	private void postProcess(Node node){
		runner.setCurrentString(node.toString());
		runner.aggregateMetadata(this.storePkqlMetadata);
		runner.storeResponse();
		
		// we need to clear the previous references to the metadata
		// these get stored within the PKQLTransformation and pushed onto the 
		// insight
		// but the translation must lose the reference such that the recipe explanation
		// which builds of all the internal explanations of embedded pkqls, doesn't need
		// to use the previous ones
		this.storePkqlMetadata = new Vector<IPkqlMetadata>();
	}
	
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF REACTORS //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF REACTORS //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF REACTORS //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF REACTORS //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF REACTORS //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF REACTORS //////////////////////////////////////
	
	public void initReactor(String myName) {
		String parentName = null;
		if(reactorHash != null) {
			// I am not sure I need to add element here
			// I need 2 things in here
			// I need the name of a parent i.e. what is my name and my parent name
			// actually I just need my name
			parentName = (String)reactorHash.get("SELF");
		}
		reactorHash = new Hashtable<String, Object>();
		if(parentName != null) {
			reactorHash.put("PARENT_NAME", parentName);
		}
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
			e.printStackTrace();
		}
	}
	
	public Hashtable <String, Object> deinitReactor(String myName, String input, String output) {
		return deinitReactor(myName, input, output, true);
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
				if(put) {
					curReactor.put(output, value);
				} else {
					curReactor.set(output, value);
				}
			}
		} else if(reactorHash.size() > 0) { //if there is no parent reactor eg.: data.type
			//String self = (String) thisReactorHash.get("SELF");
			//if(self != null && reactorHash.containsKey(self)) {
			//curReactor = (IScriptReactor)reactorHash.get(self);
			if(put) {
				curReactor.put(output, value);
			} else {
				curReactor.set(output, value);
			}
		}
		
		// note that this method exists in the abstract
		// currently only a few reactors override this method
		// because the metadata is used outside translation
		IPkqlMetadata pkqlMetadata = thisReactor.getPkqlMetadata();
		if(pkqlMetadata != null) {
			storePkqlMetadata.add(pkqlMetadata);
			String explanationStrTest = pkqlMetadata.getExplanation();
			System.out.println(">>>>>>>>>>>>>> " + explanationStrTest);
		}
		
		return thisReactorHash;
	}

	private void synchronizeValues(String input, String[] values2Sync, IScriptReactor thisReactor) {
		for(int valIndex = 0;valIndex < values2Sync.length;valIndex++) {
			Object value = thisReactor.getValue(values2Sync[valIndex]);
			if(value != null) {
				curReactor.put(input + "_" + values2Sync[valIndex], value);
			}
		}		
	}

	/////////////////////////////////// DATA IMPORTING OPERATIONS //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS //////////////////////////////////////

	@Override
	public void caseAImportData(AImportData node)
	{
		inAImportData(node);
		// NEED TO PROCESS THE TABLE JOINS FIRST
		// THIS IS BECAUSE THE API REACTOR NEEDS THE
		// TABLE JOINS TO OPTIMIZE THE QUERY BEING USED
		if(node.getJoins() != null)
		{
			node.getJoins().apply(this);
		}
		
		// everything else takes the normal execution route
		if(node.getDataimporttoken() != null)
		{
			node.getDataimporttoken().apply(this);
		}
		if(node.getImport() != null)
		{
			node.getImport().apply(this);
		}
		outAImportData(node);
	}

	@Override
	public void inAImportData(AImportData node){
		if(reactorNames.containsKey(PKQLEnum.IMPORT_DATA)) {
			// make the determination to say if this is a frame.. yes it is
			/// if it is so change the reactor to the new reactor
			initReactor(PKQLEnum.IMPORT_DATA);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.IMPORT_DATA, nodeStr);
		}
	}

	@Override
	public void outAImportData(AImportData node) {
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
	public void inAApiBlock(AApiBlock node) {
		if(reactorNames.containsKey(PKQLEnum.API)) {
			// this is here because we are overriding the data.import order of execution to process the joins
			// before we process the iterator
			// curReactor at this point is still an ImportDataReactor
			List tableJoins = null;
			if(curReactor.getValue(PKQLEnum.JOINS) != null) {
				tableJoins = (List) curReactor.getValue(PKQLEnum.JOINS);
			}
						
			// now the curReactor will be some kind of ApiReactor based on strategy pattern described below
			// strategy pattern uses the engine to determine the type
			// assumption if not predefined, it is an engine name that is query-able via a IQueryInterpreter
			String engine = node.getEngineName().toString().trim();
			if(engine.equalsIgnoreCase("ImportIO") || engine.equalsIgnoreCase("AmazonProduct")){
				// we have a web api
				this.reactorNames.put(PKQLEnum.API, this.apiReactorNames.get(PKQLEnum.WEB_API));
			} else if(engine.equalsIgnoreCase("csvFile")) {
				// we have a csv api
				this.reactorNames.put(PKQLEnum.API, this.apiReactorNames.get(PKQLEnum.CSV_API));
			} else if(engine.equalsIgnoreCase("R")) {
				// we have an R api to connect
				this.reactorNames.put(PKQLEnum.API, this.apiReactorNames.get(PKQLEnum.R_API));
			} else {
				// default is a query api
				this.reactorNames.put(PKQLEnum.API, this.apiReactorNames.get(PKQLEnum.QUERY_API));
			}
			
			// make the api type
			// set in the values
			initReactor(PKQLEnum.API);
			
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.API, nodeStr);
			curReactor.put("ENGINE", engine);
			
			// something to do with parameters... need to look into this at some point...
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
			
			// add in the table joins if present
			if(tableJoins != null) {
				curReactor.put(PKQLEnum.TABLE_JOINS, tableJoins);
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
			if(values2Sync != null) {
				synchronizeValues(PKQLEnum.API, values2Sync, thisReactor);
			}
		}
		
		runner.setResponse(thisReactor.getValue("RESPONSE"));
		runner.setStatus((STATUS) thisReactor.getValue("STATUS"));
	}
	
	@Override
	public void inAColWhere(AColWhere node) {
		// still need a reactor for this piece
		// since a where clause can take in a term
		// it could be based on a formula
		// could be based on a api_blick
		// could be baed on a math evaluation
		// too many options... right now the reactor
		// only considers a csv_row
		// but could/should be expanded
		if(reactorNames.containsKey(PKQLEnum.WHERE)) {
			initReactor(PKQLEnum.WHERE);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.WHERE, nodeStr.trim());
			curReactor.put(PKQLEnum.COMPARATOR, (node.getEqualOrCompare()+"").trim());
		}		
	}

	@Override
	public void outAColWhere(AColWhere node) {
		String nodeStr = node.toString().trim();
		deinitReactor(PKQLEnum.WHERE, nodeStr, PKQLEnum.FILTER, false);
	}

	@Override
	public void inARelationDef(ARelationDef node) {
		// note: this operation does not require a frame
		// therefore, we do not need a reactor
		// and we do not need an out method
		
		// we store each relationship as a hashtable
		Hashtable<String, Object> relHash = new Hashtable<String, Object>();
		
		// get the relationship type
		TRelType type = node.getRelType();
		relHash.put(PKQLEnum.REL_TYPE, type.getText());
		
		// get the from column
		AColDef from = (AColDef) node.getFrom();
		relHash.put(PKQLEnum.FROM_COL, from.getColname().getText());

		// get the from column
		AColDef to = (AColDef) node.getTo();
		relHash.put(PKQLEnum.TO_COL, to.getColname().getText());
		
		//TODO: why do we define it as a RelDef and then every reactor to use it calls it Joins...
		curReactor.set(PKQLEnum.JOINS, relHash);
		curReactor.addReplacer(node.toString(), relHash);
	}
	
	@Override
	public void inAPastedData(APastedData node)
	{
		if(reactorNames.containsKey(PKQLEnum.PASTED_DATA)) {
			initReactor(PKQLEnum.PASTED_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.PASTED_DATA, nodeStr.trim());
			String word = ((APastedDataBlock) node.parent()).getDelimitier().toString().trim();
			curReactor.set(PKQLEnum.WORD_OR_NUM, (word.substring(1, word.length()-1))); // remove the quotes
		}
	}

	@Override
	public void outAPastedData(APastedData node)
	{
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		deinitReactor(PKQLEnum.PASTED_DATA, thisNode, PKQLEnum.PASTED_DATA);
		String [] values2Sync = curReactor.getValues2Sync(PKQLEnum.PASTED_DATA);
		synchronizeValues(PKQLEnum.PASTED_DATA, values2Sync, thisReactor);
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
		
		String engine = node.getEngine().toString().trim();
		String id = node.getEngineId().toString().trim();
		curReactor.put("DATA_OPEN_ENGINE", engine);
		curReactor.put("DATA_OPEN_ID", id);
		
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.OPEN_DATA, nodeOpen, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.OPEN_DATA);
		
		Map<String, Object> webData = (Map<String, Object>)previousReactor.getValue("webData");
//		runner.setDataMap(webData);
		
		IDataMaker dm = (IDataMaker)curReactor.getValue("G");
		if(curReactor.getValue("G") instanceof Dashboard) {
			Dashboard dash = (Dashboard)curReactor.getValue("G");
			dash.setInsightOutput((String)webData.get("insightID"), webData);
		}
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
		curReactor.put(PKQLEnum.VIZ, node.toString());
		curReactor.put("layout", node.getLayout().getText());
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
		if(node.getUioptions() != null){
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


	// this is just grabbing the comment information and storing it in the
	// runner
	// should be split out into reactor
	@Override
	public void outAPanelComment(APanelComment node) {
		System.out.println("out a viz change");

		// get the comment id
		String nodeCommentString = node.getPanelcommentadd().toString();
		String cid = "0";
		if (nodeCommentString.contains(".comment[")) {
			nodeCommentString = nodeCommentString.substring(nodeCommentString.indexOf(".comment[") + 9);
			cid = nodeCommentString.substring(0, nodeCommentString.indexOf("]"));
		}

		// set the information
		Map<String, Object> commentMap = new HashMap<String, Object>();
		String textWithQuotes = node.getText().toString().trim();
		commentMap.put("text", textWithQuotes.substring(1, textWithQuotes.length() - 1)); // remove
		commentMap.put("group", node.getGroup().toString().trim());
		commentMap.put("type", node.getType().toString().trim());
		commentMap.put("location", node.getLocation().toString().trim());
		commentMap.put("commentId", cid);
		Map comments = (Map) runner.getFeData("comments");
		if (comments == null) {
			comments = new HashMap();
		}
		curReactor.put("commentAdded", commentMap);
		deinitReactor(PKQLEnum.VIZ, "", "");
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

	// this is just grabbing the comment information and storing it in the
	// runner
	// should be split out into reactor
	@Override
	public void outAPanelCommentEdit(APanelCommentEdit node) {
		System.out.println("out a viz comment edit");

		// get the comment id
		String nodeCommentString = node.getPanelcommentedit().toString();
		String cid = "0";
		if (nodeCommentString.contains(".comment[")) {
			nodeCommentString = nodeCommentString.substring(nodeCommentString.indexOf(".comment[") + 9);
			cid = nodeCommentString.substring(0, nodeCommentString.indexOf("]"));
		}

		// set the information
		Map<String, Object> commentMap = new HashMap<String, Object>();
		String textWithQuotes = node.getText().toString().trim();
		commentMap.put("text", textWithQuotes.substring(1, textWithQuotes.length() - 1)); // remove
		commentMap.put("group", node.getGroup().toString().trim());
		commentMap.put("type", node.getType().toString().trim());
		commentMap.put("location", node.getLocation().toString().trim());
		commentMap.put("commentId", cid);
		Map comments = (Map) runner.getFeData("comments");
		if (comments == null) {
			comments = new HashMap();
		}
		comments.put(cid, commentMap);
		curReactor.put("commentEdited", commentMap);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.addFeData("comments", comments, true);
		runner.setResponse("Successfully edited comment " + cid + " : " + node.getText().toString().trim());
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
		curReactor.put("commentRemoved", true);
		deinitReactor(PKQLEnum.VIZ, "", "");

		// get the comment id
		String nodeCommentString = node.getPanelcommentremove().toString();
		String cid = "0";
		if (nodeCommentString.contains(".comment[")) {
			nodeCommentString = nodeCommentString.substring(nodeCommentString.indexOf(".comment[") + 9);
			cid = nodeCommentString.substring(0, nodeCommentString.indexOf("]"));
		}

		// set it as closed
		Map<String, Object> commentMap = new HashMap<String, Object>();
		commentMap.put("closed", true);

		Map comments = (Map) runner.getFeData("comments");
		if (comments == null) {
			comments = new HashMap();
		}
		comments.put(cid, commentMap);
		runner.addFeData("comments", comments, true);
		runner.setResponse("Successfully removed comment " + cid);
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}


	@Override
	public void inAPanelClone(APanelClone node){
		System.out.println("in a panel clone");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelClone(APanelClone node) {
		System.out.println("out a panel clone");
		String newId = node.getNewid().getText();
		// we add to the current fe data the new panel id
		runner.addFeData("newPanelId", newId, false);
		// also copy current state to this new panel id
		runner.copyFeData(newId);
		curReactor.put("oldPanel", node.getPanelclone().toString()); // explain
		curReactor.put("clone", newId); // explain
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully cloned! New panel id: " + newId);
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelLookAndFeel(APanelLookAndFeel node){
		System.out.println("in a panel laf");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelLookAndFeel(APanelLookAndFeel node) {
		System.out.println("out a panel laf");
		Map laf = (Map) runner.getFeData("lookandfeel");
		if (laf == null) {
			laf = new HashMap();
		}
		laf.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		curReactor.put("lookAndFeel", laf);
		runner.addFeData("lookandfeel", laf, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set look and feel");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelTools(APanelTools node){
		System.out.println("in a panel tools");
		initReactor(PKQLEnum.VIZ);
	}
	
	@Override
	public void outAPanelTools(APanelTools node) {
		System.out.println("out a panel tools");
		Map tools = (Map) runner.getFeData("tools");
		if (tools == null) {
			tools = new HashMap();
		}
		tools.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		runner.addFeData("tools", tools, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set tools");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelConfig(APanelConfig node){
		System.out.println("in a panel config");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelConfig(APanelConfig node) {
		System.out.println("out a panel config");
		Map config = (Map) runner.getFeData("config");
		if (config == null) {
			config = new HashMap();
		}
		config.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		curReactor.put("configMap", config);
		runner.addFeData("config", config, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set config");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelClose(APanelClose node){
		System.out.println("in a panel close");
		initReactor(PKQLEnum.VIZ);
	}

	@Override
	public void outAPanelClose(APanelClose node) {
		System.out.println("out a panel close");
		runner.addFeData("closed", true, true);
		curReactor.put("closedPanel", (String) node.getPanelclose().toString());
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully closed panel");
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
	
	@Override
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
	
	@Override
	public void outADashboardJoin(ADashboardJoin node)
	{
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		curReactor.put("G", this.frame);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DASHBOARD_JOIN, nodeStr, PKQLEnum.DASHBOARD_JOIN);
		
		Map dashboardData = (Map)runner.getDashboardData();
		if(dashboardData == null) {
			runner.setDashBoardData(thisReactor.getValue("DashboardData"));
		} else {
			Map<String, List> newDashboardData = (Map<String, List>)thisReactor.getValue("DashboardData");
			if(dashboardData.containsKey("joinedInsights")) {
				List list = (List)dashboardData.get("joinedInsights");
				list.addAll(newDashboardData.get("joinedInsights"));
			} else {
				List list = (List)newDashboardData.get("joinedInsights");
				dashboardData.put("joinedInsights", list);
			}
			runner.setDashBoardData(dashboardData);
		}
	}
	
	public void inADashboardAdd(ADashboardAdd node)
    {
		System.out.println("Have dashboard join as " + node);
		if (reactorNames.containsKey(PKQLEnum.DASHBOARD_ADD)) {
			initReactor(PKQLEnum.DASHBOARD_ADD);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.DASHBOARD_ADD, nodeStr.trim());
		}
    }

    public void outADashboardAdd(ADashboardAdd node)
    {
    	String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		curReactor.put("G", this.frame);
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DASHBOARD_ADD, nodeStr,	PKQLEnum.DASHBOARD_ADD);
		
		Map dashboardData = (Map)runner.getDashboardData();
		if(dashboardData == null) {
			runner.setDashBoardData(thisReactor.getValue("DashboardData"));
		} else {
			Map<String, List> newDashboardData = (Map<String, List>)thisReactor.getValue("DashboardData");
			if(dashboardData.containsKey("addedInsights")) {
				List list = (List)dashboardData.get("addedInsights");
				list.addAll(newDashboardData.get("addedInsights"));
			} else {
				List list = (List)newDashboardData.get("addedInsights");
				dashboardData.put("addedInsights", list);
			}
			runner.setDashBoardData(dashboardData);
		}
    }

	public void inADashboardopScript(ADashboardopScript node) {
		runner.openFeDataBlock("Dashboard");
		runner.addFeData("panelId", "Dashboard", true);
    }
	
    public void inADashboardConfig(ADashboardConfig node) {
//    	initReactor(PKQLEnum.VIZ);
    }

    public void outADashboardConfig(ADashboardConfig node) {
    	System.out.println("out a dashboard config");
		
		Dashboard dm = (Dashboard)this.frame;
		
		String json = node.getJsonblock().getText();
		json = json.replace("<json>", "");
		json = json.replace("</json>", "");
		try {
			Map object = new Gson().fromJson(json, HashMap.class);
			dm.setConfig(object);
		} catch(Exception e) {
			dm.setConfig(json);
		}

		runner.setResponse("Successfully set config");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
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
    public void caseADataFrame(ADataFrame node)
    {
        inADataFrame(node);
        outADataFrame(node);
    }
	
	@Override
	public void inADataFrame(ADataFrame node) {
		if(reactorNames.containsKey(PKQLEnum.DATA_FRAME)) {
			// get the appropriate reactor
			initReactor(PKQLEnum.DATA_FRAME);
			String word = ((AAlphaWordOrNum) node.getBuilder()).getWord().getText().trim();
			String cleanWord = word.substring(1, word.length()-1);// remove the quotes
			curReactor.put(DataFrameReactor.DATA_FRAME_TYPE, cleanWord);
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
	public void inAApiTerm(AApiTerm node) {
		// an example of this is when we use a term
		// inside a user_input reactor to get the list
		// of values for a parameter
		// without adding to a frame
		
		// it just goes through the api block
		// and will be grabbed that way
		// this doesn't do anything else
	}
	
	@Override
	public void inACsvTerm(ACsvTerm node) {
		// an example of this is [ "WB" , "Fox" ] inside a 
		// where clause of c:Studio = ["WB","Fox"] which is 
		// used in col filter and data importing
		
		// it just goes through inACSVRow
		// can grab it that way
		// this doesn't do anything else
		System.out.println("in a csv term");
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
	public void inAFlexSelectorRow(AFlexSelectorRow node) {
		//TODO: really need to build this out...
		if(node.getTerm() != null) {
			curReactor.set("TERM", node.getTerm()+"");
		}
	}
	
	public void inAColTerm(AColTerm node) {
		System.out.println("in a col term");
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
		}
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

			//for panel.viz2
			curReactor.put("MATH_EXPRESSION", node.toString().trim());

			curReactor.put(PKQLEnum.PROC_NAME, procedureName); // don't need once all algorithms have been refactored into Reactors
			if(expr != null)
				curReactor.put(PKQLEnum.EXPR_TERM, expr);
		}	
	}

	@Override
	//TODO: LOOK INTO THIS
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
	
	
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE ////////////////////////////////////////////////////
	
	@Override
	public void caseAMathParam(AMathParam node) {
		// this is literally just a comma with a map object
		// we apply the map object
		node.getMapObj().apply(this);
		// the out will take that map object
		// and put it back with a Math_Param key
		outAMathParam(node);
	}

	@Override
	public void caseAMapObj(AMapObj node) {
		// we have the map object specifically go through the key-value
		// pair such that it is responsbile for how it will
		// aggregate the pieces
		// in this case, it puts them all into a single map
		inAMapObj(node);
	}

	@Override
	public void inAMapObj(AMapObj node) {
		// values will store the user input map object
		Map<Object, Object> values = new Hashtable<Object, Object>();

		// we specifically call the processing on each of the key-value
		// groups such that we are responsible for the aggregation of
		// these pieces
		if(node.getKeyvalue() != null)
		{
			node.getKeyvalue().apply(this);
			// going through a key-value will put a map in mystore
			// that only has one key
			values.putAll((Map<Object, Object>) curReactor.removeLastStoredKey());
		}
		List<PKeyvalueGroup> copy = new ArrayList<PKeyvalueGroup>(node.getKeyvalueGroup());
		for(PKeyvalueGroup e : copy)
		{
			e.apply(this);
			// going through a key-value will put a map in mystore
			// that only has one key
			values.putAll((Map<Object, Object>) curReactor.removeLastStoredKey());
		}
		curReactor.put(PKQLEnum.MAP_OBJ, values);
	}

	@Override
	public void caseAKeyvalue(AKeyvalue node) {
		// key-value will create a map with a single key
		inAKeyvalue(node);
	}

	@Override
	public void inAKeyvalue(AKeyvalue node) {
		// get the key for the map
		// here we just apply on the word_or_num
		node.getWord1().apply(this);
		Object mapKey = curReactor.removeLastStoredKey();

		// now we get the value of the map
		// and this can be another word_or_num
		// or, it could be another map, which would go through the flow from
		// the start of this section again
		node.getWord2().apply(this);
		Object mapValue = curReactor.removeLastStoredKey();

		// we store the key and value and return it
		Map<Object, Object> map = new Hashtable<Object, Object>();
		map.put(mapKey, mapValue);
		curReactor.put(PKQLEnum.KEY_VALUE_PAIR, map);
	}

	@Override
	public void caseAKeyvalueGroup(AKeyvalueGroup node) {
		// this code will only go through the key value
		// anything that is using a keyValueGroup will be responsible
		// for aggregating the keyValues into the proper structure they want
		// currently, only thing using this is a MapObj and we get the last 
		// key-value group after each apply
		node.getKeyvalue().apply(this);
	}

	@Override
	public void outAMathParam(AMathParam node) {
		// this is called right after a map object has just been placed into the current reactor
		// just grab that map and put it back into the reactor but with a Math_Param key
		Map<Object, Object> mathParamMapObj = (Map<Object, Object>) curReactor.removeLastStoredKey();
		curReactor.put(PKQLEnum.MATH_PARAM, mathParamMapObj);
	}

	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR MANIPULATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR MANIPULATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR MANIPULATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR MANIPULATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR MANIPULATION HERE ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR MANIPULATION HERE ////////////////////////////////////////////////////

    @Override
    public void caseACsvTable(ACsvTable node) {
        inACsvTable(node);
    }
	
	@Override
	public void inACsvTable(ACsvTable node) {
		List<List<Object>> csvTable = new Vector<List<Object>>();
		
		List<PCsvRow> copy = new ArrayList<PCsvRow>(node.getCsvRow());
        for(PCsvRow e : copy) {
            e.apply(this);
            csvTable.add((List<Object>) curReactor.removeLastStoredKey());
        }
        curReactor.put(PKQLEnum.CSV_TABLE, csvTable); 
	}
	
    @Override
    public void caseAColCsv(AColCsv node) {
        inAColCsv(node);
    }
	
	@Override
	public void inAColCsv(AColCsv node) {
		// note: this operation does not require a frame
		// therefore, we do not need a reactor
		// and we do not need an out method

		// create the array to store the list of values
		List<String> colVec = new Vector<String>();
		
		// a col csv requires at least one input
		// grab that input -> defined as a col def
		AColDef col = (AColDef) node.getColDef();
		colVec.add(col.getColname().getText());
		
		// the col csv may contain multiple other col defs
		// grab that list and iterate through it to add the other
		// cols that are defined
		LinkedList<PColGroup> optionalCols = node.getColGroup();
		if(optionalCols != null && !optionalCols.isEmpty()) {
			ListIterator<PColGroup> it = optionalCols.listIterator();
			while(it.hasNext()) {
				AColGroup group = (AColGroup) it.next();
				colVec.add( ((AColDef) group.getColDef()).getColname().getText() );
			}
		}
		
		curReactor.put(PKQLEnum.COL_CSV, colVec);
		curReactor.addReplacer(node.toString(), colVec);
	}

    @Override
    public void caseACsvRow(ACsvRow node) {
        inACsvRow(node);
    }
	
	@Override
	public void inACsvRow(ACsvRow node) {
		// note: this operation does not require a frame
		// therefore, we do not need a reactor
		// and we do not need an out method

		// create the array to store the list of values
		List<Object> rowVec = new Vector<Object>();
		
		// we use the logic defined in the general caseACsvRow
		// to iterate through all the constituent parts
		// these parts are put into the myStore and we grab them
		// out and put them inside the rowVec
		if(node.getWordOrNum() != null) {
            node.getWordOrNum().apply(this);
            rowVec.add(curReactor.removeLastStoredKey());
        }
        List<PCsvGroup> copy = new ArrayList<PCsvGroup>(node.getCsvGroup());
        for(PCsvGroup e : copy) {
            e.apply(this);
            rowVec.add(curReactor.removeLastStoredKey());
        }
		
		curReactor.put(PKQLEnum.ROW_CSV, rowVec);
		curReactor.addReplacer(node.toString(), rowVec);
	}
	
	@Override
	public void inAAlphaWordOrNum(AAlphaWordOrNum node) {
		String word = (node.getWord() + "").trim();
		String cleaned = word.substring(1, word.length()-1);// remove the quotes
		curReactor.put(PKQLEnum.WORD_OR_NUM, cleaned); 
		curReactor.addReplacer(word, cleaned);
	}
	
	@Override
	public void inANumWordOrNum(ANumWordOrNum node) {
		ADecimal dec = (ADecimal) node.getDecimal();
		String fraction = dec.getFraction() + "";
		Number num = null;
		String number = dec.getWhole().toString().trim();
		if(dec.getFraction() != null) {
			number = number + "." + fraction;
			num = Double.parseDouble(number);
		} else {
			num = Integer.parseInt(number);
		}
		curReactor.put(PKQLEnum.WORD_OR_NUM, num);
	}
	
	@Override
	public void inAExprWordOrNum(AExprWordOrNum node) {

	}

	@Override
	public void inADecimal(ADecimal node) {
		String fraction = node.getFraction() + "";
		String number = node.getWhole().toString().trim();
		if(node.getFraction() != null) {
			number = number + "." + fraction;
		}
		curReactor.addReplacer(node.toString().trim(), Double.parseDouble(number));
	}
	
	@Override
	public void inAColDef(AColDef node) {
		String colName = node.getColname().toString().trim();
		curReactor.set(PKQLEnum.COL_DEF, colName);
		curReactor.addReplacer((node + "").trim(), colName);
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
    	String nodeDataconnect = node.getDataconnectToken().toString().trim();
    	String thisNode = node.toString().trim();
    	IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_CONNECT, nodeDataconnect, PKQLEnum.DATA_CONNECT);
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
	
    public void inAJOp(AJOp node)
    {
		if (reactorNames.containsKey(PKQLEnum.JAVA_OP)) {
			initReactor(PKQLEnum.JAVA_OP);
			curReactor.put("PKQLRunner", runner);
			String nodeExpr = node.getCodeblock().toString().trim();
			curReactor.put(PKQLEnum.JAVA_OP, nodeExpr);
		}
    }

    public void outAJOp(AJOp node)
    {
		if (reactorNames.containsKey(PKQLEnum.JAVA_OP)) {
    	deinitReactor(PKQLEnum.JAVA_OP, node.getCodeblock()+"", null, false);
		runner.setResponse(curReactor.getValue("RESPONSE"));
		runner.setStatus((STATUS) curReactor.getValue("STATUS"));
		}
		// highest level of the grammar definition
		// need to call post process to aggregate into runner
		postProcess(node);
    }
    
    public void inADatanetworkconnect(ADatanetworkconnect node)
    {
		if (reactorNames.containsKey(PKQLEnum.NETWORK_CONNECT)) {
			initReactor(PKQLEnum.NETWORK_CONNECT);
			//curReactor.put(PKQLEnum.NETWORK_CONNECT, "CONNECT");
			if(node.getTablename() != null)
				curReactor.put("TABLE_NAME", node.getTablename());
		}
    }

    public void outADatanetworkconnect(ADatanetworkconnect node)
    {
		if (reactorNames.containsKey(PKQLEnum.NETWORK_CONNECT)) 
		{
	    	deinitReactor(PKQLEnum.NETWORK_CONNECT, PKQLEnum.NETWORK_CONNECT, null, false);
			runner.setResponse(curReactor.getValue("RESPONSE"));
			runner.setStatus((STATUS) curReactor.getValue("STATUS"));
		}
    }

    public void inADatanetworkdisconnect(ADatanetworkdisconnect node)
    {
		if (reactorNames.containsKey(PKQLEnum.NETWORK_DISCONNECT)) {
			initReactor(PKQLEnum.NETWORK_DISCONNECT);
		}
    }

    public void outADatanetworkdisconnect(ADatanetworkdisconnect node)
    {
    	deinitReactor(PKQLEnum.NETWORK_DISCONNECT, PKQLEnum.NETWORK_DISCONNECT, null, false);
    }

    
	//**************************************** SYNCHRONIZATION OF DATAMAKER ****************************************//
    
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
	
	//**************************************** DATABASE RELATED OPERATIONS ****************************************//
	
	public void inADatabaseList(ADatabaseList node)
	{
		System.out.println("Translation.inADatabaseList() with node = "+ node );
		if(reactorNames.containsKey(PKQLEnum.DATABASE_LIST)) {
			initReactor(PKQLEnum.DATABASE_LIST);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.DATABASE_LIST, nodeStr);
		}
	}

	public void outADatabaseList(ADatabaseList node)
	{
		System.out.println("Translation.outADatabaseList() with node = "+ node );
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATABASE_LIST, thisNode, PKQLEnum.DATABASE_LIST);
		runner.setResponse(thisReactor.getValue(PKQLEnum.DATABASE_LIST));
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
		runner.setCurrentString(PKQLEnum.DATABASE_LIST);
		runner.storeResponse();
		System.out.println("");
	}
	
	public void inADatabaseConcepts(ADatabaseConcepts node)
	{
		System.out.println("Translation.inADatabaseConcepts() with node = "+ node );
		if(reactorNames.containsKey(PKQLEnum.DATABASE_CONCEPTS)) {
			initReactor(PKQLEnum.DATABASE_CONCEPTS);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.DATABASE_CONCEPTS, nodeStr);
		}
	}

	public void outADatabaseConcepts(ADatabaseConcepts node)
	{
		System.out.println("Translation.outADatabaseConcepts() with node = "+ node );
		String nodeDatabaseConcepts = node.getDatabaseconceptsToken().toString().trim();
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATABASE_CONCEPTS, nodeDatabaseConcepts, PKQLEnum.DATABASE_CONCEPTS);
		runner.setResponse(thisReactor.getValue(PKQLEnum.DATABASE_CONCEPTS));
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
		runner.setCurrentString(PKQLEnum.DATABASE_CONCEPTS);
		runner.storeResponse();
		System.out.println("");
	}
    
}