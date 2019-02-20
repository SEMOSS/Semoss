package prerna.sablecc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.impl.BaseReducerReactor;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IScriptReactor;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.nameserver.utility.MetamodelVertex;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.analysis.DepthFirstAdapter;
import prerna.sablecc.expressions.r.builder.RExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.ExpressionGenerator;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.meta.DataInsightMetaData;
import prerna.sablecc.meta.GenericMetaData;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.node.*;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.Constants;

public class Translation extends DepthFirstAdapter {
	
	private static final Logger LOGGER = LogManager.getLogger(Translation.class.getName());

	// this is the third version of this shit I am building
	// I need some way of having logical points for me to know when to start
	// another reactor
	// for instance I could have an expr term within a math function which
	// itself could be within another expr term
	// the question then is do I have 2 expr terms etc.
	// so I start the first expr term
	// I start assimilating
	// get to a point where I start a new one
	// my vector tells me that
	// the init and deinit should take care of it I bet ?
	// how do I know when I am done ?
	// it has to be invoked at the last level
	Hashtable<String, Object> reactorHash = null;
	IScriptReactor curReactor = null;

	// reactor vector
	Vector<Hashtable<String, Object>> reactorHashHistory = new Vector<Hashtable<String, Object>>();
	Vector<Hashtable<String, Object>> reactorStack = new Vector<Hashtable<String, Object>>();

	// set of reactors
	// which serves 2 purposes
	// a. Where to initiate
	// b. What is the name of the reactor

	// frame specific reactors are stored in reactorNames
	// used for strategy pattern of pkql input to specific reactor class
	Map<String, String> reactorNames = new Hashtable<String, String>();

//	// used for strategy pattern of pkql input to specific api type
//	// this is not a part of reactor names since we have not defined these are
//	// constants
//	// within the postfix, but are passed as string values in the "engineName"
//	// component of a data.import. It has to be defined as a string since the
//	// engine names
//	// that could exist should obviously not be stored within the postfix
//	Map<String, String> apiReactorNames = new Hashtable<String, String>();

	// keep a list of pkql metadata that is useful outside of translation
	// currently only used for csv file imports since need this information to
	// later create an engine and modify a pkql string upon saving of insight
	List<IPkqlMetadata> storePkqlMetadata = new Vector<IPkqlMetadata>();

	IDataMaker frame = null;
	PKQLRunner runner = null;

	/**
	 * Constructor for translation Used for PKQL operations where a frame is not
	 * required
	 * 
	 * @param runner
	 */
	public Translation(PKQLRunner runner) {
		this.runner = runner;
		this.reactorNames = getDefaultReactors();
	}

	private Map<String, String> getDefaultReactors() {
		Map<String, String> defaultReactors = new Hashtable<String, String>();
		// this is the base so we can query on an engine without needing a frame
		defaultReactors.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
		// this is the base so we can run a custom query on an engine without needing a frame
		defaultReactors.put(PKQLEnum.RAW_API, "prerna.sablecc.RawQueryApiReactor");
		// this is for searching instances
		defaultReactors.put(PKQLEnum.SEARCH_QUERY_API, "prerna.sablecc.SearchQueryApiReactor");
		// this is the outside wrapper for the search query
		defaultReactors.put(PKQLEnum.QUERY_DATA, "prerna.sablecc.QueryDataReactor");
		// this is to get the recipe for a pkqled insight and initialize the insight with an id
		defaultReactors.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
		// this is to run the recipe for an insight and replace the current insight by the insight returned from this output data
		defaultReactors.put(PKQLEnum.OUTPUT_DATA, "prerna.sablecc.OutputDataReactor");
		// this is used to clear the cache
		defaultReactors.put(PKQLEnum.CLEAR_CACHE, "prerna.sablecc.CacheReactor");
		// this is used to set whether to use cache
		defaultReactors.put(PKQLEnum.USE_CACHE, "prerna.sablecc.SetCacheReactor");
		// for where statements
		defaultReactors.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		// able to generate a frame
		defaultReactors.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		
		// for inputs/outputs
		defaultReactors.put(PKQLReactor.VAR.toString(), "prerna.sablecc.VarReactor");
		defaultReactors.put(PKQLReactor.INPUT.toString(), "prerna.sablecc.InputReactor");

		return defaultReactors;
	}

	/**
	 * Constructor that takes in the dataframe that it will perform its
	 * calculations off of and the runner that invoked the translation
	 * 
	 * @param frame
	 *            IDataMaker
	 * @param runner
	 *            PKQLRunner: holds response from PKQL script and the status of
	 *            whether the script errored or not
	 */
	public Translation(IDataMaker frame, PKQLRunner runner) {
		this(runner);
		// set the dataframe and add frame specific reactors
		this.frame = frame;
		Map<String, String> frameReactorNames = frame.getScriptReactors();
		if (reactorNames != null) {
			this.reactorNames.putAll(frameReactorNames);
		}

	}

	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT
	/////////////////////////////////// OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT
	/////////////////////////////////// OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT
	/////////////////////////////////// OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT
	/////////////////////////////////// OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT
	/////////////////////////////////// OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// HIGHLEST LEVEL PKQL IN AND OUT
	/////////////////////////////////// OPERATIONS
	/////////////////////////////////// //////////////////////////////////////

	@Override
	public void inAConfiguration(AConfiguration node) {
		System.out.println(node.toString());
		
		List<PScript> pkqlToRun = node.getScript();

		int index = 0;
		while (index < pkqlToRun.size()) {
			PScript script = pkqlToRun.get(0);
			if (runner.unassignedVars.isEmpty() || script instanceof AVaropScript) { // if no vars are unassigned.. we are good. otherwise we only look for their assignment
				// PVarop varop = ((AVaropScript)script).getVarop();
				PScript pkqlToExecute = pkqlToRun.remove(0);
				pkqlToExecute.apply(this);
				index = 0;
			} else {
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
		if (reactorNames.containsKey(PKQLEnum.EXPR_SCRIPT)) {
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
		// since an expression may not be fully executed
		// ex. 55 * c:Movie_Budget / m:Sum([c:Revenue_International]) ;
		// which will only execute the sum and do a replace without adding back a value
		// we need to compute when necessary
		// TODO: really need an expression iterator for other frames...
		// TODO: really need an expression iterator for other frames...
		// TODO: really need an expression iterator for other frames...
		// TODO: really need an expression iterator for other frames...
//		Object exprResult = curReactor.getValue(nodeStr);
//		if(exprResult instanceof String && !exprResult.toString().trim().isEmpty()) {
//			// since multiple math routines can be added together
//			// need to get a unique set of values used in the join
//			Vector<String> columns = (Vector <String>) curReactor.getValue(PKQLEnum.COL_DEF);
//			Set<String> joinCols = new HashSet<String>();
//			joinCols.addAll(columns);
//			String[] joins = joinCols.toArray(new String[]{});
//
//			List<String> exprList = new Vector<String>();
//			exprList.add(exprResult.toString());
//			List<String> newColList = new Vector<String>();
//			newColList.add("EXPRESSION_COL");
//
//			if(frame instanceof H2Frame) {
//				H2SqlExpressionIterator it = new H2SqlExpressionIterator((H2Frame) curReactor.getValue("G"), exprList, newColList, joins, null);
//				this.runner.setResponse(it);
//			}
//		}
		postProcess(node);
	}

	@Override
	public void outAFormula(AFormula node)
	{
		// ugh... if there is a value associated with the expression
		// we need to push it up to also match for the formula
		// to fix when there are additional things like
		// parenthesis and brackets
		if(curReactor != null) {
			String previousExpr = node.getExpr().toString().trim();
			if(curReactor.getValue(previousExpr) != null) {
				curReactor.put(node.toString().trim(), curReactor.getValue(previousExpr));
			}
		}
	}

	// at the highest level, make sure to save to the runner as a completed
	// expression
	@Override
	public void inAHelpScript(AHelpScript node) {
		// TODO: build out a String that explains PKQL and the commands
		runner.setResponse("Welcome to PKQL. Please look through documentation to find available functions.");
		runner.setStatus(STATUS.SUCCESS);
	}

	@Override
	public void outAHelpScript(AHelpScript node) {
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed
	// expression
	@Override
	public void outAVaropScript(AVaropScript node) {
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed
	// expression
	@Override
	public void outAColopScript(AColopScript node) {
		postProcess(node);
	}

	// at the highest level, make sure to save to the runner as a completed
	// expression
	@Override
	public void outAPanelopScript(APanelopScript node) {
		postProcess(node);
	}

	public void outADashboardopScript(ADashboardopScript node) {
		postProcess(node);
	}

	public void outADataopScript(ADataopScript node) {
		postProcess(node);
	}

	public void outADatabaseopScript(ADatabaseopScript node) {
		postProcess(node);
	}
	
	public void outAJOpScript(AJOpScript node) {
		postProcess(node);
	}

	// the highest level above all commands
	// tracks the most basic things all pkql should have
	private void postProcess(Node node) {
		runner.setCurrentString(node.toString());
		runner.aggregateMetadata(this.storePkqlMetadata);
		runner.storeResponse();

		// we need to clear the previous references to the metadata
		// these get stored within the PKQLTransformation and pushed onto the
		// insight
		// but the translation must lose the reference such that the recipe
		// explanation
		// which builds of all the internal explanations of embedded pkqls,
		// doesn't need
		// to use the previous ones
		this.storePkqlMetadata = new Vector<IPkqlMetadata>();
	}

	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF
	/////////////////////////////////// REACTORS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF
	/////////////////////////////////// REACTORS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF
	/////////////////////////////////// REACTORS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF
	/////////////////////////////////// REACTORS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF
	/////////////////////////////////// REACTORS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// INITIALIZATION AND DEINITALIZATION OF
	/////////////////////////////////// REACTORS
	/////////////////////////////////// //////////////////////////////////////

	public void initReactor(String myName) {
		String parentName = null;
		if (reactorHash != null) {
			// I am not sure I need to add element here
			// I need 2 things in here
			// I need the name of a parent i.e. what is my name and my parent
			// name
			// actually I just need my name
			parentName = (String) reactorHash.get("SELF");
		}
		reactorHash = new Hashtable<String, Object>();
		if (parentName != null) {
			reactorHash.put("PARENT_NAME", parentName);
		}
		reactorHash.put("SELF", myName);
		reactorStack.addElement(reactorHash);

		// I should also possibly initialize the reactor here
		try {
			String reactorName = reactorNames.get(myName);
			curReactor = (IScriptReactor) Class.forName(reactorName).newInstance();
			curReactor.put(PKQLEnum.G, frame);
			curReactor.setInsightId(this.runner.getInsightId());
			// this is how I can get access to the parent when that happens
			reactorHash.put(myName, curReactor);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public Hashtable<String, Object> deinitReactor(String myName, String input, String output) {
		return deinitReactor(myName, input, output, true);
	}

	public Hashtable<String, Object> deinitReactor(String myName, String input, String output, boolean put) {
		Hashtable<String, Object> thisReactorHash = reactorStack.lastElement();
		reactorStack.remove(thisReactorHash);
		IScriptReactor thisReactor = (IScriptReactor) thisReactorHash.get(myName);
		// this is still one level up
		boolean hasError = false;
		String errorMessage = null;
		Object value = null;
		try {
			thisReactor.process();
			value = thisReactor.getValue(input);
			System.out.println("Value is .. " + value);
		} catch(Exception e) {
			e.printStackTrace();
			LOGGER.info("There was an error with deinit for " + myName);
			hasError = true;
			if(e.getMessage() != null) {
				errorMessage = e.getMessage();
			}
		}

		if (reactorStack.size() > 0) {
			reactorHash = reactorStack.lastElement();
			// also set the cur reactor
			String parent = (String) thisReactorHash.get("PARENT_NAME");

			// if the parent is not null
			if (parent != null && reactorHash.containsKey(parent)) {
				curReactor = (IScriptReactor) reactorHash.get(parent);
				if(!hasError) {
					if (put) {
						curReactor.put(output, value);
					} else {
						curReactor.set(output, value);
					}
				} else {
					// this is for cases where we dont want to completely break execution
					// which is the case for parameter insights where there are many data.imports
					// but not all of them will return for every user input
					// but we need the current parent to not break
					curReactor.put(PKQLEnum.CHILD_ERROR, true);
					if(errorMessage != null) {
						curReactor.put(PKQLEnum.CHILD_ERROR_MESSAGE, errorMessage);
					}
				}
			}
		} else if (reactorHash.size() > 0) { // if there is no parent reactor
												// eg.: data.type
			// String self = (String) thisReactorHash.get("SELF");
			// if(self != null && reactorHash.containsKey(self)) {
			// curReactor = (IScriptReactor)reactorHash.get(self);
			
			if(!hasError) {
				if (put) {
					curReactor.put(output, value);
				} else {
					curReactor.set(output, value);
				}
			} else {
				curReactor.put(PKQLEnum.CHILD_ERROR, true);
				if(errorMessage != null) {
					curReactor.put(PKQLEnum.CHILD_ERROR_MESSAGE, errorMessage);
				}
			}
		}

		// note that this method exists in the abstract
		// currently only a few reactors override this method
		// because the metadata is used outside translation
		IPkqlMetadata pkqlMetadata = thisReactor.getPkqlMetadata();
		if (pkqlMetadata != null) {
			storePkqlMetadata.add(pkqlMetadata);
			String explanationStrTest = pkqlMetadata.getExplanation();
			System.out.println(">>>>>>>>>>>>>> " + explanationStrTest);
		}

		return thisReactorHash;
	}

	private void synchronizeValues(String input, String[] values2Sync, IScriptReactor thisReactor) {
		for (int valIndex = 0; valIndex < values2Sync.length; valIndex++) {
			Object value = thisReactor.getValue(values2Sync[valIndex]);
			if (value != null) {
				curReactor.put(input + "_" + values2Sync[valIndex], value);
			}
		}
	}

	/////////////////////////////////// DATA IMPORTING OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS
	/////////////////////////////////// //////////////////////////////////////
	/////////////////////////////////// DATA IMPORTING OPERATIONS
	/////////////////////////////////// //////////////////////////////////////

	@Override
	public void caseAQueryData(AQueryData node) {
		inAQueryData(node);
		// NEED TO PROCESS THE TABLE JOINS FIRST
		// THIS IS BECAUSE THE API REACTOR NEEDS THE
		// TABLE JOINS TO OPTIMIZE THE QUERY BEING USED
		if (node.getJoins() != null) {
			node.getJoins().apply(this);
		}

		// everything else takes the normal execution route
		if (node.getDataquerytoken() != null) {
			node.getDataquerytoken().apply(this);
		}
		if (node.getImport() != null) {
			node.getImport().apply(this);
		}
        outAQueryData(node);
	}
	
	@Override
    public void inAQueryData(AQueryData node) {
		if (reactorNames.containsKey(PKQLEnum.QUERY_DATA)) {
			// make the determination to say if this is a frame.. yes it is
			/// if it is so change the reactor to the new reactor
			initReactor(PKQLEnum.QUERY_DATA);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.QUERY_DATA, nodeStr);
			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.SEARCH_QUERY_API));
		}
    }

	@Override
    public void outAQueryData(AQueryData node) {
		String nodeImport = node.getImport().toString().trim();
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeImport);
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.QUERY_DATA, nodeImport, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.QUERY_DATA);
//		runner.setNewColumns((Map<String, String>) previousReactor.getValue("logicalToValue"));
		runner.setResponse(previousReactor.getValue(nodeStr));
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		
		Map<String, Object> searchData = new HashMap<>(1);
		String source = (String)previousReactor.getValue("source");
		List searchDataValues = (List) previousReactor.getValue("searchData");
		if("engine".equals(source)) {
			if(searchDataValues.get(0) instanceof Map) {
				searchData.put("list", searchDataValues);
			} else {
				searchData.put("list", convertListToListMaps(searchDataValues));
			}
		} else {
			searchData.put("list", searchDataValues);
		}
		runner.setReturnData(searchData);
		this.frame = (IDataMaker) previousReactor.getValue(PKQLEnum.G);
    }
    
	@Override
	public void caseAImportData(AImportData node) {
		inAImportData(node);
		// NEED TO PROCESS THE TABLE JOINS FIRST
		// THIS IS BECAUSE THE API REACTOR NEEDS THE
		// TABLE JOINS TO OPTIMIZE THE QUERY BEING USED
		if (node.getJoins() != null) {
			node.getJoins().apply(this);
		}		
		// everything else takes the normal execution route
		if (node.getDataimporttoken() != null) {
			node.getDataimporttoken().apply(this);
		}
		if (node.getImport() != null) {
			node.getImport().apply(this);
		}
		if(node.getProperties() != null) {
            node.getProperties().apply(this);
        }
		outAImportData(node);
	}

	@Override
	public void inAImportData(AImportData node) {
		if (reactorNames.containsKey(PKQLEnum.IMPORT_DATA)) {
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
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.IMPORT_DATA, nodeImport, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLReactor.IMPORT_DATA.toString());
		runner.setNewColumns((Map<String, String>) previousReactor.getValue("logicalToValue"));
		runner.setResponse(previousReactor.getValue(nodeStr));
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		
		this.frame = (IDataMaker) previousReactor.getValue(PKQLEnum.G);
	}

	@Override
	public void inAApiBlock(AApiBlock node) {
		// now the curReactor will be some kind of ApiReactor based on
		// strategy pattern described below
		// strategy pattern uses the engine to determine the type
		// assumption if not predefined, it is an engine name that is
		// query-able via a IQueryInterpreter
		String engine = node.getEngineName().toString().trim();
		if (engine.equalsIgnoreCase("ImportIO") || engine.equalsIgnoreCase("AmazonProduct")) {
			// we have a web api
			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.WEB_API));
		} else if (engine.equalsIgnoreCase("csvFile")) {
			// we have a csv api
			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.CSV_API));
		} else if (engine.equalsIgnoreCase("excelFile")) {
			// we have a csv api
			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.EXCEL_API));
		}
//		else if (engine.equalsIgnoreCase("R")) {
//			// we have an R api to connect
//			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.R_API));
//		} 
		else if(engine.equalsIgnoreCase("frame")) {
			//we are querying the frame
			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.FRAME_API));
		} else if(this.reactorNames.get(PKQLEnum.API) == null) {
			// default is a query api
			this.reactorNames.put(PKQLEnum.API, this.reactorNames.get(PKQLEnum.QUERY_API));
		}
		// this is here because we are overriding the data.import order of
		// execution to process the joins
		// before we process the iterator
		// curReactor at this point is still an ImportDataReactor
		List tableJoins = null;
		if (curReactor.getValue(PKQLEnum.JOINS) != null) {
			tableJoins = (List) curReactor.getValue(PKQLEnum.JOINS);
		}
				
		// make the api type
		// set in the values
		initReactor(PKQLEnum.API);

		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.API, nodeStr);
		curReactor.put("ENGINE", engine);
		
		// add the table joins if present
		if(tableJoins != null) {
			curReactor.put(PKQLEnum.TABLE_JOINS, tableJoins);
		}
		
		// something to do with parameters... need to look into this at some
		// point...
		curReactor.put("INSIGHT", node.getInsight().toString());
		Map<String, Map<String, Object>> varMap = runner.getVarMap();
		Map<String, Map<String, Object>> varMapForReactor = new HashMap<String, Map<String, Object>>();
		// Grab any param data for the current engine so API reactor grabs
		// those values as needed
		for (String var : varMap.keySet()) {
			Map<String, Object> paramValues = varMap.get(var);
			if (engine.equals(paramValues.get(Constants.ENGINE))) {
				varMapForReactor.put(var, varMap.get(var));
			}
		}
		curReactor.put("VARMAP", varMapForReactor);
	}

	@Override
	public void outAApiBlock(AApiBlock node) {
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		try {
			Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.API, nodeStr, PKQLEnum.API); // I need to make this into a string
			if (curReactor != null && node.parent() != null
					&& (node.parent() instanceof AApiImportBlock || node.parent() instanceof AApiTerm)
					&& !node.getEngineName().toString().equalsIgnoreCase("ImportIO")) {
				String[] values2Sync = curReactor.getValues2Sync(PKQLEnum.API);
				if (values2Sync != null) {
					synchronizeValues(PKQLEnum.API, values2Sync, thisReactor);
				}
			}
		} catch(IllegalArgumentException ex) {
			ex.printStackTrace();
			// well, something messed up with the query
			// however, for saved insights, we probably want to return this error at this step
			// but still try to do the other steps
			
		}

		runner.setResponse(thisReactor.getValue("RESPONSE"));
		runner.setStatus((STATUS) thisReactor.getValue("STATUS"));
	}

	@Override
	public void inARawApiBlock(ARawApiBlock node)
    {
		// make a raw api import block which will
		// take in a user query and execute it to construct the frame
		String engineName = node.getEngineName().getText().trim();
		if(engineName.equalsIgnoreCase("frame")) {
			//we are querying the frame
			this.reactorNames.put(PKQLEnum.RAW_API, this.reactorNames.get(PKQLEnum.FRAME_RAW_API));
			initReactor(PKQLEnum.RAW_API);
		} else if (engineName.equalsIgnoreCase("remoteConnection")) {
			this.reactorNames.put(PKQLEnum.RAW_API, this.reactorNames.get(PKQLEnum.REMOTE_RDBMS_QUERY_API));
			initReactor(PKQLEnum.RAW_API);
		} else {
			this.reactorNames.put(PKQLEnum.RAW_API, "prerna.sablecc.RawQueryApiReactor");
			initReactor(PKQLEnum.RAW_API);
		}
		// set the engine
		// even if frame, we just use the same key...
		curReactor.put(RawQueryApiReactor.ENGINE_KEY, engineName);
		String query = node.getQueryblock().getText().trim().replace("<query>", "").replace("</query>", "");
		curReactor.put(RawQueryApiReactor.QUERY_KEY, query);

		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.RAW_API, nodeStr);
    }
	
	@Override
	public void outARawApiBlock(ARawApiBlock node)
    {
		IScriptReactor thisReactor = curReactor;
		String nodeStr = node.toString().trim();

		// make a raw api import block which will
		// take in a user query and execute it to construct the frame
		deinitReactor(PKQLEnum.RAW_API, node.toString().trim(), PKQLEnum.RAW_API);
		
		// set the iterator in the parent
		curReactor.put(PKQLEnum.RAW_API, thisReactor.getValue(nodeStr));
		
		// merge values with parent
		String[] values2Sync = curReactor.getValues2Sync(PKQLEnum.RAW_API);
		if (values2Sync != null) {
			synchronizeValues(PKQLEnum.RAW_API, values2Sync, thisReactor);
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
		if (reactorNames.containsKey(PKQLEnum.WHERE)) {
			initReactor(PKQLEnum.WHERE);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.WHERE, nodeStr.trim());
			curReactor.put(PKQLEnum.COMPARATOR, (node.getEqualOrCompare() + "").trim());
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

		// TODO: why do we define it as a RelDef and then every reactor to use
		// it calls it Joins...
		curReactor.set(PKQLEnum.JOINS, relHash);
		curReactor.addReplacer(node.toString(), relHash);
	}

	@Override
	public void inAPastedData(APastedData node) {
		if (reactorNames.containsKey(PKQLEnum.PASTED_DATA)) {
			initReactor(PKQLEnum.PASTED_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.PASTED_DATA, nodeStr.trim());
			String word = ((APastedDataBlock) node.parent()).getDelimitier().toString().trim();
			// remove the quotes via substring
			curReactor.put(PastedDataReactor.DELIMITER, (word.substring(1, word.length() - 1))); 
		}
	}

	@Override
	public void outAPastedData(APastedData node) {
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		deinitReactor(PKQLEnum.PASTED_DATA, thisNode, PKQLEnum.PASTED_DATA);
		String[] values2Sync = curReactor.getValues2Sync(PKQLEnum.PASTED_DATA);
		synchronizeValues(PKQLEnum.PASTED_DATA, values2Sync, thisReactor);
	}

	@Override
	public void inAOpenData(AOpenData node) {
		initReactor(PKQLEnum.OPEN_DATA);
	}

	@Override
	public void outAOpenData(AOpenData node) {
		String nodeOpen = node.getDataopentoken().toString().trim();
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeOpen);

		String engine = node.getEngine().toString().trim();
		String id = node.getEngineId().toString().trim();
		curReactor.put("DATA_OPEN_ENGINE", engine);
		curReactor.put("DATA_OPEN_ID", id);

		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.OPEN_DATA, nodeOpen, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.OPEN_DATA);

		Map<String, Object> webData = (Map<String, Object>) previousReactor.getValue("webData");
		this.runner.addNewInsight(webData);

//		if (curReactor.getValue("G") instanceof Dashboard) {
//			Dashboard dash = (Dashboard) curReactor.getValue("G");
//			dash.setInsightOutput((String) webData.get("insightID"), webData);
//		}
		curReactor.set(PKQLEnum.OPEN_DATA, previousReactor.getValue(PKQLEnum.OPEN_DATA));
	}
	
	@Override
	public void inAOutputData(AOutputData node) {
		initReactor(PKQLEnum.OUTPUT_DATA);
	}

	@Override
	public void outAOutputData(AOutputData node) {
		String nodeOpen = node.getDataoutputtoken().toString().trim();
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeOpen);

		String engine = node.getEngine().toString().trim();
		String id = node.getEngineId().toString().trim();
		curReactor.put("DATA_OPEN_ENGINE", engine);
		curReactor.put("DATA_OPEN_ID", id);
		curReactor.put("INSIGHT_ID", this.runner.getInsightId());
		
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.OUTPUT_DATA, nodeOpen, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.OUTPUT_DATA);

		Map<String, Object> webData = (Map<String, Object>) previousReactor.getValue("webData");
		this.runner.setReturnData(webData);
		// ugh... in case there are places where there are other panel.viz's
		// we need to make sure we set the information that is returned into this runner
		// because the OutputDataReactor used a different runner to get the webData information
		this.runner.setNewColumns((Map<String, String>) webData.get("newColumns"));
		this.runner.setReturnData(webData.get("pkqlData"));
		this.runner.setFeData((Map<String, Map<String, Object>>) webData.get("feData"));
		
		
		IDataMaker dm = (IDataMaker) curReactor.getValue("G");
		if(dm != null) {
			this.frame = dm;
			// need to update the script reactors
			Map<String, String> frameReactorNames = frame.getScriptReactors();
			this.reactorNames.putAll(frameReactorNames);		
		}
//		if (curReactor.getValue("G") instanceof Dashboard) {
//			Dashboard dash = (Dashboard) curReactor.getValue("G");
//			dash.setInsightOutput((String) webData.get("insightID"), webData);
//		}
		curReactor.set(PKQLEnum.OUTPUT_DATA, previousReactor.getValue(PKQLEnum.OUTPUT_DATA));
	}
	
	@Override
	public void inAClearData(AClearData node) {
		if (reactorNames.containsKey(PKQLEnum.CLEAR_DATA)) {
			initReactor(PKQLEnum.CLEAR_DATA);
		}
	}
	
	@Override
	public void outAClearData(AClearData node) {
		if (reactorNames.containsKey(PKQLEnum.CLEAR_DATA)) {
			IScriptReactor prevReactor = curReactor;
			deinitReactor(PKQLEnum.CLEAR_DATA, node.toString().trim(), PKQLEnum.CLEAR_DATA);
			IDataMaker dm = (IDataMaker) prevReactor.getValue("G");
			if(dm != null) {
				this.frame = dm;
			}
			this.runner.setDataClear(true);
		}
	}

	@Override
	public void inAClearCache(AClearCache node) {
		initReactor(PKQLEnum.CLEAR_CACHE);
	}
	
	@Override
	public void outAClearCache(AClearCache node) {
        
		PWordOrNum engineName_Node = node.getEngine();
        if(engineName_Node != null) {
        	String engineName = removeQuotes(engineName_Node.toString().trim());
        	curReactor.put("ENGINE_NAME", engineName);
        }
        
        ACsvGroup engineID_Node = (ACsvGroup)node.getEngineId();
        if(engineID_Node != null) {
        	String engineID = removeQuotes(engineID_Node.getCsv().toString().trim());
        	curReactor.put("ENGINE_ID", engineID);
        }
        
        deinitReactor(PKQLEnum.CLEAR_CACHE, node.toString().trim(), PKQLEnum.CLEAR_CACHE);
    }
	
	@Override
	public void inAUseCache(AUseCache node) {
		initReactor(PKQLEnum.USE_CACHE);
	}
	
	@Override
	public void outAUseCache(AUseCache node) {
		String cacheSetting = node.getCacheSetting().toString();
		curReactor.set("CACHE_SETTING", cacheSetting);
		deinitReactor(PKQLEnum.USE_CACHE, node.toString(), PKQLEnum.USE_CACHE);
	}

	@Override
	public void inARemoveData(ARemoveData node) {
		if (reactorNames.containsKey(PKQLEnum.REMOVE_DATA)) {
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
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.REMOVE_DATA, nodeStr, (node + "").trim());
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.REMOVE_DATA);
		runner.setResponse(previousReactor.getValue(node.toString().trim()));//
		runner.setStatus((STATUS) previousReactor.getValue("STATUS"));
	}

	// **************************************** START PANEL OPERATIONS
	// **********************************************//

	@Override
	public void inAPanelopScript(APanelopScript node) {
		String nodeString = node.getPanelop().toString().trim();
		
		String id = "0";
		if (nodeString.startsWith("panel[")) {
			nodeString = nodeString.substring(nodeString.indexOf("[") + 1);
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
		Map<String, Object> chartDataObj = new HashMap<String, Object>();

		deinitReactor(PKQLEnum.VIZ, "", "");
		// this will add in the necessary information around the expressions
		// that are calculated temporally to return to the FE
		chartDataObj.put("dataTableKeys", curReactor.getValue("VizTableKeys"));
		chartDataObj.put("dataTableValues", curReactor.getValue("VizTableValues"));

		/*
		 * This will hold the UI options that are passed in that do 
		 * not go through any BE processing
		 * We just send this to the FE.
		 */
		String layout = node.getLayout().toString().trim();
		chartDataObj.put("layout", layout);
		
		Map<Object, Object> optionsMap = (Map<Object, Object>) curReactor.getValue(PKQLEnum.MAP_OBJ);
		if (optionsMap != null) {
			// going to separate out the data table options
			// vs other tools
			Map<String, Object> vizTableOptionsMap = new HashMap<String, Object>();
			vizTableOptionsMap.put("limit", optionsMap.remove("limit"));
			vizTableOptionsMap.put("offset", optionsMap.remove("offset"));
			vizTableOptionsMap.put("sortVar", optionsMap.remove("sortVar"));
			vizTableOptionsMap.put("sortDir", optionsMap.remove("sortDir"));
			
			if(!vizTableOptionsMap.isEmpty()) {
				chartDataObj.put("dataTableOptions", vizTableOptionsMap);
			}
			if(!optionsMap.isEmpty()) {
				chartDataObj.put("uiOptions", optionsMap.toString().trim());
			}
		}
		// set the FE data to create the appropriate panel for the panel state
		runner.addFeData("chartData", chartDataObj, true);
		// set the console response
		runner.setResponse("Successfully set layout to " + layout + " with alignment " + node.getDatatablealign());
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);

		
	}

	@Override
	public void inAPanelComment(APanelComment node) {
		System.out.println("in a viz comment");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
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
		String text = node.getText().toString().trim();
		commentMap.put("text", text);
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
		curReactor.put(PKQLEnum.VIZ, node.toString());
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
		String text = node.getText().toString().trim();
		commentMap.put("text", text); // remove
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
		curReactor.put(PKQLEnum.VIZ, node.toString());
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
	public void inAPanelClone(APanelClone node) {
		System.out.println("in a panel clone");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
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
	public void inAPanelLookAndFeel(APanelLookAndFeel node) {
		System.out.println("in a panel laf");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
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
	public void inAPanelTools(APanelTools node) {
		System.out.println("in a panel tools");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
	}

	@Override
	public void outAPanelTools(APanelTools node) {
		System.out.println("out a panel tools");
		
		
		List tools = (List) runner.getFeData("tools");
		if (tools == null) {
			tools = new ArrayList();
		}
		Map thisTools = new Gson().fromJson(node.getMap().toString(), HashMap.class);
		curReactor.put("tools", thisTools);
		PWordOrNum state = node.getState();
		String stateName;
		if(state == null) {
			stateName = "defaultState";
		} else {
			stateName = state.toString().trim();
			stateName = removeQuotes(stateName);
		}
		Map newToolMap = new HashMap();
		newToolMap.put(stateName, thisTools);
		
		tools.add(newToolMap);
				
//		Map tools = (Map) runner.getFeData("tools");
//		if (tools == null) {
//			tools = new HashMap();
//		}
//		tools.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		runner.addFeData("tools", tools, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set tools");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void inAPanelConfig(APanelConfig node) {
		System.out.println("in a panel config");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
	}

	@Override
	public void outAPanelConfig(APanelConfig node) {
		System.out.println("out a panel config");
//		String config = (Map) runner.getFeData("config");
//		if (config == null) {
//			config = new HashMap();
//		}
//		config.putAll(new Gson().fromJson(node.getMap().toString(), HashMap.class));
		
		String json = node.getJson().toString();
		json = json.replace("<json>", "");
		json = json.replace("</json>", "");
		
		curReactor.put("configMap", json);
		runner.addFeData("config", json, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set config");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}
	
	@Override
	public void inAPanelHandle(APanelHandle node) {
		System.out.println("in a panel handle");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
	}

	@Override
	public void outAPanelHandle(APanelHandle node) {
		System.out.println("out a panel handle");
		List handleKeys = (List)curReactor.getValue(PKQLEnum.ROW_CSV);

		runner.addFeData("handle", handleKeys, true);
		deinitReactor(PKQLEnum.VIZ, "", "");
		runner.setResponse("Successfully set handle keys");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}
	
	@Override
	public void outADataModel(ADataModel node) {
		String json = node.getJson().toString();
		json = json.replace("<json>", "");
		json = json.replace("</json>", "");
		runner.setReturnData(json);
		runner.setResponse("Successfully parsed default widget");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
		storePkqlMetadata.add(new GenericMetaData("Set Data Model"));
//		try {
//			Map object = new Gson().fromJson(json, HashMap.class);
//			Map<String, Object> returnData = new HashMap<>();
//			returnData.put("json", object);
//			runner.setReturnData(object);
//			
//			runner.setResponse("Successfully parsed default widget");
//			runner.setStatus(PKQLRunner.STATUS.SUCCESS);
//		} catch (Exception e) {
//			runner.setResponse("Error parsing default widget");
//			runner.setStatus(PKQLRunner.STATUS.ERROR);
//		}		
	}

	@Override
	public void inAPanelClose(APanelClose node) {
		System.out.println("in a panel close");
		initReactor(PKQLEnum.VIZ);
		curReactor.put(PKQLEnum.VIZ, node.toString());
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

	// **************************************** END PANEL OPERATIONS
	// **********************************************//

	// **************************************** START JOIN OPERATIONS
	// **********************************************//
	@Override
	public void outAOpenDataJoinParam(AOpenDataJoinParam node) {
		String insightID = (String) curReactor.getValue(PKQLEnum.OPEN_DATA);
		curReactor.set(PKQLEnum.JOIN_PARAM, insightID);
	}

	@Override
	public void outAInsightidJoinParam(AInsightidJoinParam node) {
		String insightID = node.getWord().toString().trim();
		String cleanedInsightID = insightID.substring(1, insightID.length() - 1);
		curReactor.set(PKQLEnum.JOIN_PARAM, cleanedInsightID);
	}

	@Override
	public void outAVariableJoinParam(AVariableJoinParam node) {
		String varName = ((AVarDef) node.getVarDef()).getValname().toString().trim();
		String insightID = (String) runner.getVariableValue(varName);
		curReactor.set(PKQLEnum.JOIN_PARAM, insightID);
	}

	@Override
	public void inADashboardJoin(ADashboardJoin node) {
		System.out.println("Have dashboard join as " + node);
		if (reactorNames.containsKey(PKQLEnum.DASHBOARD_JOIN)) {
			initReactor(PKQLEnum.DASHBOARD_JOIN);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.DASHBOARD_JOIN, nodeStr.trim());
			// rel type is a token, not a production, so no in/out to add it to
			// the reactor
			// need to add it here
			if (node.getRel() != null)
				curReactor.put(PKQLEnum.REL_TYPE, node.getRel().toString().trim());
		}
	}

	@Override
	public void outADashboardJoin(ADashboardJoin node) {
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		curReactor.put("G", this.frame);
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DASHBOARD_JOIN, nodeStr,
				PKQLEnum.DASHBOARD_JOIN);

		Map dashboardData = (Map) runner.getDashboardData();
		if (dashboardData == null) {
			runner.setDashBoardData((Map)thisReactor.getValue("DashboardData"));
		} else {
			Map<String, List> newDashboardData = (Map<String, List>) thisReactor.getValue("DashboardData");
			if (dashboardData.containsKey("joinedInsights")) {
				List list = (List) dashboardData.get("joinedInsights");
				list.addAll(newDashboardData.get("joinedInsights"));
			} else {
				List list = (List) newDashboardData.get("joinedInsights");
				dashboardData.put("joinedInsights", list);
			}
			runner.setDashBoardData(dashboardData);
		}
	}
	
	@Override
	public void inADashboardUnjoin(ADashboardUnjoin node) {
		System.out.println("Have dashboard join as " + node);
		if (reactorNames.containsKey(PKQLEnum.DASHBOARD_UNJOIN)) {
			initReactor(PKQLEnum.DASHBOARD_UNJOIN);
		}
	}
	
	@Override
	public void outADashboardUnjoin(ADashboardUnjoin node) {
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		curReactor.put("G", this.frame);
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DASHBOARD_UNJOIN, nodeStr, PKQLEnum.DASHBOARD_UNJOIN);

		Map dashboardData = (Map) runner.getDashboardData();
		if (dashboardData == null) {
			runner.setDashBoardData((Map)thisReactor.getValue("DashboardData"));
		} else {
			Map<String, List> newDashboardData = (Map<String, List>) thisReactor.getValue("DashboardData");
			if (dashboardData.containsKey("unJoinedInsights")) {
				List list = (List) dashboardData.get("unJoinedInsights");
				list.addAll(newDashboardData.get("unJoinedInsights"));
			} else {
				List list = (List) newDashboardData.get("unJoinedInsights");
				dashboardData.put("unJoinedInsights", list);
			}
			runner.setDashBoardData(dashboardData);
		}
	}

	public void inADashboardAdd(ADashboardAdd node) {
		System.out.println("Have dashboard join as " + node);
		
		//TOOD: this is really crappy!!!!
		//unless I instantiate a frame on Insight creation... which I don't want to do
		//there is nothing where the FE creates a dashboard :/
//		if (!reactorNames.containsKey(PKQLEnum.DASHBOARD_ADD)) {
//			this.frame = new Dashboard();
//			this.reactorNames.putAll(this.frame.getScriptReactors());
//		}
		
		initReactor(PKQLEnum.DASHBOARD_ADD);
		String nodeStr = node + "";
		curReactor.put(PKQLEnum.DASHBOARD_ADD, nodeStr.trim());
	}

	public void outADashboardAdd(ADashboardAdd node) {
		String nodeStr = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		curReactor.put("G", this.frame);
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DASHBOARD_ADD, nodeStr, PKQLEnum.DASHBOARD_ADD);

		Map dashboardData = (Map) runner.getDashboardData();
		if (dashboardData == null) {
			runner.setDashBoardData((Map)thisReactor.getValue("DashboardData"));
		} else {
			Map<String, List> newDashboardData = (Map<String, List>) thisReactor.getValue("DashboardData");
			if (dashboardData.containsKey("addedInsights")) {
				List list = (List) dashboardData.get("addedInsights");
				list.addAll(newDashboardData.get("addedInsights"));
			} else {
				List list = (List) newDashboardData.get("addedInsights");
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
		// initReactor(PKQLEnum.VIZ);
	}

	public void outADashboardConfig(ADashboardConfig node) {
//		System.out.println("out a dashboard config");
//
//		Dashboard dm = (Dashboard) this.frame;
//
//		String json = node.getJson().toString();
//		json = json.replace("<json>", "");
//		json = json.replace("</json>", "");
//		
//		dm.setConfig(json);
//		runner.addToDashBoardData("config", json);
//		
////		try {
////			Map object = new Gson().fromJson(json, HashMap.class);
////			dm.setConfig(object);
//////			runner.setReturnData(object);
////			runner.addToDashBoardData("config", object);
////		} catch (Exception e) {
////			dm.setConfig(json);
//////			runner.setReturnData(json);
////			runner.addToDashBoardData("config", json);
////		}
//
//		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	// **************************************** END JOIN OPERATIONS
	// **********************************************//

	// **************************************** START VAR OPERATIONS
	// **********************************************//

	public void outAExprInputOrExpr(AExprInputOrExpr node) {
		// need to get expr and set it to var_param
		String value = node.getExpr().toString().trim();
		curReactor.put(PKQLEnum.VAR_PARAM, value);
	}

	public void outAInputInputOrExpr(AInputInputOrExpr node) {
	}

	public void outAOpenDataInputOrExpr(AOpenDataInputOrExpr node) {
		String insightID = ((List) curReactor.getValue(PKQLEnum.OPEN_DATA)).get(0).toString();
		curReactor.put(PKQLEnum.VAR_PARAM, insightID);
	}

	@Override
	public void inAVarop(AVarop node) {
		if (reactorNames.containsKey(PKQLReactor.VAR.toString())) {
			String varName = (node.getVarDef() + "").trim();
			String expr = (node.getInputOrExpr() + "").trim();

			initReactor(PKQLReactor.VAR.toString());
			curReactor.put(PKQLReactor.VAR.toString(), varName);
			curReactor.put(PKQLEnum.EXPR_TERM, expr); // don't need once all
														// algorithms have been
														// refactored into
														// Reactors
		}
	}

	// this is only used for setting a var (aka v:test = 'true')
	// AVarTerm will be used in expressions (aka c:Budget + v:test)
	@Override
	public void outAVarop(AVarop node) {
		if (reactorNames.containsKey(PKQLReactor.VAR.toString())) {
			String nodeStr = PKQLReactor.VAR.toString();

			boolean updatingExistingVar = false;
			Map<String, Object> thisReactorHash = deinitReactor(PKQLReactor.VAR.toString(), nodeStr, nodeStr);
			IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLReactor.VAR.toString());
			String varName = previousReactor.getValue(PKQLReactor.VAR.toString()) + "";
			Object inputNeeded = previousReactor.getValue(PKQLEnum.INPUT);
			if (inputNeeded == null) // if no input needed for this var, set it
										// and we are good
			{
				String varParam = curReactor.getValue(PKQLEnum.VAR_PARAM) + "";
				runner.unassignedVars.remove(varName);
				if (runner.getVariableData(varName) != null) {
					updatingExistingVar = true;
				}
				runner.setVariableValue(varName, varParam.replaceAll("^\'|\"|\'$", "").trim());
				runner.setResponse("Set variable " + varName + " to " + varParam);
				runner.setStatus(STATUS.SUCCESS);
				curReactor.put(PKQLReactor.VAR.toString(), varName);
			} else {
				runner.unassignedVars.add(varName);
				if (runner.getVariableData(varName) == null) {
					runner.addNewVariable(varName, previousReactor.getValue(Constants.ENGINE).toString(),
							((Vector) previousReactor.getValue(Constants.TYPE)).get(0).toString());
				}
				runner.setResponse("Need input on variable " + varName);
				Map<String, Object> paramMap = new HashMap<String, Object>();
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

	// **************************************** END VAR OPERATIONS
	// **********************************************//

	@Override
	public void caseADataFrame(ADataFrame node) {
		inADataFrame(node);
		outADataFrame(node);
	}

	@Override
	public void inADataFrame(ADataFrame node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME)) {
			// get the appropriate reactor
			initReactor(PKQLEnum.DATA_FRAME);
			String word = ((AAlphaWordOrNum) node.getBuilder()).getWord().getText().trim();
			String cleanWord = word.substring(1, word.length() - 1);// remove
																	// the
																	// quotes
			curReactor.put(PKQLEnum.DATA_FRAME, node.toString());
			curReactor.put(DataFrameReactor.DATA_FRAME_TYPE, cleanWord);
			curReactor.put("G", frame);
		}
	}

	@Override
	public void outADataFrame(ADataFrame node) {
		// then deinit
		// grab the new G from the reactor
		// set into this class

		deinitReactor(PKQLEnum.DATA_FRAME, node.getBuilder().toString().trim(), node.toString().trim());
		this.frame = (IDataMaker) curReactor.getValue(PKQLEnum.G);
		
		// we need to set the connection in the PKQLRunner so we can call it via Java Reactor
//		if(this.frame instanceof RDataTable) {
//			this.runner.setVariableValue(BaseJavaReactor.R_CONN, ((RDataTable) this.frame).getConnection());
//		}
		
		// set the script reactors for this new frame
		this.reactorNames = frame.getScriptReactors();
		this.reactorNames.putAll(getDefaultReactors());
	}

	@Override
	public void inADataFrameHeader(ADataFrameHeader node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME_HEADER)) {
			initReactor(PKQLEnum.DATA_FRAME_HEADER);
			curReactor.put(PKQLEnum.DATA_FRAME_HEADER, node.toString());
		}
	}

	@Override
	public void outADataFrameHeader(ADataFrameHeader node) {
		IScriptReactor thisReactor = curReactor;
		deinitReactor(PKQLEnum.DATA_FRAME_HEADER, node.toString().trim(), node.toString().trim());
		this.runner.setResponse(thisReactor.getValue("tableHeaders"));
		
		Map retData = new HashMap();
		retData.put("list", thisReactor.getValue("tableHeaders"));
		this.runner.setReturnData(retData);
	}

	@Override
	public void inADataFrameDuplicatesColop(ADataFrameDuplicatesColop node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME_DUPLICATES)) {
			initReactor(PKQLEnum.DATA_FRAME_DUPLICATES);
			curReactor.put(PKQLEnum.DATA_FRAME_DUPLICATES, node.toString());
		}
	}

	@Override
	public void outADataFrameDuplicatesColop(ADataFrameDuplicatesColop node) {
		IScriptReactor thisReactor = curReactor;
		deinitReactor(PKQLEnum.DATA_FRAME_DUPLICATES, node.toString().trim(), node.toString().trim());
		this.runner.setResponse(thisReactor.getValue("hasDuplicates"));
		this.runner.setReturnData(thisReactor.getValue("hasDuplicates"));
	}
	
	@Override
    public void inADataFrameChangeTypes(ADataFrameChangeTypes node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME_CHANGE_TYPE)) {
			initReactor(PKQLEnum.DATA_FRAME_CHANGE_TYPE);
			curReactor.put(PKQLEnum.DATA_FRAME_CHANGE_TYPE, node.toString());
		}
    }

	@Override
    public void outADataFrameChangeTypes(ADataFrameChangeTypes node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME_CHANGE_TYPE)) {
			deinitReactor(PKQLEnum.DATA_FRAME_CHANGE_TYPE, node.toString().trim(), node.toString().trim());
		}
    }
	
	@Override
	public void inADataFrameSetEdgeHash(ADataFrameSetEdgeHash node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME_SET_EDGE_HASH)) {
			initReactor(PKQLEnum.DATA_FRAME_SET_EDGE_HASH);
			curReactor.put(PKQLEnum.DATA_FRAME_SET_EDGE_HASH, node.toString());
		}
	}
	
	@Override
	public void outADataFrameSetEdgeHash(ADataFrameSetEdgeHash node) {
		if (reactorNames.containsKey(PKQLEnum.DATA_FRAME_SET_EDGE_HASH)) {
			deinitReactor(PKQLEnum.DATA_FRAME_SET_EDGE_HASH, node.toString().trim(), node.toString().trim());
		}
	}
	
	@Override
	public void inATermExpr(ATermExpr node) {
		if (reactorNames.containsKey(PKQLEnum.EXPR_TERM)) {
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
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.EXPR_TERM, node.getTerm().toString().trim(),
				node.toString().trim());

		if (thisReactorHash.get(PKQLEnum.EXPR_TERM) instanceof ExprReactor) {
			ExprReactor thisReactor = (ExprReactor) thisReactorHash.get(PKQLEnum.EXPR_TERM);
			String expr = (String) thisReactor.getValue(PKQLEnum.EXPR_TERM);
			Object objVal = thisReactor.getValue(PKQLEnum.COL_DEF);
			if (objVal != null) {
				if (objVal instanceof Collection) {
					Collection<? extends Object> values = (Collection<? extends Object>) objVal;
					for (Object obj : values) {
						curReactor.set(PKQLEnum.COL_DEF, obj);
					}
				} else {
					curReactor.set(PKQLEnum.COL_DEF, objVal);
				}
			}

			// this commented out code is part of the shift to getting derived
			// calculation info
			// objVal = thisReactor.getValue(PKQLEnum.COL_CSV);
			// if(objVal != null) {
			// curReactor.put(PKQLEnum.COL_CSV, objVal);
			// }
			// objVal = thisReactor.getValue(PKQLEnum.PROC_NAME);
			// if(objVal != null) {
			// curReactor.put(PKQLEnum.PROC_NAME, objVal);
			// }
			curReactor.addReplacer(expr, thisReactor.getValue(expr));
			// runner.setResponse(thisReactor.getValue(expr));
			// runner.setStatus((String)thisReactor.getValue("STATUS"));
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
			result = (Double) (leftObj) + (Double) (rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName.trim());
			curReactor.removeReplacer(rightKeyName.trim());
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			if(curReactor.getValue("G") instanceof H2Frame) {
				H2Frame frame = (H2Frame) curReactor.getValue("G");
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getPlus().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			} else if(curReactor.getValue("G") instanceof RDataTable) {
				RDataTable frame = (RDataTable) curReactor.getValue("G");
				RExpressionBuilder builder = ExpressionGenerator.rGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getPlus().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			}
		}
	}

	@Override
	public void outAMinusExpr(AMinusExpr node) {
		String leftKeyName = node.getLeft().toString().trim();
		String rightKeyName = node.getRight().toString().trim();

		Object leftObj = curReactor.getValue(leftKeyName);
		Object rightObj = curReactor.getValue(rightKeyName);
		Object result = null;
		if (rightObj instanceof Double && leftObj instanceof Double) {
			result = (Double) (leftObj) - (Double) (rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			if(curReactor.getValue("G") instanceof H2Frame) {
				H2Frame frame = (H2Frame) curReactor.getValue("G");
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getMinus().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			} else if(curReactor.getValue("G") instanceof RDataTable) {
				RDataTable frame = (RDataTable) curReactor.getValue("G");
				RExpressionBuilder builder = ExpressionGenerator.rGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getMinus().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			}
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
			result = (Double) (leftObj) * (Double) (rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		}
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			if(curReactor.getValue("G") instanceof H2Frame) {
				H2Frame frame = (H2Frame) curReactor.getValue("G");
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getMult().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			} else if(curReactor.getValue("G") instanceof RDataTable) {
				RDataTable frame = (RDataTable) curReactor.getValue("G");
				RExpressionBuilder builder = ExpressionGenerator.rGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getMult().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			}
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
			result = (Double) (leftObj) / (Double) (rightObj);
			// remove the left and right key
			curReactor.addReplacer(node.toString().trim(), result);
			curReactor.removeReplacer(leftKeyName);
			curReactor.removeReplacer(rightKeyName);
		} 
		else  
		{
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			//TODO: need to make this generic and get the correct type of expression it
			if(curReactor.getValue("G") instanceof H2Frame) {
				H2Frame frame = (H2Frame) curReactor.getValue("G");
				SqlExpressionBuilder builder = ExpressionGenerator.sqlGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getDiv().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			} else if(curReactor.getValue("G") instanceof RDataTable) {
				RDataTable frame = (RDataTable) curReactor.getValue("G");
				RExpressionBuilder builder = ExpressionGenerator.rGenerateSimpleMathExpressions(frame, leftObj, rightObj, node.getDiv().toString().trim());
				curReactor.addReplacer(node.toString().trim(), builder);
				curReactor.removeReplacer(leftKeyName);
				curReactor.removeReplacer(rightKeyName);
				curReactor.put(node.toString().trim(), builder);
				this.runner.setResponse(builder);
			}
		}

	}
	

	@Override
	public void outAModExpr(AModExpr node) {

	}

//	@Override
//	public void inAFlexSelectorRow(AFlexSelectorRow node) {
//		// TODO: really need to build this out...
//		if (node.getSelectorTerm() != null) {
//			curReactor.set("VIZ_SELECTOR", node.getSelectorTerm() + "");
//		}
//	}
	
	@Override
    public void caseASelectorTerm(ASelectorTerm node)
    {
        inASelectorTerm(node);
        if(node.getVizType() != null)
        {
            curReactor.set("VIZ_TYPE", node.getVizType().toString().trim());
        } else 
        {
            curReactor.set("VIZ_TYPE", "NOT_DEFINED");
        }
        if(node.getTerm() != null)
        {
            node.getTerm().apply(this);
            curReactor.set("VIZ_SELECTOR", curReactor.getValue(node.getTerm().toString().trim()));
            curReactor.set("VIZ_FORMULA", node.getTerm().toString().trim());
        }
        outASelectorTerm(node);
    }

	public void inAColTerm(AColTerm node) {
		
	}

	@Override
	public void inAAddColumn(AAddColumn node) {
		if (reactorNames.containsKey(PKQLEnum.COL_ADD)) {
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
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.COL_ADD, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.COL_ADD.toString());
		runner.setStatus((STATUS) previousReactor.getValue("STATUS"));
		runner.setResponse((String) previousReactor.getValue("RESPONSE"));
	}

	@Override
	public void inAFilterColumn(AFilterColumn node) {
		if (reactorNames.containsKey(PKQLEnum.FILTER_DATA)) {
			initReactor(PKQLEnum.FILTER_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.FILTER_DATA, nodeStr.trim());
		}
	}

	//
	@Override
	public void outAFilterColumn(AFilterColumn node) {
		String nodeExpr = node.getWhere().toString().trim();
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.FILTER_DATA, nodeExpr,
				node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.FILTER_DATA.toString());
		runner.setStatus((STATUS) previousReactor.getValue("STATUS"));
		runner.setResponse((String) previousReactor.getValue("FILTER_RESPONSE"));
	}

	@Override
	public void inAUnfilterColumn(AUnfilterColumn node) {
		if (reactorNames.containsKey(PKQLEnum.UNFILTER_DATA)) {
			initReactor(PKQLEnum.UNFILTER_DATA);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.UNFILTER_DATA, nodeStr.trim());
		}
	}

	@Override
	public void outAUnfilterColumn(AUnfilterColumn node) {
		String nodeExpr = node.getColDef().toString().trim();
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.UNFILTER_DATA, nodeExpr,
				node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.UNFILTER_DATA.toString());
		runner.setStatus((STATUS) previousReactor.getValue("STATUS"));
		runner.setResponse("Unfiltered Column: " + (String) previousReactor.getValue("FILTER_COLUMN"));
	}
	
	@Override
	public void inAFiltermodelColop(AFiltermodelColop node) {
		if (reactorNames.containsKey(PKQLEnum.COL_FILTER_MODEL)) {
			initReactor(PKQLEnum.COL_FILTER_MODEL);
			curReactor.put(PKQLEnum.COL_FILTER_MODEL, node.toString());
		}	}

	@Override
	public void outAFiltermodelColop(AFiltermodelColop node) {
		IScriptReactor thisReactor = curReactor;
		
		PWordOrNum filter = ((AFilterModel) node.getFilterModel()).getWordOrNum();
		if(filter != null) {
		curReactor.put("filterWord", filter.toString());
		}
		deinitReactor(PKQLEnum.COL_FILTER_MODEL, node.toString().trim(), node.toString().trim());
		this.runner.setResponse(thisReactor.getValue("filterRS"));
		this.runner.setReturnData(thisReactor.getValue("filterRS"));		
	}

	@Override
	public void inASplitColumn(ASplitColumn node) {

		if (reactorNames.containsKey(PKQLEnum.COL_SPLIT)) {
			initReactor(PKQLEnum.COL_SPLIT);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.COL_SPLIT, nodeStr.trim());
		}
	}

	@Override
	public void outASplitColumn(ASplitColumn node) {

		String nodeExpr = node.getColsplit().toString().trim();
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.COL_SPLIT, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLEnum.COL_SPLIT.toString());
		// runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		// runner.setResponse("SplitColumn: " +
		// (String)previousReactor.getValue("FILTER_COLUMN"));
	}
	
	@Override
    public void inARenameColumn(ARenameColumn node)
    {
		if(reactorNames.containsKey(PKQLEnum.COL_RENAME)) {
			initReactor(PKQLEnum.COL_RENAME);
			String nodeStr = node + "";
			curReactor.put(PKQLEnum.COL_RENAME, nodeStr.trim());
		}	
    }

	@Override
    public void outARenameColumn(ARenameColumn node)
    {
		String nodeExpr = node.toString().trim();
		curReactor.put(PKQLEnum.EXPR_TERM, nodeExpr);
		Hashtable <String, Object> thisReactorHash = deinitReactor(PKQLEnum.COL_RENAME, nodeExpr, node.toString().trim());
		IScriptReactor previousReactor = (IScriptReactor)thisReactorHash.get(PKQLEnum.COL_RENAME.toString());
		runner.setStatus((STATUS)previousReactor.getValue("STATUS"));
		runner.setResponse((String)previousReactor.getValue("RESPONSE"));
    }

	@Override
	public void outAExprGroup(AExprGroup node) {

	}

	@Override
	public void outASetColumn(ASetColumn node) {
	}

	@Override
	public void inAUserInput(AUserInput node) {
		if (reactorNames.containsKey(PKQLReactor.INPUT.toString())) {
			// String options = node.getOptions().toString().trim();
			// String selections = node.getSelections().toString().trim();

			initReactor(PKQLReactor.INPUT.toString());
			curReactor.put(PKQLReactor.INPUT.toString(), node.toString());
			// curReactor.put(PKQLEnum.EXPR_TERM, expr); // don't need once all
			// algorithms have been refactored into Reactors
		}
	}

	@Override
	public void outAUserInput(AUserInput node) {
		if (reactorNames.containsKey(PKQLReactor.INPUT.toString())) {
			String nodeStr = PKQLReactor.INPUT.toString();
			Map<String, Object> thisReactorHash = deinitReactor(PKQLReactor.INPUT.toString(), nodeStr, nodeStr);
			IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLReactor.INPUT.toString());
			List options = (List) previousReactor.getValue("options");
			String selections = node.getSelections().toString().trim();
			// need to pause processing here........
			// a user defined value must be retrieved from the front end
			// this.unassignedVars.add(node.toString());
			curReactor.put("options", options);
			curReactor.put("selectAmount", selections);
			curReactor.put(Constants.ENGINE, previousReactor.getValue(PKQLEnum.API + "_" + Constants.ENGINE));
			curReactor.put(Constants.TYPE, previousReactor.getValue(PKQLEnum.API + "_" + PKQLEnum.COL_CSV));
			// node.replaceBy(null); // need to get out of finishing the
			// processing of this pkql..... how do i just return out of this bad
			// boy??
			// the plan is:
			// return out of this bad boy
			// allow for term = term which will be the way the front end sets it
			// in the config we look for that type of script and allow it if our
			// missing piece is there

			// FOR NOW ASSUMING USER INPUT IS JUST FOR VAR ASSIGNMENT
			// DONT NEED TO WORRY ABOUT RETURNING OUT OF ANYTHING
		}
	}

	@Override
	public void inAExprRow(AExprRow node) {
	}

	@Override
	public void outAExprRow(AExprRow node) {
		if(curReactor != null) {
			String previousExpr = node.getExpr().toString().trim();
			if(curReactor.getValue(previousExpr) != null) {
				curReactor.put(node.toString().trim(), curReactor.getValue(previousExpr));
			}
		}
	}

	@Override
	public void inAMathFunTerm(AMathFunTerm node) {
	}

	@Override
	public void inAMathFun(AMathFun node) {
		if (reactorNames.containsKey(PKQLEnum.MATH_FUN)) {
			String procedureName = node.getId().toString().trim();
			String nodeStr = node.getExpr().toString().trim();

			String procedureAlgo = "";
			if (reactorNames.containsKey(procedureName.toUpperCase())) {
				// the frame has defined a specific reactor for this procedure
				procedureAlgo = reactorNames.get(procedureName.toUpperCase());
			} else {
				procedureAlgo = "prerna.algorithm.impl." + procedureName + "Reactor";
			}

			reactorNames.put(PKQLReactor.MATH_FUN.toString(), procedureAlgo);
			String expr = (String) curReactor.getValue(PKQLEnum.EXPR_TERM);

			initReactor(PKQLReactor.MATH_FUN.toString());
			curReactor.put(PKQLEnum.G, frame);
			curReactor.put(PKQLEnum.MATH_FUN, nodeStr.trim());

			if (expr != null)
				curReactor.put(PKQLEnum.EXPR_TERM, expr);
		}
	}

	@Override
	// TODO: LOOK INTO THIS
	public void outAMathFun(AMathFun node) {
		String nodeStr = node.toString().trim();
		curReactor.put("FORMULA", nodeStr);
		// set the term for the math fun to operate on
		String expr = node.getExpr().toString().trim();
		curReactor.put("TERM", curReactor.getValue(expr));
		
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLReactor.MATH_FUN.toString(), expr, nodeStr);
		IScriptReactor previousReactor = (IScriptReactor) thisReactorHash.get(PKQLReactor.MATH_FUN.toString());
		
		curReactor.put(PKQLEnum.COL_DEF, previousReactor.getValue(PKQLEnum.COL_DEF));
		curReactor.put(PKQLEnum.COL_CSV, previousReactor.getValue(PKQLEnum.COL_CSV));
		curReactor.addReplacer(nodeStr, previousReactor.getValue(expr));
		
		// above is all old stuff
		// using new object to send new header info
		if(previousReactor instanceof BaseReducerReactor) {
			Map<String, Object> headerInfo = ((BaseReducerReactor) previousReactor).getColumnDataMap();
			curReactor.set("MERGE_HEADER_INFO", headerInfo);
		}
		
		runner.setResponse(previousReactor.getValue(expr));
		runner.setStatus((STATUS) previousReactor.getValue("STATUS"));
	}

	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MAP OBJECT MANIPUALATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////

	@Override
	public void caseAOptionsMap(AOptionsMap node) {
		// this is literally just a comma with a map object
		// we apply the map object
		node.getMapObj().apply(this);
		// the out will take that map object
		// and put it back with a Math_Param key
		outAOptionsMap(node);
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
		Map<Object, Object> values = new LinkedHashMap<Object, Object>();

		// we specifically call the processing on each of the key-value
		// groups such that we are responsible for the aggregation of
		// these pieces
		if (node.getKeyvalue() != null) {
			node.getKeyvalue().apply(this);
			// going through a key-value will put a map in mystore
			// that only has one key
			values.putAll((Map<Object, Object>) curReactor.removeLastStoredKey());
		}
		List<PKeyvalueGroup> copy = new ArrayList<PKeyvalueGroup>(node.getKeyvalueGroup());
		for (PKeyvalueGroup e : copy) {
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

    public void outAHtmlWordOrNumOrNestedObj(AHtmlWordOrNumOrNestedObj node)
    {
    	String word = node.getHtmlText().toString().trim();
		String cleaned = word.substring(1, word.length() - 1);// remove the
																// quotes
		curReactor.put(PKQLEnum.WORD_OR_NUM, cleaned);
		curReactor.addReplacer(word, cleaned);
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
	public void outAOptionsMap(AOptionsMap node) {
		// this is called right after a map object has just been placed into the
		// current reactor
		// just grab that map and put it back into the reactor but with a
		// Math_Param key
		Map<Object, Object> mathParamMapObj = (Map<Object, Object>) curReactor.removeLastStoredKey();
		curReactor.put(PKQLEnum.MAP_OBJ, mathParamMapObj);
	}

	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR
	/////////////////////////////////////////// MANIPULATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR
	/////////////////////////////////////////// MANIPULATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR
	/////////////////////////////////////////// MANIPULATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR
	/////////////////////////////////////////// MANIPULATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR
	/////////////////////////////////////////// MANIPULATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////
	/////////////////////////////////////////// MATRIX + VECTOR + SCALAR
	/////////////////////////////////////////// MANIPULATION HERE
	/////////////////////////////////////////// ////////////////////////////////////////////////////

	@Override
	public void caseACsvTable(ACsvTable node) {
		inACsvTable(node);
	}

	@Override
	public void inACsvTable(ACsvTable node) {
		List<List<Object>> csvTable = new Vector<List<Object>>();

		List<PCsvRow> copy = new ArrayList<PCsvRow>(node.getCsvRow());
		for (PCsvRow e : copy) {
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
		if (optionalCols != null && !optionalCols.isEmpty()) {
			ListIterator<PColGroup> it = optionalCols.listIterator();
			while (it.hasNext()) {
				AColGroup group = (AColGroup) it.next();
				colVec.add(((AColDef) group.getColDef()).getColname().getText());
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
		if (node.getWordOrNum() != null) {
			node.getWordOrNum().apply(this);
			rowVec.add(curReactor.removeLastStoredKey());
		}
		List<PCsvGroup> copy = new ArrayList<PCsvGroup>(node.getCsvGroup());
		for (PCsvGroup e : copy) {
			e.apply(this);
			rowVec.add(curReactor.removeLastStoredKey());
		}

		curReactor.put(PKQLEnum.ROW_CSV, rowVec);
		curReactor.addReplacer(node.toString(), rowVec);
	}

	@Override
	public void inAAlphaWordOrNum(AAlphaWordOrNum node) {
		String word = (node.getWord() + "").trim();
		String cleaned = word.substring(1, word.length() - 1);// remove the
																// quotes
		curReactor.put(PKQLEnum.WORD_OR_NUM, cleaned);
		curReactor.addReplacer(word, cleaned);
	}

	@Override
	public void inANumWordOrNum(ANumWordOrNum node) {
		ADecimal dec = (ADecimal) node.getDecimal();
		String fraction = dec.getFraction() + "";
		Number num = null;
		String number = dec.getWhole().toString().trim();
		if (dec.getFraction() != null) {
			number = number + "." + fraction;
			num = Double.parseDouble(number);
		} else {
			try {
				num = Integer.parseInt(number);
			} catch(NumberFormatException e) {
				num = Long.parseLong(number);//if number is too big for integer parse it as a long
			}
		}
		curReactor.put(PKQLEnum.WORD_OR_NUM, num);
	}

    public void inAEmptyWordOrNum(AEmptyWordOrNum node)
    {
    	TNull nullVal = (TNull) node.getNull();
//		curReactor.put(PKQLEnum.WORD_OR_NUM, AbstractTableDataFrame.VALUE.NULL);
    }
    
    public void outAVariableWordOrNum(AVariableWordOrNum node)
    {
       AVarDef node2 = (AVarDef) node.getVarDef();
       String valName = node2.getValname().toString().trim();
       Object value = runner.getVariableValue(valName);
       curReactor.put(PKQLEnum.WORD_OR_NUM, value);
    }
  
	@Override
	public void inAExprWordOrNum(AExprWordOrNum node) {

	}
	
	
	@Override
	public void inADecimal(ADecimal node) {
		String fraction = node.getFraction() + "";
		String number = node.getWhole().toString().trim();
		if (node.getFraction() != null) {
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

	// **************************************** START DATA OPERATIONS
	// **********************************************//

	public void inADatatype(ADatatype node) {
		System.out.println("Translation.inADatatype() with node = " + node);
		if (reactorNames.containsKey(PKQLEnum.DATA_TYPE)) {
			initReactor(PKQLEnum.DATA_TYPE);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.DATA_TYPE, nodeStr);
		}
	}

	public void outADatatype(ADatatype node) {
		System.out.println("Translation.outADatatype() with node = " + node);
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_TYPE, thisNode, PKQLEnum.DATA_TYPE);
		runner.setResponse("PKQL processing complete");
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
		// runner.setCurrentString(PKQLEnum.DATA_TYPE);

		// set retData
		Map<String, Object> retDataMap = new HashMap<String, Object>();
		retDataMap.put(PKQLEnum.DATA_TYPE, thisReactor.getValue(PKQLEnum.DATA_TYPE));
		runner.setReturnData(retDataMap);
	}

	public void inADataconnect(ADataconnect node) {
		System.out.println("Translation.inADataconnect() with node = " + node);
		if (reactorNames.containsKey(PKQLEnum.DATA_CONNECT)) {
			initReactor(PKQLEnum.DATA_CONNECT);
			String nodeStr = node.toString().trim();
			curReactor.put(PKQLEnum.DATA_CONNECT, nodeStr);
		}
	}

	public void outADataconnect(ADataconnect node) {
		System.out.println("Translation.outADataconnect() with node = " + node);
		String nodeDataconnect = node.getDataconnectToken().toString().trim();
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_CONNECT, nodeDataconnect,
				PKQLEnum.DATA_CONNECT);
		runner.setResponse("PKQL processing complete");
		runner.setStatus((STATUS) thisReactor.getValue("STATUS"));//
		// runner.setCurrentString(PKQLEnum.DATA_CONNECT);

		// set feData
		Map<String, Object> retDataMap = new HashMap<String, Object>();
		retDataMap.put("connection", thisReactor.getValue(PKQLEnum.DATA_CONNECT));
		runner.setReturnData(retDataMap);
	}

	public void inADataconnectdb(ADataconnectdb node) {
		initReactor(PKQLEnum.DATA_CONNECTDB);
		String nodeStr = node.toString().trim();
		curReactor.put(PKQLEnum.DATA_CONNECTDB, nodeStr);
	}

	public void outADataconnectdb(ADataconnectdb node) {
		String thisNode = node.toString().trim();
		IScriptReactor thisReactor = curReactor;
		Hashtable<String, Object> thisReactorHash = deinitReactor(PKQLEnum.DATA_CONNECTDB, thisNode,
				PKQLEnum.DATA_CONNECTDB);
	}

	public void inAJOp(AJOp node) {
		if (reactorNames.containsKey(PKQLEnum.JAVA_OP)) {
			initReactor(PKQLEnum.JAVA_OP);
			curReactor.put("PKQLRunner", runner);
			String nodeExpr = node.getCodeblock().toString().trim();
			curReactor.put(PKQLEnum.JAVA_OP, nodeExpr);
		}
	}

	public void outAJOp(AJOp node) {
		if (reactorNames.containsKey(PKQLEnum.JAVA_OP)) {
			IScriptReactor thisReactor = curReactor;
			deinitReactor(PKQLEnum.JAVA_OP, node.getCodeblock() + "", null, false);
			IDataMaker newFrame = (IDataMaker) thisReactor.getValue("G");
			if(newFrame != null) {
				this.frame = newFrame;
				this.reactorNames = this.frame.getScriptReactors();
			}
			Object retData = thisReactor.getValue("returnData");
			if(retData != null){
				runner.setReturnData(retData);
			}
			runner.setResponse(curReactor.getValue("RESPONSE"));
			runner.setStatus((STATUS) curReactor.getValue("STATUS"));
		}
	}

	public void inADatanetworkconnect(ADatanetworkconnect node) {
		if (reactorNames.containsKey(PKQLEnum.NETWORK_CONNECT)) {
			initReactor(PKQLEnum.NETWORK_CONNECT);
			// curReactor.put(PKQLEnum.NETWORK_CONNECT, "CONNECT");
			if (node.getTablename() != null)
				curReactor.put("TABLE_NAME", node.getTablename());
		}
	}

	public void outADatanetworkconnect(ADatanetworkconnect node) {
		if (reactorNames.containsKey(PKQLEnum.NETWORK_CONNECT)) {
			deinitReactor(PKQLEnum.NETWORK_CONNECT, PKQLEnum.NETWORK_CONNECT, null, false);
			runner.setResponse(curReactor.getValue("RESPONSE"));
			runner.setStatus((STATUS) curReactor.getValue("STATUS"));
		}
	}

	public void inADatanetworkdisconnect(ADatanetworkdisconnect node) {
		if (reactorNames.containsKey(PKQLEnum.NETWORK_DISCONNECT)) {
			initReactor(PKQLEnum.NETWORK_DISCONNECT);
		}
	}

	public void outADatanetworkdisconnect(ADatanetworkdisconnect node) {
		deinitReactor(PKQLEnum.NETWORK_DISCONNECT, PKQLEnum.NETWORK_DISCONNECT, null, false);
	}
	
	@Override
	public void outADataInsightid(ADataInsightid node) {
		String insightId = runner.getInsightId();
		
		List<Map<String, Object>> returnDataList = new ArrayList<>();
		Map<String, Object> insightIdMap = new HashMap<>();
		insightIdMap.put("name", insightId);
		returnDataList.add(insightIdMap);
		
		Map returnData = new HashMap();
		returnData.put("list", returnDataList);
		
		storePkqlMetadata.add(new DataInsightMetaData());
		
		runner.setReturnData(returnData);
		runner.setResponse(runner.getInsightId());
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
    }
	
	

	// **************************************** SYNCHRONIZATION OF DATAMAKER
	// ****************************************//

	public IDataMaker getDataFrame() {
		if (this.curReactor != null) {
			IDataMaker table = (IDataMaker) this.curReactor.getValue("G");
			if (table == null) {
				return this.frame;
			}
			return table;
		} else {
			return null;
		}
	}

	// **************************************** DATABASE RELATED OPERATIONS
	// ****************************************//

	@Override
	public void outADatabaseList(ADatabaseList node) {
		// just get the list of engines
		List<String> dbList = MasterDatabaseUtility.getAllEngineIds();
		// put it in a map so the FE knows what it is looking at
		Map returnData = new HashMap();
		returnData.put("list", convertListToListMaps(dbList));
		runner.setReturnData(returnData);
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void outADatabaseConcepts(ADatabaseConcepts node) {
		// get the engine
		String engineName = node.getEngineName().toString().trim();
		Set<String> concepts = MasterDatabaseUtility.getConceptsWithinEngineRDBMS(engineName);

		// put it in a map so the FE knows what it is looking at
		Map returnData = new HashMap();
		returnData.put("list", convertListToListMaps(new ArrayList<>(concepts)));

		runner.setReturnData(returnData);
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}
	
	@Override
    public void outADatabaseConnectedConcepts(ADatabaseConnectedConcepts node) {
		// TODO: this should technically take in a list
		// TODO: currently only doing this for a single logical name
		
		String conceptLogicalName = node.getConceptType().toString().trim();
		List<String> logicalNames = new Vector<String>();
		logicalNames.add(conceptLogicalName);
		
		Map connectedConceptsData = MasterDatabaseUtility.getConnectedConceptsRDBMS(logicalNames, null);

		runner.setReturnData(connectedConceptsData);
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void outADatabaseMetamodel(ADatabaseMetamodel node) {
		// get the engine
		String engineName = node.getEngineName().toString().trim();
		// get the metamodel for the engine
		runner.setReturnData(MasterDatabaseUtility.getMetamodelRDBMS(engineName, true));
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}

	@Override
	public void outADatabaseConceptProperties(ADatabaseConceptProperties node) {
		// TODO: this should technically take in a list
		// TODO: currently only doing this for a single logical name
		
		// get the concept name
		String conceptLogicalName = node.getConceptName().toString().trim();
		List<String> logicalNames = new Vector<String>();
		logicalNames.add(conceptLogicalName);
	
		//see if we need to get for a specific engine
		boolean hasEngine = node.getEngineName() != null;
		String engineName = null;
		if(hasEngine) {
			engineName = node.getEngineName().toString().trim();
			List<String> eList = new Vector<String>();
			eList.add(engineName);
			Map<String, Object[]> retData = MasterDatabaseUtility.getConceptPropertiesRDBMS(logicalNames, eList);
			//iterate through cause not sure if engine key will get reformatted
			for(String engineKey : retData.keySet()) {
				Object[] props = retData.get(engineKey);
				
				if(props.length > 0) {
					Map propMap = ((MetamodelVertex)props[0]).toMap();
					Set<String> values = (Set<String>)propMap.get("propSet");//should be an array or list
					List<String> valuesList = new ArrayList<>(values);
					Map<String, Object> formattedRetData = new HashMap<>();
					formattedRetData.put("list", convertListToListMaps(valuesList));
					runner.setReturnData(formattedRetData);
				}
				break;
			}
		} else {
			Map<String, Object[]> retData = MasterDatabaseUtility.getConceptPropertiesRDBMS(logicalNames, null);
			runner.setReturnData(retData);
		}
		
		// get the properties for this concept across all engines
//		runner.setReturnData(DatabasePkqlService.getConceptProperties(logicalNames, null));
		runner.setStatus(PKQLRunner.STATUS.SUCCESS);
	}
	
	private String removeQuotes(String value) {
		if(value.startsWith("'") || value.startsWith("\"")) {
			value = value.trim().substring(1, value.length() - 1);
		}
		return value;
	}
	
	private List<Map<String, Object>> convertListToListMaps(List valueList) {
		if (valueList == null) return new ArrayList<>();
		
		List<Map<String, Object>> listMaps = new ArrayList<>(valueList.size());
		for(Object value : valueList) {
			Map<String, Object> map = new HashMap<>(1);
			map.put("name", value);
			listMaps.add(map);
		}
		return listMaps;
	}
}