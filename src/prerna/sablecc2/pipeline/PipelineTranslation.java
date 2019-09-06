package prerna.sablecc2.pipeline;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.om.Insight;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.AbstractFileQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.JdbcHardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.frame.FrameFactory;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class PipelineTranslation extends LazyTranslation {

	private static final Logger LOGGER = LogManager.getLogger(PipelineTranslation.class.getName());
	private static Map<String, String> reactorToId = null;
	
	private static Map<AbstractQueryStruct.QUERY_STRUCT_TYPE, String> qsToWidget = new HashMap<AbstractQueryStruct.QUERY_STRUCT_TYPE, String>();
	private static List<String> qsReactors = new Vector<String>();
	private static List<String> fileReactors = new Vector<String>();
	static {
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE, "pipeline-app");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME, "pipeline-frame");
//		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.LAMBDA, value);
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, "pipeline-query");
//		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY, "pipeline-query");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY, "pipeline-external");
		
		// frame importing
		qsReactors.add("Import");
		qsReactors.add("Merge");
		qsReactors.add("FuzzyMerge");
		
		// file + database importing
		fileReactors.add("ToCsv");
		fileReactors.add("ToTsv");
		fileReactors.add("ToTxt");
		fileReactors.add("ToExcel");
		fileReactors.add("ToDatabase");
	}
	
	private List<List<PipelineOperation>> allRoutines = new Vector<List<PipelineOperation>>();
	private List<PipelineOperation> curRoutine;
	
	public PipelineTranslation(Insight insight) {
		super();
		this.insight = insight;
		if(this.insight != null) {
			// we will copy the var store
			VarStore copy = new VarStore();
			copy.putAll(insight.getVarStore());
			this.planner.setVarStore(copy);
		}
		if(reactorToId == null) {
			init();
		}
	}
	
	@Override
	public void caseAConfiguration(AConfiguration node) {
        List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        int size = copy.size();
        for(int pixelstep = 0; pixelstep < size; pixelstep++)
        {
        	PRoutine e = copy.get(pixelstep);
        	try {
            	this.curRoutine = new Vector<PipelineOperation>();
        		this.allRoutines.add(this.curRoutine);
        		
        		e.apply(this);
        		// reset the state of the frame
        		this.currentFrame = null;
        	} catch(SemossPixelException ex) {
        		trackError(e.toString(), this.isMeta, ex);
        		ex.printStackTrace();
        		// if we want to continue the thread of execution
        		// nothing special
        		// just add the error to the return
        		if(ex.isContinueThreadOfExecution()) {
        			planner.addVariable("$RESULT", ex.getNoun());
            		postProcess(e.toString().trim());
        		} else {
        			// if we do want to stop
        			// propagate the error up and the PixelRunner
        			// will handle grabbing the meta and returning it to the FE
        			postRuntimeErrorProcess(e.toString(), ex.getNoun(), 
        					copy.subList(pixelstep+1, size).stream().map(p -> p.toString()).collect(Collectors.toList()));
        			throw ex;
        		}
        	} catch(Exception ex) {
        		trackError(e.toString(), this.isMeta, ex);
        		ex.printStackTrace();
        		planner.addVariable("$RESULT", new NounMetadata(ex.getMessage(), PixelDataType.ERROR, PixelOperationType.ERROR));
        		postProcess(e.toString().trim());
        	}
        }
	}
	
	////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Scalar values
	 */
	
	@Override
	public void inAIdWordOrId(AIdWordOrId node) {
		super.inAIdWordOrId(node);
		if(this.curReactor == null) {
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());

	    	String idInput = node.getId().getText().trim();

			Map<String, Object> obj = new HashMap<String, Object>();
			if(this.planner.hasVariable(idInput)) {
				NounMetadata noun = this.planner.getVariable(idInput);
				if(noun.getNounType() != PixelDataType.LAMBDA) {
					if(noun.getNounType() == PixelDataType.FRAME) {
						op = new ConstantPipelineOperation(idInput, idInput);
						obj.putAll(processFrameNounMap(noun));
					} else {
						op = new ConstantPipelineOperation(idInput, idInput);
						obj.putAll(processBasicNounMap(noun));
					}
				}
	    	}
			// if we haven't filled it
			// just set to default
			if(obj.isEmpty()) {
				op = new ConstantPipelineOperation("direct value", idInput);
				obj.put("type", "COLUMN_OR_VAR");
				obj.put("value", idInput);
	    	}
	    	
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}
	
	@Override
	public void inAWordWordOrId(AWordWordOrId node) {
		super.inAWordWordOrId(node);
		if(this.curReactor == null) {
			Map<String, Object> obj = new HashMap<String, Object>();
			obj.put("type", "STRING");
			obj.put("value", PixelUtility.removeSurroundingQuotes(node.getWord().getText().toString()));
			
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}
	
	@Override
	public void inAFractionDecimal(AFractionDecimal node) {
		super.inAFractionDecimal(node);
		if(this.curReactor == null) {
			Map<String, Object> obj = new HashMap<String, Object>();
			obj.put("type", "NUMBER");
			String fraction = (node.getFraction()+"").trim();
			Number retNum = new BigDecimal("0." + fraction);
			obj.put("value", retNum);
			
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}
	
	@Override
	public void inAWholeDecimal(AWholeDecimal node) {
		super.inAWholeDecimal(node);
		if(this.curReactor == null) {
			Map<String, Object> obj = new HashMap<String, Object>();
			obj.put("type", "NUMBER");

			boolean isDouble = false;
	    	String whole = "";
	    	String fraction = "";
	    	// get the whole portion
	    	if(node.getWhole() != null) {
	    		whole = node.getWhole().toString().trim();
	    	}
	    	// get the fraction portion
	    	if(node.getFraction() != null) {
	    		isDouble = true;
	    		fraction = (node.getFraction()+"").trim();
	    	} else {
	    		fraction = "0";
	    	}
			Number retNum = new BigDecimal(whole + "." + fraction);
			
			if(isDouble) {
				obj.put("value", retNum.doubleValue());
	    	} else {
				obj.put("value", retNum.intValue());
	    	}
			
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}
	
	@Override
	public void inABooleanScalar(ABooleanScalar node) {
		super.inABooleanScalar(node);
		if(this.curReactor == null) {
			String booleanStr = node.getBoolean().toString().trim();
	    	Boolean bool = Boolean.parseBoolean(booleanStr);
	    	Map<String, Object> obj = new HashMap<String, Object>();
			obj.put("type", "BOOLEAN");
			obj.put("value", bool);
			ConstantPipelineOperation op = new ConstantPipelineOperation("direct value", node.toString());
			op.setScalarMap(obj);
			this.curRoutine.add(op);
		}
	}
	
	////////////////////////////////////////////////////////////////////////////////

	
	/**
	 * Same method as in lazy with addition of addRoutine method
	 */
	@Override
    protected void deInitReactor() {
    	if(curReactor != null) {
    		// merge up and update the plan
    		try {
    			curReactor.mergeUp();
    			curReactor.updatePlan();
    			addRoutine();
    		} catch(Exception e) {
    			e.printStackTrace();
    			throw new IllegalArgumentException(e.getMessage());
    		}
    		// get the parent
    		Object parent = curReactor.Out();
    		// set the parent as the curReactor if it is present
    		prevReactor = curReactor;
    		if(parent != null && parent instanceof IReactor) {
    			curReactor = (IReactor) parent;
    		} else {
    			curReactor = null;
    		}
    	}
    }
    
    private void addRoutine() {
    	if(this.curReactor.getParentReactor() == null) {
    		// we have a reactor with no parent
    		// let us go and process the operation
    		// this method is recursive to handle embedded reactors
    		// which would be children for this root reactor
    		PipelineOperation op = generatePipelineOperation(this.curReactor);
    		
    		// add the op to the routine
    		this.curRoutine.add(op);
    	}
    }
    
	////////////////////////////////////////////////////////////////////////////////

    private PipelineOperation generatePipelineOperation(IReactor reactor) {
    	String[] reactorInputs = reactor.getPixel();
    	String reactorId = reactorInputs[0];
		PipelineOperation op = new PipelineOperation(reactorId, reactorInputs[1]);
		
		if(PipelineTranslation.qsReactors.contains(reactorId) 
				|| PipelineTranslation.fileReactors.contains(reactorId)) {
			// we need to merge the previous QS portions into this reactor
			combineQSComponents(op);
		} 
		// the file reactors do not set the widget id directly since 
		// the structure doesn't care about the qs source at the moment...
		// so will set it here if reactor to id has the stuff
		// the import/merge are not defined here
		if(PipelineTranslation.reactorToId != null && PipelineTranslation.reactorToId.containsKey(reactorId)) {
			op.setWidgetId(PipelineTranslation.reactorToId.get(reactorId));
		}
		
		if(reactorId.equals("R") || reactorId.equals("Py") || reactorId.equals("Java")) {
			// we need to process codeblock a bit different
			// we want to decode what we input
			NounStore store = reactor.getNounStore();
			GenRowStruct struct = store.getNoun("all");
			if(struct != null && !struct.isEmpty()) {
				String value = struct.get(0).toString();
				value = Utility.decodeURIComponent(value);
				NounMetadata noun = new NounMetadata(value, PixelDataType.CONST_STRING);
				Map<String, Object> nounMapRet = processNounMetadata(noun);
				op.addRowInput(nounMapRet);
			}
		} else {
			// now add all the inputs
			NounStore store = reactor.getNounStore();
			Set<String> nounKeys = store.nounRow.keySet();
			for(String nounKey : nounKeys) {
				// grab the genrowstruct for the noun
				// and add its vector to the inputs list
				GenRowStruct struct = store.getNoun(nounKey);
				if(nounKey.equals("all")) {
					// this is passed directly into the reactor
					for(int i = 0; i < struct.size(); i++) {
						NounMetadata noun = struct.getNoun(i);
						// handles if lambda, frame, or basic input
						Map<String, Object> nounMapRet = processNounMetadata(noun);
						op.addRowInput(nounMapRet);
					}
				} else {
					// this is passed based on a key
					for(int i = 0; i < struct.size(); i++) {
						NounMetadata noun = struct.getNoun(i);
						// handles if lambda, frame, or basic input
						Map<String, Object> nounMapRet = processNounMetadata(noun);
						op.addNounInputs(nounKey, nounMapRet);
					}
				}
			}
		}
		
		return op;
    }
    
    private void combineQSComponents(PipelineOperation op) {
    	Map<String, Object> qsMap = new HashMap<String, Object>();
    	// we will add the qs at the end
    	// since we could reassign the qs object
    	SelectQueryStruct qs = new SelectQueryStruct();
    	
    	// combine all the existing noun portions
    	// thankfully the QS is already a builder construct
    	for(PipelineOperation routine : this.curRoutine) {
    		// we will do a lot of stuff based on the routine name
    		// as it is the reactor name
    		
    		String opName = routine.getOpName();
    		if(opName.equals("Database")) {
    			qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
    			
    			// grab the database id from the  input
    			String dbId = getStringOpNounInput("database", routine.getNounInputs());
    			if(dbId == null) {
    				dbId = getStringOpRowInput(0, routine.getRowInputs());
    			}
    			qs.setEngineId(dbId);
    			
    		} else if(opName.equals("Frame")) {
    			qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME);
    			
    			// grab the database id from the  input
    			String frameName = getFrameNameOpNounInput("frame", routine.getNounInputs());
    			if(frameName == null) {
    				frameName = getFrameNameOpRowInput(0, routine.getRowInputs());
    			}
    			qs.setFrameName(frameName);
    			
    		} else if(opName.equals("Select")) {
    			List<Map> rowInputs = routine.getRowInputs();
    			int size = rowInputs.size();
    			
    			for(int i = 0; i < size; i++) {
    				// the value should already be a query column selector
    				Map<String, Object> rowMap = rowInputs.get(i);
    				Object rowValue = rowMap.get("value");
    				if(rowValue instanceof IQuerySelector) {
    					qs.addSelector( (IQuerySelector) rowValue);
    				}
    			}
    			
    		} else if(opName.equals("Query")) {
    			// make sure we have the correct type of raw query
    			// if it is run on an engine or a frame
    			HardSelectQueryStruct newQs = new HardSelectQueryStruct();
    			if(qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
    				newQs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
        			newQs.setEngineId(qs.getEngineId());
    			} else {
    				newQs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY);
    			}
    			
    			// set the query directly
    			String query = getStringOpRowInput(0, routine.getRowInputs());
       			query = Utility.decodeURIComponent(query);
       			newQs.setQuery(query);
    			
    			// reset reference of qs to the new hard qs after merging the inputs
    			qs = newQs;
    		
    		} else if(opName.equals("FileRead")) {
    			
    			// first grab the specific componenets for excel vs. csv/text file
    			Map<String, List<Map>> nounInputs = routine.getNounInputs();
				String filepath = getStringOpNounInput("filePath", nounInputs);
    			String sheetName = getStringOpNounInput("sheetName", nounInputs);
    			String sheetRange = getStringOpNounInput("sheetRange", nounInputs);
				String delimiter = getStringOpNounInput("delimiter", nounInputs);

    			boolean isExcel = sheetName != null && sheetRange != null;
    			
    			AbstractFileQueryStruct newQs = null;
    			if(isExcel) {
    				newQs = new ExcelQueryStruct();
    				// get the sheet name
    				((ExcelQueryStruct) newQs).setSheetName(sheetName);
    				// get the sheet range
    				((ExcelQueryStruct) newQs).setSheetName(sheetRange);
    			} else {
    				newQs = new CsvQueryStruct();
    				((CsvQueryStruct) newQs).setDelimiter(delimiter.charAt(0));
    			}
    			
    			// now grab the shared parts
    			Map<String, String> dataTypeMap = parseMapReactor("dataTypeMap", nounInputs);
    			Map<String, String> additionalDataTypes = parseMapReactor("additionalDataTypes", nounInputs);
    			Map<String, String> newHeaders = parseMapReactor("newHeaders", nounInputs);

    			newQs.setFilePath(filepath);
    			newQs.setColumnTypes(dataTypeMap);
    			newQs.setAdditionalTypes(additionalDataTypes);
    			newQs.setNewHeaderNames(newHeaders);
    			
    			qs = newQs;
    		
    		} else if(opName.equals("DirectJDBCConnection")) {
    			
    			/*
    			 * This is the same as the Query 
    			 * But will need to add some additional parts after
    			 */
    			
    			JdbcHardSelectQueryStruct newQs = new JdbcHardSelectQueryStruct();

    			// get all the inputs
    			Map<String, List<Map>> nounInputs = routine.getNounInputs();

    			String query = getStringOpNounInput(ReactorKeysEnum.QUERY_KEY.getKey(), nounInputs);
    			query = Utility.decodeURIComponent(query);
       			newQs.setQuery(query);
       			
    			String driver = getStringOpNounInput(ReactorKeysEnum.DB_DRIVER_KEY.getKey(), nounInputs);
    			String username = getStringOpNounInput(ReactorKeysEnum.PASSWORD.getKey(), nounInputs);
				String password = getStringOpNounInput(ReactorKeysEnum.USERNAME.getKey(), nounInputs);
    			String connectionUrl = getStringOpNounInput(ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), nounInputs);
    			
    			Map<String, Object> config = new HashMap<String, Object>();
    			config.put(ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), connectionUrl);
    			config.put(ReactorKeysEnum.DB_DRIVER_KEY.getKey(), driver);
    			config.put(ReactorKeysEnum.USERNAME.getKey(), username);
    			config.put(ReactorKeysEnum.PASSWORD.getKey(), password);

    			newQs.setConfig(config);
    			
    			// reset reference of qs to the new hard qs after merging the inputs
    			qs = newQs;
    		}
    	}
    	
    	// we will add the qs at the end
    	// since we could reassign the qs object
    	qsMap.put("value", qs);
    	op.addNounInputs("qs", qsMap);
    	if(PipelineTranslation.qsReactors.contains(op.getOpName())) {
    		op.setWidgetId(PipelineTranslation.qsToWidget.get(qs.getQsType()));
    	}
	}
    
    /**
     * Get map from input as it gets pushed into a reactor structure / pipeline operation
     * @param key
     * @param nounInputs
     * @return
     */
    private Map<String, String> parseMapReactor(String key, Map<String, List<Map>> nounInputs) {
    	Map<String, String> output = null;
    	if(nounInputs.containsKey(key)) {
			List<Map> nounMapList = nounInputs.get(key);
			if(nounMapList != null && !nounMapList.isEmpty()) {
				Map<String, Object> nounMap = nounMapList.get(0);
				PipelineOperation nounMapVal = (PipelineOperation) nounMap.get("value");
				// we can reconstruct the map looking at each pair of obj/val that is passed in
				List<Map> inputs = nounMapVal.getRowInputs();
				
				output = new HashMap<String, String>();
				for(int i = 0; i < inputs.size(); i=i+2) {
					Map<String, Object> keyMap = inputs.get(i);
					Map<String, Object> valMap = inputs.get(i+1);
					
					output.put(keyMap.get("value").toString(), valMap.get("value").toString());
				}
			}
		}
    	return output;
    }

    private String getStringOpNounInput(String key, Map<String, List<Map>> nounInputs) {
    	String output = null;
    	if(nounInputs.containsKey(key)) {
			List<Map> nounMap = nounInputs.get(key);
			if(nounMap != null) {
				output = (String) nounMap.get(0).get("value");
			}
		}
    	return output;
    }
    
    private String getStringOpRowInput(int index, List<Map> nounInputs) {
    	String output = null;
    	if(nounInputs.size() > index) {
			Map<String, Object> nounMap = nounInputs.get(index);
			if(nounMap != null) {
				output = (String) nounMap.get("value");
			}
		}
    	return output;
    }
    
    private String getFrameNameOpNounInput(String key, Map<String, List<Map>> nounInputs) {
    	String output = null;
    	if(nounInputs.containsKey(key)) {
			List<Map> nounMap = nounInputs.get(key);
			if(nounMap != null) {
				output = (String) nounMap.get(0).get("name");
			}
		}
    	return output;
    }
    
    private String getFrameNameOpRowInput(int index, List<Map> nounInputs) {
    	String output = null;
    	if(nounInputs.size() > index) {
			Map<String, Object> nounMap = nounInputs.get(index);
			if(nounMap != null) {
				output = (String) nounMap.get("name");
			}
		}
    	return output;
    }
    
	////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////

    /*
     * Process noun metadata based on the type + allow for gson
     */
    
    private Map<String, Object> processNounMetadata(NounMetadata noun) {
		PixelDataType type = noun.getNounType();
		if(type == PixelDataType.LAMBDA) {
			return processLambdaNounMap(noun);
		} else if(type == PixelDataType.FRAME) {
			return processFrameNounMap(noun);
		} else {
			return processBasicNounMap(noun);
		}
    }
    
    private Map<String, Object> processLambdaNounMap(NounMetadata noun) {
    	Map<String, Object> lambdaMap = new HashMap<String, Object>();
    	lambdaMap.put("type", noun.getNounType());
    	// the value is another PixelOperation
    	lambdaMap.put("value", generatePipelineOperation(  (IReactor) noun.getValue()));
    	return lambdaMap;
    }
    
    private Map<String, Object> processBasicNounMap(NounMetadata noun) {
    	Map<String, Object> basicInput = new HashMap<String, Object>();
		basicInput.put("type", noun.getNounType());
		basicInput.put("value", noun.getValue());
		return basicInput;
    }
    
    private Map<String, Object> processFrameNounMap(NounMetadata noun) {
    	Map<String, Object> frameMap = new HashMap<String, Object>();
		ITableDataFrame frame = (ITableDataFrame) noun.getValue();
		frameMap.put("type", FrameFactory.getFrameType(frame));
		String name = frame.getName();
		if(name != null) {
			frameMap.put("name", name);
		}
		return frameMap;
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////

    
    /**
     * Return the reactor based on the reactorId
     * @param reactorId - reactor id or operation
     * @param nodeString - full operation
     * @return	IReactor		A new instance of the reactor
     */
    protected IReactor getReactor(String reactorId, String nodeString) {
    	try {
    		return super.getReactor(reactorId, nodeString);
    	} catch(Exception e) {
    		// error finding reactor
    		// just return a generic reactor placeholder
    		LOGGER.info("Error finding reactor " + reactorId);
    	}
    	
    	UndeterminedPipelineReactor reactor = new UndeterminedPipelineReactor();
    	reactor.setPixel(reactorId, nodeString);
    	return reactor;
    }
    
	private static synchronized void init() {
		if(PipelineTranslation.reactorToId == null) {
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			File reactorWidgetFile = new File(baseFolder + "/reactorToWidget.csv");
			if(reactorWidgetFile.exists()) {
				CSVFileHelper helper = new CSVFileHelper();
				helper.parse(reactorWidgetFile.getAbsolutePath());
				
				PipelineTranslation.reactorToId = new HashMap<String, String>();
				
				String[] row = null;
				while( (row = helper.getNextRow()) != null ) {
					PipelineTranslation.reactorToId.put(row[0], row[1]);
				}
			}
		}
	}
	
	public List<List<PipelineOperation>> getAllRoutines() {
		return this.allRoutines;
	}
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	/*
	 * Testing
	 */
	
	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\workspace2\\Semoss_Dev\\RDF_Map.prop");
//		IEngine coreEngine = new RDBMSNativeEngine();
//		coreEngine.openDB("C:\\workspace2\\Semoss_Dev\\db\\MovieDatesH2__f77ba49e-a8a3-41bd-94c5-91d0a3103bbb.smss");
//		DIHelper.getInstance().setLocalProperty(coreEngine.getEngineId(), coreEngine);

		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		
		String pixel = "" 
//				+ "AddPanel ( 0 ) ;" 
//				+ "Panel ( 0 ) | AddPanelConfig ( config = [ { \"config\" : { \"type\" : \"STANDARD\" , \"opacity\" : 100 } } ] ) ;" 
//				+ "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;" + 
//				"Panel ( 0 ) | RetrievePanelEvents ( ) ;" 
//				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;" 
//				+ "Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"<encode>{\"app_id\":\"NEWSEMOSSAPP\"}</encode>\" ) ;" 
//				+ "CreateFrame ( frameType = [ GRID ] ) .as ( [ 'FRAME238470' ] ) ;" 
//				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) | Select ( MOVIE_DATES , MOVIE_DATES__Cast_Formed , MOVIE_DATES__DVD_Release , MOVIE_DATES__Director , MOVIE_DATES__Genre , MOVIE_DATES__MovieBudget , MOVIE_DATES__Nominated , MOVIE_DATES__Production_End , MOVIE_DATES__Production_Start , MOVIE_DATES__Revenue_Domestic , MOVIE_DATES__Revenue_International , MOVIE_DATES__RottenTomatoes_Audience , MOVIE_DATES__RottenTomatoes_Critics , MOVIE_DATES__Studio , MOVIE_DATES__Theatre_Release_Date , MOVIE_DATES__Title ) .as ( [ MOVIE_DATES , Cast_Formed , DVD_Release , Director , Genre , MovieBudget , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ] ) | Import ( frame = [ FRAME238470 ] ) ;" 
//				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) | Query(\"<encode> select * from movie_dates </encode>\") | Import ( frame = [ FRAME238470 ] ) ;" 
//				+ "FileRead( filePath=[\"$IF\\diabetes_____UNIQUE2019_08_14_15_20_44_0226.csv\" ], dataTypeMap=[{\"patient\":\"INT\",\"chol\":\"INT\",\"stab_glu\":\"INT\",\"hdl\":\"INT\",\"ratio\":\"DOUBLE\",\"glyhb\":\"DOUBLE\",\"location\":\"STRING\",\"age\":\"INT\",\"gender\":\"STRING\",\"height\":\"INT\",\"weight\":\"INT\",\"frame\":\"STRING\",\"bp_1s\":\"INT\",\"bp_1d\":\"INT\",\"bp_2s\":\"INT\",\"bp_2d\":\"INT\",\"waist\":\"INT\",\"hip\":\"INT\",\"time_ppn\":\"INT\",\"Drug\":\"STRING\",\"start_date\":\"DATE\",\"end_date\":\"DATE\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"diabetes.csv\"], additionalDataTypes=[{\"start_date\":\"M/d/yyyy\",\"end_date\":\"M/d/yyyy\"}])|Select(DND__patient, DND__chol, DND__stab_glu, DND__hdl, DND__ratio, DND__glyhb, DND__location, DND__age, DND__gender, DND__height, DND__weight, DND__frame, DND__bp_1s, DND__bp_1d, DND__bp_2s, DND__bp_2d, DND__waist, DND__hip, DND__time_ppn, DND__Drug, DND__start_date, DND__end_date).as([patient, chol, stab_glu, hdl, ratio, glyhb, location, age, gender, height, weight, frame, bp_1s, bp_1d, bp_2s, bp_2d, waist, hip, time_ppn, Drug, start_date, end_date])|Import( frame=[FRAME238470] );"
//				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;" 
//				+ "Frame ( frame = [ FRAME238470 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;" 
//				+ "FRAME238470 | Convert ( frameType = [ R ] ) .as ( [ 'FRAME238470' ] ) ;" 
//				+ "Frame ( frame = [ FRAME238470 ] ) | Select ( Cast_Formed , Director , DVD_Release , Genre , MovieBudget , MOVIE_DATES , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ) .as ( [ Cast_Formed , Director , DVD_Release , Genre , MovieBudget , MOVIE_DATES , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Cast_Formed\" , \"Director\" , \"DVD_Release\" , \"Genre\" , \"MovieBudget\" , \"MOVIE_DATES\" , \"Nominated\" , \"Production_End\" , \"Production_Start\" , \"Revenue_Domestic\" , \"Revenue_International\" , \"RottenTomatoes_Audience\" , \"RottenTomatoes_Critics\" , \"Studio\" , \"Theatre_Release_Date\" , \"Title\" ] } } } ) | Collect ( 2000 ) ;" 
//				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) | Select ( MOVIE_DATES , MOVIE_DATES__Production_Start , MOVIE_DATES__Revenue_International , MOVIE_DATES__RottenTomatoes_Audience ) .as ( [ MOVIE_DATES , Production_Start , Revenue_International , RottenTomatoes_Audience ] ) | Merge ( joins = [ ( MOVIE_DATES , inner.join , MOVIE_DATES ), ( x, inner.join, y ) ] , frame = [ FRAME238470 ] ) ;"
//				+ "FRAME238470 | Convert(frameType=['R']);"
//				+ "FRAME238470 | DateExpander ( column = [ \"Cast_Formed\" ] , options = [ \"year\" , \"month\" , \"month-name\" ] ) ;"
//				+ "Frame(FRAME238470) | QueryAll() | ToCsv();"
//				+ "RunSimilarity(instance=[\"Title\"], attributes=[\"Cast_Formed\",\"DVD_Release\"]);"
//				+ "DirectJDBCConnection(query = [\"<encode>select * from city</encode>\"], dbDriver = [\"MYSQL\"], connectionString = [\"jdbc:mysql://localhost:3306/world?user=root&password=password\"], username = [\"root\"], password = [\"password\"])|Import( frame=[FRAME238470] );"
				+ "R(\"<encode>2+2</encode>\");"
				//				+ "if(true, 5+5, 6+6);" 
//				+ "ifError ( ( Frame ( frame = [ FRAME238470 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ) , ( true ) ) ;"
				;

		Insight in = new Insight();
		in.getVarStore().put("FRAME238470", new NounMetadata(new H2Frame("FRAME238470"), PixelDataType.FRAME));
		PipelineTranslation translation = null;
		Map<String, String> encodedTextToOriginal = new HashMap<String, String>();
		try {
			pixel = PixelPreProcessor.preProcessPixel(pixel.trim(), encodedTextToOriginal);
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
			translation = new PipelineTranslation(in);

			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch(SemossPixelException e) {
			if(!e.isContinueThreadOfExecution()) {
				throw e;
			}
		} catch (ParserException | LexerException | IOException e) {
			// we only need to catch invalid syntax here
			// other exceptions are caught in lazy translation
			e.printStackTrace();
			String eMessage = e.getMessage();
			if(eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if(matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length()-1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + pixel.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
				}
			}
			System.out.println(eMessage);
		}
		
//		PixelPlanner thisPlanner = translation.getPlanner();
//		GraphTraversal<Edge, Edge> it = thisPlanner.g.traversal().E();
//		while(it.hasNext()) {
//			Edge e = it.next();
//			Vertex inV = e.inVertex();
//			Vertex outV = e.outVertex();
//			System.out.println(e.property("TYPE").value()
//					+ " ::: " 
//					+ outV.property(PixelPlanner.TINKER_ID).value() 
//					+ " ::: " 
//					+ inV.property(PixelPlanner.TINKER_ID).value());
//		}
		
		Gson gson = GsonUtility.getDefaultGson(true);
		System.out.println(gson.toJson(translation.allRoutines));
	}
	
}

