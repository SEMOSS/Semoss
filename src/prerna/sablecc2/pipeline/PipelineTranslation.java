package prerna.sablecc2.pipeline;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.reactor.IReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.node.ABooleanScalar;
import prerna.sablecc2.node.AFractionDecimal;
import prerna.sablecc2.node.AIdWordOrId;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.AWholeDecimal;
import prerna.sablecc2.node.AWordWordOrId;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PipelineTranslation extends LazyTranslation {

	private static final Logger logger = LogManager.getLogger(PipelineTranslation.class);
	private static Map<String, String> reactorToId = null;
	
	private static Map<AbstractQueryStruct.QUERY_STRUCT_TYPE, String> qsToWidget = new HashMap<>();
	private static List<String> qsReactors = new ArrayList<>();
	private static List<String> fileReactors = new ArrayList<>();
	private static List<String> codeBlocks = new ArrayList<>();
	static {
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE, "pipeline-app");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME, "pipeline-frame");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, "pipeline-query");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY, "pipeline-external");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY, "pipeline-rdf-file");

		// frame importing
		qsReactors.add("Import");
		qsReactors.add("Merge");
		qsReactors.add("FuzzyMerge");
		qsReactors.add("Union");
		
		// file + database importing
		fileReactors.add("ToCsv");
		fileReactors.add("ToTsv");
		fileReactors.add("ToTxt");
		fileReactors.add("ToExcel");
		fileReactors.add("ToDatabase");
		
		codeBlocks.add("R");
		codeBlocks.add("Py");
		codeBlocks.add("Java");
	}
	
	private List<List<PipelineOperation>> allRoutines = new ArrayList<>();
	private List<PipelineOperation> curRoutine;
	private List<Map<String, Object>> pixelIdToOperation = new ArrayList<>();
	
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
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
        List<PRoutine> copy = new ArrayList<>(node.getRoutine());
        int size = copy.size();
        for(int pixelstep = 0; pixelstep < size; pixelstep++)
        {
    		long start = System.currentTimeMillis();
        	PRoutine e = copy.get(pixelstep);
        	try {
        		this.resultKey = "$RESULT_" + e.hashCode();
        		this.pixelObj = new Pixel("tempStorage", e.toString());

            	this.curRoutine = new ArrayList<>();
        		// add the routine
            	this.allRoutines.add(this.curRoutine);
            	
        		e.apply(this);
        		// reset the state of the frame
        		this.currentFrame = null;
        	} catch(SemossPixelException ex) {
        		trackError(e.toString(), this.pixelObj.isMeta(), ex);
        		logger.error(Constants.STACKTRACE, ex);
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
        		trackError(e.toString(), this.pixelObj.isMeta(), ex);
        		logger.error(Constants.STACKTRACE, ex);
        		planner.addVariable("$RESULT", new NounMetadata(ex.getMessage(), PixelDataType.ERROR, PixelOperationType.ERROR));
        		postProcess(e.toString().trim());
        	}
        	long end = System.currentTimeMillis();
            logger.debug("Time to process = " + (end-start) + " for " + e.toString());
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

			Map<String, Object> obj = new HashMap<>();
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
			Map<String, Object> obj = new HashMap<>();
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
			Map<String, Object> obj = new HashMap<>();
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
			Map<String, Object> obj = new HashMap<>();
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
	    	Map<String, Object> obj = new HashMap<>();
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
    			logger.error(Constants.STACKTRACE, e);
    			throw new IllegalArgumentException(e.getMessage());
    		}
    		
    		// get the parent
    		Object parent = curReactor.Out();
    		// set the parent as the curReactor if it is present
    		prevReactor = curReactor;
    		if(parent instanceof IReactor) {
    			curReactor = (IReactor) parent;
    		} else {
    			curReactor = null;
    		}

    		// account for moving qs
    		if(curReactor == null && prevReactor instanceof AbstractQueryStructReactor) {
    			AbstractQueryStruct qs = ((AbstractQueryStructReactor) prevReactor).getQs();
	    		this.planner.addVariable(this.resultKey, new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
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
		
		// the file reactors do not set the widget id directly since 
		// the structure doesn't care about the qs source at the moment...
		// so will set it here if reactor to id has the stuff
		// the import/merge are not defined here
		if(PipelineTranslation.reactorToId != null && PipelineTranslation.reactorToId.containsKey(reactorId)) {
			op.setWidgetId(PipelineTranslation.reactorToId.get(reactorId));
		}
		
		if(PipelineTranslation.codeBlocks.contains(reactorId)) {
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
			Map<String, List<Map>> storeMap = reactor.getStoreMap();
			op.setNounInputs(storeMap);
		}
		
		return op;
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
    	Map<String, Object> lambdaMap = new HashMap<>();
    	lambdaMap.put("type", noun.getNounType().getKey());
    	// the value is another PixelOperation
    	lambdaMap.put("value", generatePipelineOperation(  (IReactor) noun.getValue()));
    	return lambdaMap;
    }
    
    private Map<String, Object> processBasicNounMap(NounMetadata noun) {
    	Map<String, Object> basicInput = new HashMap<>();
		basicInput.put("type", noun.getNounType().getKey());
		basicInput.put("value", noun.getValue());
		return basicInput;
    }
    
    private Map<String, Object> processFrameNounMap(NounMetadata noun) {
    	if(noun.getOpType().contains(PixelOperationType.FRAME_MAP)) {
    		return processBasicNounMap(noun);
    	}
    	Map<String, Object> frameMap = new HashMap<>();
		ITableDataFrame frame = (ITableDataFrame) noun.getValue();
		frameMap.put(ReactorKeysEnum.FRAME_TYPE.getKey(), frame.getFrameType().getTypeAsString());
		String name = frame.getOriginalName();
		if(name != null) {
			frameMap.put(PixelDataType.ALIAS.getKey(), name);
			if(!name.equals(frame.getName())) {
				frameMap.put("queryName", frame.getName());
			}
		}
		
		Map<String, Object> nounStructure = new HashMap<>();
		nounStructure.put("type", PixelDataType.FRAME_MAP.getKey());
		nounStructure.put("value", frameMap);
		return nounStructure;
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
    @Override
    protected IReactor getReactor(String reactorId, String nodeString) {
    	try {
    		return super.getReactor(reactorId, nodeString);
    	} catch(Exception e) {
    		// error finding reactor
    		// just return a generic reactor placeholder
    		logger.error("Error finding reactor " + reactorId, e);
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
				
				PipelineTranslation.reactorToId = new HashMap<>();
				
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
	
	public List<Map<String, Object>> getPixelIdToOperation() {
		return pixelIdToOperation;
	}
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	/*
	 * Testing
	 */
	
//	public static void main(String[] args) throws Exception {
////		TestUtilityMethods.loadAll("C:\\workspace2\\Semoss_Dev\\RDF_Map.prop");
////		IEngine coreEngine = new RDBMSNativeEngine();
////		coreEngine.open("C:\\workspace2\\Semoss_Dev\\db\\MovieDatesH2__f77ba49e-a8a3-41bd-94c5-91d0a3103bbb.smss");
////		DIHelper.getInstance().setLocalProperty(coreEngine.getEngineId(), coreEngine);
//
//		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//		
//		String expression = ""
////				+ "AddPanel ( panel = [ 0 ] , sheet = [ \"0\" ] ) ; "
////				+ "Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ; "
////				+ "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ; "
////				+ "Panel ( 0 ) | RetrievePanelEvents ( ) ;"
////				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;"
////				+ "Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ; "
////				+ "CreateFrame ( frameType = [ GRID ] , override = [ true ] ) .as ( [ \"diabetes_csv_FRAME954152\" ] ) ;"
////				+ "FileRead ( filePath = [ \"INSIGHT_FOLDER\\diabetes.csv\" ] , dataTypeMap = [ { \"patient\" : \"INT\" , \"chol\" : \"INT\" , \"stab_glu\" : \"INT\" , \"hdl\" : \"INT\" , \"ratio\" : \"DOUBLE\" , \"glyhb\" : \"DOUBLE\" , \"location\" : \"STRING\" , \"age\" : \"INT\" , \"gender\" : \"STRING\" , \"height\" : \"INT\" , \"weight\" : \"INT\" , \"frame\" : \"STRING\" , \"bp_1s\" : \"INT\" , \"bp_1d\" : \"INT\" , \"bp_2s\" : \"INT\" , \"bp_2d\" : \"INT\" , \"waist\" : \"INT\" , \"hip\" : \"INT\" , \"time_ppn\" : \"INT\" , \"Drug\" : \"STRING\" , \"start_date\" : \"DATE\" , \"end_date\" : \"DATE\" } ] , delimiter = [ \",\" ] , newHeaders = [ { } ] , fileName = [ \"diabetes.csv\" ] , additionalDataTypes = [ { \"start_date\" : \"M/d/yyyy\" , \"end_date\" : \"M/d/yyyy\" } ] ) | Select ( DND__patient , DND__chol , DND__stab_glu , DND__hdl , DND__ratio , DND__glyhb , DND__location , DND__age , DND__gender , DND__height , DND__weight , DND__frame , DND__bp_1s , DND__bp_1d , DND__bp_2s , DND__bp_2d , DND__waist , DND__hip , DND__time_ppn , DND__Drug , DND__start_date , DND__end_date ) .as ( [ patient , chol , stab_glu , hdl , ratio , glyhb , location , age , gender , height , weight , frame , bp_1s , bp_1d , bp_2s , bp_2d , waist , hip , time_ppn , Drug , start_date , end_date ] ) | Import ( frame = [ diabetes_csv_FRAME954152 ] ) ; "
//				
////				+ "FileRead ( filePath = [ \"INSIGHT_FOLDER\\diabetes.csv\" ] , dataTypeMap = [ { \"patient\" : \"INT\" , \"chol\" : \"INT\" , \"stab_glu\" : \"INT\" , \"hdl\" : \"INT\" , \"ratio\" : \"DOUBLE\" , \"glyhb\" : \"DOUBLE\" , \"location\" : \"STRING\" , \"age\" : \"INT\" , \"gender\" : \"STRING\" , \"height\" : \"INT\" , \"weight\" : \"INT\" , \"frame\" : \"STRING\" , \"bp_1s\" : \"INT\" , \"bp_1d\" : \"INT\" , \"bp_2s\" : \"INT\" , \"bp_2d\" : \"INT\" , \"waist\" : \"INT\" , \"hip\" : \"INT\" , \"time_ppn\" : \"INT\" , \"Drug\" : \"STRING\" , \"start_date\" : \"DATE\" , \"end_date\" : \"DATE\" } ] , delimiter = [ \",\" ] , newHeaders = [ { } ] , fileName = [ \"diabetes.csv\" ] , additionalDataTypes = [ { \"start_date\" : \"M/d/yyyy\" , \"end_date\" : \"M/d/yyyy\" } ] ) | Select ( DND__patient , DND__chol , DND__stab_glu , DND__hdl , DND__ratio , DND__glyhb , DND__location , DND__age , DND__gender , DND__height , DND__weight , DND__frame , DND__bp_1s , DND__bp_1d , DND__bp_2s , DND__bp_2d , DND__waist , DND__hip , DND__time_ppn , DND__Drug , DND__start_date , DND__end_date ) .as ( [ patient , chol , stab_glu , hdl , ratio , glyhb , location , age , gender , height , weight , frame , bp_1s , bp_1d , bp_2s , bp_2d , waist , hip , time_ppn , Drug , start_date , end_date ] ) | Import ( frame = [ CreateFrame ( frameType = [ GRID ] , override = [ true ] ) .as ( [ \"diabetes_csv_FRAME954152\" ] ) ] ) ; "
////				+ "Frame ( frame = [ diabetes_csv_FRAME954152 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;"
////				+ "diabetes_csv_FRAME954152 | ToUpperCase(\"location\"); "
////				+ "AddPanel ( 0 ) ;" 
////				+ "Panel ( 0 ) | AddPanelConfig ( config = [ { \"config\" : { \"type\" : \"STANDARD\" , \"opacity\" : 100 } } ] ) ;" 
////				+ "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;"
////				+ "Panel ( 0 ) | RetrievePanelEvents ( ) ;" 
////				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;" 
////				+ "Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"<encode>{\"app_id\":\"NEWSEMOSSAPP\"}</encode>\" ) ;" 
////				+ "CreateFrame ( frameType = [ GRID ] ) .as ( [ 'FRAME238470' ] ) ;" 
//				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) "
//					+ "| Select ( Min(MOVIE_DATES__Cast_Formed) * 2, MOVIE_DATES__GENRE).as(['CAST_FORMED', 'GENRE']) "
//					+ "| Filter( "
//							+ "("
//							+ "MOVIE_DATES__NOMINATED == ['NO'] "
//							+ "AND "
//							+ "MOVIE_DATES__NOMINATED == ['YES'] "
//							+ ") "
//							+ "AND "
//							+ "MOVIE_DATES__NOMINATED == (Database(\"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\") | Select(MOVIE_DATES__NOMINATED) | Collect(-1) ) "
//					+ ") "
//					+ "| Group(MOVIE_DATES__GENRE) | Import ( frame = [ FRAME238470 ] ) ;" 
//
////				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) | Query(\"<encode> select * from movie_dates </encode>\") | Import ( frame = [ FRAME238470 ] ) ;" 
////				+ "FileRead( filePath=[\"$IF\\diabetes_____UNIQUE2019_08_14_15_20_44_0226.csv\" ], dataTypeMap=[{\"patient\":\"INT\",\"chol\":\"INT\",\"stab_glu\":\"INT\",\"hdl\":\"INT\",\"ratio\":\"DOUBLE\",\"glyhb\":\"DOUBLE\",\"location\":\"STRING\",\"age\":\"INT\",\"gender\":\"STRING\",\"height\":\"INT\",\"weight\":\"INT\",\"frame\":\"STRING\",\"bp_1s\":\"INT\",\"bp_1d\":\"INT\",\"bp_2s\":\"INT\",\"bp_2d\":\"INT\",\"waist\":\"INT\",\"hip\":\"INT\",\"time_ppn\":\"INT\",\"Drug\":\"STRING\",\"start_date\":\"DATE\",\"end_date\":\"DATE\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"diabetes.csv\"], additionalDataTypes=[{\"start_date\":\"M/d/yyyy\",\"end_date\":\"M/d/yyyy\"}])|Select(DND__patient, DND__chol, DND__stab_glu, DND__hdl, DND__ratio, DND__glyhb, DND__location, DND__age, DND__gender, DND__height, DND__weight, DND__frame, DND__bp_1s, DND__bp_1d, DND__bp_2s, DND__bp_2d, DND__waist, DND__hip, DND__time_ppn, DND__Drug, DND__start_date, DND__end_date).as([patient, chol, stab_glu, hdl, ratio, glyhb, location, age, gender, height, weight, frame, bp_1s, bp_1d, bp_2s, bp_2d, waist, hip, time_ppn, Drug, start_date, end_date])|Import( frame=[FRAME238470] );"
////				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;" 
////				+ "Frame ( frame = [ FRAME238470 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;" 
////				+ "FRAME238470 | Convert ( frameType = [ R ] ) .as ( [ 'FRAME238470' ] ) ;" 
////				+ "Frame ( frame = [ FRAME238470 ] ) | Select ( Cast_Formed , Director , DVD_Release , Genre , MovieBudget , MOVIE_DATES , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ) .as ( [ Cast_Formed , Director , DVD_Release , Genre , MovieBudget , MOVIE_DATES , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Cast_Formed\" , \"Director\" , \"DVD_Release\" , \"Genre\" , \"MovieBudget\" , \"MOVIE_DATES\" , \"Nominated\" , \"Production_End\" , \"Production_Start\" , \"Revenue_Domestic\" , \"Revenue_International\" , \"RottenTomatoes_Audience\" , \"RottenTomatoes_Critics\" , \"Studio\" , \"Theatre_Release_Date\" , \"Title\" ] } } } ) | Collect ( 2000 ) ;" 
////				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) | Select ( MOVIE_DATES , MOVIE_DATES__Production_Start , MOVIE_DATES__Revenue_International , MOVIE_DATES__RottenTomatoes_Audience ) .as ( [ MOVIE_DATES , Production_Start , Revenue_International , RottenTomatoes_Audience ] ) | Merge ( joins = [ ( MOVIE_DATES , inner.join , MOVIE_DATES ), ( x, inner.join, y ) ] , frame = [ FRAME238470 ] ) ;"
////				+ "FRAME238470 | Convert(frameType=['R']);"
////				+ "FRAME238470 | DateExpander ( column = [ \"Cast_Formed\" ] , options = [ \"year\" , \"month\" , \"month-name\" ] ) ;"
////				+ "Frame(FRAME238470) | QueryAll() | ToCsv();"
////				+ "RunSimilarity(instance=[\"Title\"], attributes=[\"Cast_Formed\",\"DVD_Release\"]);"
////				+ "DirectJDBCConnection(query = [\"<encode>select * from city</encode>\"], dbDriver = [\"MYSQL\"], connectionString = [\"jdbc:mysql://localhost:3306/world?user=root&password=password\"], username = [\"root\"], password = [\"password\"])|Import( frame=[FRAME238470] );"
//
////				+ "R(\"<encode>2+2</encode>\");"
//				//				+ "if(true, 5+5, 6+6);" 
////				+ "ifError ( ( Frame ( frame = [ FRAME238470 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ) , ( true ) ) ;"
//				;
//
////		expression =
////				"AddPanel ( panel = [ 0 ] , sheet = [ \"0\" ] ) ;\r\n" + 
////				"Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;\r\n" + 
////				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;\r\n" + 
////				"Panel ( 0 ) | RetrievePanelEvents ( ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Database ( database = [ \"3877fc71-6090-4e4b-9574-12db48d436d8\" ] ) | Select ( CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR , Sum ( CII_FACT_CLM_LINE__ACCT_PAID_AMT ) , CII_FACT_CLM_LINE__ACCT_ID , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) .as ( [ Month , MED_ACCT_PAID_AMT , Account , MBR_CVRG_TYPE_DESC ] ) | Join ( ( CII_FACT_CLM_LINE , inner.join , DIM_MBR_CVRG_TYPE ) ) | Group ( CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR , CII_FACT_CLM_LINE__ACCT_ID , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) | Filter ( ( CII_FACT_CLM_LINE__ACCT_ID == [ \"W0000238\" ] ) ) | Filter ( ( CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR >= [ 201909 ] ) ) | Filter ( ( CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR <= [ 202008 ] ) ) | Filter ( ( DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC == [ \"Medical\" ] ) ) | Import ( frame = [ CreateFrame ( frameType = [ R ] , override = [ true ] ) .as ( [ \"Snowflake_ACIISST_Claims_FRAME229886\" ] ) ] ) ;\r\n" + 
////				"Database ( database = [ \"ed711f71-67c3-4925-8423-4fbdc8c90b12\" ] ) | Select ( CII_FACT_MBRSHP__ACCT_ID , CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR , Sum ( CII_FACT_MBRSHP__MBR_CVRG_CNT ) , Sum ( CII_FACT_MBRSHP__SBSCRBR_CVRG_CNT ) , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) .as ( [ Account , Month , Medical_Members , Medical_Subscribers , MBR_CVRG_TYPE_DESC ] ) | Join ( ( CII_FACT_MBRSHP , inner.join , DIM_MBR_CVRG_TYPE ) ) | Group ( CII_FACT_MBRSHP__ACCT_ID , CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) | Filter ( ( CII_FACT_MBRSHP__ACCT_ID == [ \"W0000238\" ] ) ) | Filter ( ( CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR >= [ 201909 ] ) ) | Filter ( ( CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR <= [ 202008 ] ) ) | Filter ( ( DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC == [ \"Medical\" ] ) ) | Import ( frame = [ CreateFrame ( frameType = [ R ] , override = [ true ] ) .as ( [ \"Snowflake_ACIISST_MBR_FRAME902045\" ] ) ] ) ;\r\n" + 
////				"Frame ( Snowflake_ACIISST_Claims_FRAME229886 ) | QueryAll ( ) | Merge ( joins = [ ( Account , inner.join , Account ) , ( MBR_CVRG_TYPE_DESC , inner.join , MBR_CVRG_TYPE_DESC ) , ( Month , inner.join , Month ) ] , frame = [ Snowflake_ACIISST_MBR_FRAME902045 ] ) ;\r\n" + 
////				"Database ( database = [ \"3877fc71-6090-4e4b-9574-12db48d436d8\" ] ) | Select ( CII_FACT_CLM_LINE__ACCT_ID , CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR , Sum ( CII_FACT_CLM_LINE__ACCT_PAID_AMT ) , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) .as ( [ Account , Month , PHARMA_ACCT_PAID_AMT , MBR_CVRG_TYPE_DESC ] ) | Join ( ( CII_FACT_CLM_LINE , inner.join , DIM_MBR_CVRG_TYPE ) ) | Group ( CII_FACT_CLM_LINE__ACCT_ID , CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) | Filter ( ( CII_FACT_CLM_LINE__ACCT_ID == [ \"W0000238\" ] ) ) | Filter ( ( CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR >= [ 201909 ] ) ) | Filter ( ( CII_FACT_CLM_LINE__RPTG_PAID_YEAR_MNTH_NBR <= [ 202008 ] ) ) | Filter ( ( DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC == [ \"Pharmacy\" ] ) ) | Import ( frame = [ CreateFrame ( frameType = [ R ] , override = [ true ] ) .as ( [ \"Snowflake_ACIISST_Claims_FRAME389144\" ] ) ] ) ;\r\n" + 
////				"Database ( database = [ \"ed711f71-67c3-4925-8423-4fbdc8c90b12\" ] ) | Select ( CII_FACT_MBRSHP__ACCT_ID , Sum ( CII_FACT_MBRSHP__MBR_CVRG_CNT ) , Sum ( CII_FACT_MBRSHP__SBSCRBR_CVRG_CNT ) , CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) .as ( [ Account , Pharmacy_Members , Pharmacy_Subscribers , Month , MBR_CVRG_TYPE_DESC ] ) | Join ( ( CII_FACT_MBRSHP , inner.join , DIM_MBR_CVRG_TYPE ) ) | Group ( CII_FACT_MBRSHP__ACCT_ID , CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR , DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC ) | Filter ( ( CII_FACT_MBRSHP__ACCT_ID == [ \"W0000238\" ] ) ) | Filter ( ( CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR >= [ 201909 ] ) ) | Filter ( ( CII_FACT_MBRSHP__ELGBLTY_CY_MNTH_END_NBR <= [ 202008 ] ) ) | Filter ( ( DIM_MBR_CVRG_TYPE__MBR_CVRG_TYPE_DESC == [ \"Pharmacy\" ] ) ) | Import ( frame = [ CreateFrame ( frameType = [ R ] , override = [ true ] ) .as ( [ \"Snowflake_ACIISST_MBR_FRAME725834\" ] ) ] ) ;\r\n" + 
////				"Frame ( Snowflake_ACIISST_Claims_FRAME389144 ) | QueryAll ( ) | Merge ( joins = [ ( Account , inner.join , Account ) , ( MBR_CVRG_TYPE_DESC , inner.join , MBR_CVRG_TYPE_DESC ) , ( Month , inner.join , Month ) ] , frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME902045 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | AddPanelOrnaments ( { \"tools\" : { \"shared\" : { \"gridSpanRows\" : true } } } ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelSort ( columns = [ \"Account\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MBR_CVRG_TYPE_DESC\" , \"AGE_GRP_DESC\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Paid_PEPM\" , \"Pharmacy_Paid_PMPM\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Pharmacy\" ] , sort = [ \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" , \"asc\" ] ) ;\r\n" + 
////				"Panel ( 0 ) | AddPanelOrnaments ( { \"tools\" : { \"shared\" : { \"gridSpanRows\" : false } } } ) ;\r\n" + 
////				"Panel ( 0 ) | UnsortPanel ( ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"AddPanel ( panel = [ 1 ] , sheet = [ \"0\" ] ) ;\r\n" + 
////				"Panel ( 1 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;\r\n" + 
////				"Panel ( 1 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;\r\n" + 
////				"Panel ( 1 ) | RetrievePanelEvents ( ) ;\r\n" + 
////				"Panel ( 1 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"ClosePanel ( 1 ) ;\r\n" + 
////				"AddSheet ( \"1\" ) ;\r\n" + 
////				"Clone ( panel = [ \"0\" ] , cloneId = [ \"2\" ] , sheet = [ \"1\" ] ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Frame ( Snowflake_ACIISST_MBR_FRAME902045 ) | QueryAll ( ) | Merge ( joins = [ ( Account , left.outer.join , Account ) , ( Month , left.outer.join , Month ) ] , frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) ;\r\n" + 
////				"Snowflake_ACIISST_MBR_FRAME725834 | ReplaceColumnValue ( column = [ \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , value = [ \"null\" ] , newValue = [ \"0\" ] ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"2\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MED_ACCT_PAID_AMT , Month , Medical_Members , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MED_ACCT_PAID_AMT , Month , Medical_Members , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ Month , MED_ACCT_PAID_AMT , Medical_Members , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( MED_ACCT_PAID_AMT / Medical_Members ) ) .as ( [ Medical_Paid_PMPM ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Medical_Paid_PMPM ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Medical_Paid_PMPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Medical_Paid_PMPM\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( MED_ACCT_PAID_AMT / Medical_Subscribers ) ) .as ( [ Medical_Paid_PEPM ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Medical_Paid_PEPM ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Medical_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Medical_Paid_PEPM\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( PHARMA_ACCT_PAID_AMT / Pharmacy_Members ) ) .as ( [ Pharma_Paid_PMPM ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( PHARMA_ACCT_PAID_AMT / Pharmacy_Subscribers ) ) .as ( [ Pharma_Paid_PEPM ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , MED_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , MED_ACCT_PAID_AMT , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"MED_ACCT_PAID_AMT\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , MED_ACCT_PAID_AMT , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , MED_ACCT_PAID_AMT , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"MED_ACCT_PAID_AMT\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , MED_ACCT_PAID_AMT , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , MED_ACCT_PAID_AMT , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"MED_ACCT_PAID_AMT\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PMPM , Pharma_Paid_PEPM ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PMPM , Pharma_Paid_PEPM ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Snowflake_ACIISST_MBR_FRAME725834 | ReplaceColumnValue ( column = [ \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" ] , value = [ \"null\" ] , newValue = [ \"0\" ] ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"2\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Month , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ Medical_Members , Month , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Month\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ) .as ( [ Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( Medical_Members * Medical_Paid_PMPM ) ) .as ( [ Total_Medical ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( Pharmacy_Members * Pharma_Paid_PMPM ) ) .as ( [ Total_Pharmacy ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( Total_Medical + Total_Pharmacy ) ) .as ( [ Total_Paid_Amount ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PMPM , Pharma_Paid_PEPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharmacy_Members , Pharmacy_Subscribers , Pharma_Paid_PMPM , Pharma_Paid_PEPM , Total_Medical , Total_Pharmacy , Total_Paid_Amount ] ) | With ( Panel ( 2 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"2\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" , \"Total_Medical\" , \"Total_Pharmacy\" , \"Total_Paid_Amount\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"AddSheet ( \"2\" ) ;\r\n" + 
////				"Clone ( panel = [ \"2\" ] , cloneId = [ \"3\" ] , sheet = [ \"2\" ] ) ;\r\n" + 
////				"ClosePanel ( 0 ) ;\r\n" + 
////				"CloseSheet ( \"0\" ) ;\r\n" + 
////				"Panel ( 2 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 3 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"CloseSheet ( \"0\" ) ;\r\n" + 
////				"Panel ( 3 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"3\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PEPM , Medical_Paid_PMPM , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Subscribers , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Subscribers\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PMPM , Pharma_Paid_PEPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Month , Medical_Members , Medical_Subscribers , Pharmacy_Members , Pharmacy_Subscribers , Medical_Paid_PMPM , Medical_Paid_PEPM , Pharma_Paid_PMPM , Pharma_Paid_PEPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Month\" , \"Medical_Members\" , \"Medical_Subscribers\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"Medical_Paid_PMPM\" , \"Medical_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharma_Paid_PEPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"META | SetInsightConfig ( { \"panels\" : { \"2\" : { \"config\" : { \"type\" : \"golden\" , \"backgroundColor\" : \"\" , \"opacity\" : 100 } } , \"3\" : { \"config\" : { \"type\" : \"golden\" , \"backgroundColor\" : \"\" , \"opacity\" : 100 } } } , \"sheets\" : { \"1\" : { \"golden\" : { \"content\" : [ { \"type\" : \"row\" , \"content\" : [ { \"type\" : \"stack\" , \"activeItemIndex\" : 0 , \"width\" : 100 , \"content\" : [ { \"type\" : \"component\" , \"componentName\" : \"panel\" , \"componentState\" : { \"panelId\" : \"2\" } } ] } ] } ] } } , \"2\" : { \"golden\" : { \"content\" : [ { \"type\" : \"row\" , \"content\" : [ { \"type\" : \"stack\" , \"activeItemIndex\" : 0 , \"width\" : 100 , \"content\" : [ { \"type\" : \"component\" , \"componentName\" : \"panel\" , \"componentState\" : { \"panelId\" : \"3\" } } ] } ] } ] } } } , \"sheet\" : \"2\" } ) ;\r\n" + 
////				"META | UpdateInsight ( app = [ \"3877fc71-6090-4e4b-9574-12db48d436d8\" ] , insightName = [ \"F01 Offshore 1029\" ] , hidden = [ false ] , layout = [ \"\" ] , id = [ \"1546c708-5b70-4bfc-bc5b-68b74c129462\" ] ) ;\r\n" + 
////				"Panel ( 3 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"Panel ( 3 ) | SetPanelView ( \"visualization\" ) ;\r\n" + 
////				"META | SetInsightConfig ( { \"panels\" : { \"2\" : { \"config\" : { \"type\" : \"golden\" , \"backgroundColor\" : \"\" , \"opacity\" : 100 } } , \"3\" : { \"config\" : { \"type\" : \"golden\" , \"backgroundColor\" : \"\" , \"opacity\" : 100 } } } , \"sheets\" : { \"1\" : { \"golden\" : { \"content\" : [ { \"type\" : \"row\" , \"content\" : [ { \"type\" : \"stack\" , \"activeItemIndex\" : 0 , \"width\" : 100 , \"content\" : [ { \"type\" : \"component\" , \"componentName\" : \"panel\" , \"componentState\" : { \"panelId\" : \"2\" } } ] } ] } ] } } , \"2\" : { \"golden\" : { \"content\" : [ { \"type\" : \"row\" , \"content\" : [ { \"type\" : \"stack\" , \"activeItemIndex\" : 0 , \"width\" : 100 , \"content\" : [ { \"type\" : \"component\" , \"componentName\" : \"panel\" , \"componentState\" : { \"panelId\" : \"3\" } } ] } ] } ] } } } , \"sheet\" : \"2\" , \"presentation\" : true } ) ;\r\n" + 
////				"META | SaveInsight ( app = [ \"3877fc71-6090-4e4b-9574-12db48d436d8\" ] , insightName = [ \"F01 Offshore QTR 1029\" ] , hidden = [ false ] , layout = [ \"\" ] ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Snowflake_ACIISST_MBR_FRAME725834 | DuplicateColumn ( column = [ \"Month\" ] , newCol = [ \"Month2\" ] ) ;\r\n" + 
////				"ifError ( ( Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"3\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ) , ( true ) ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( Month2 / 100 ) ) .as ( [ Month3 ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy , Month3 ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy , Month3 ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Month2\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" , \"Month3\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Month2\" , \"Month3\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Snowflake_ACIISST_MBR_FRAME725834 | ChangeColumnType ( column = [ Month3 ] , dataType = [ \"STRING\" ] ) ;\r\n" + 
////				"ifError ( ( Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"3\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ) , ( true ) ) ;\r\n" + 
////				"Snowflake_ACIISST_MBR_FRAME725834 | SplitColumns ( columns = [ \"Month3\" ] , delimiter = [ \".\" ] , search = [ \"Exact Match\" ] ) ;\r\n" + 
////				"ifError ( ( Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"3\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ) , ( true ) ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Month3_1 , Month3_2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Month3_1 , Month3_2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Month2\" , \"Month3\" , \"Month3_1\" , \"Month3_2\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( If ( Month3_2 == ( '1' , '2' , '3' ) , \"Q1\" , If ( Month3_2 == ( '4' , '5' , '6' ) , \"Q2\" , If ( Month3_2 == ( '7' , '8' , '9' ) , \"Q3\" , \"Q4\" ) ) ) ) ) .as ( [ QTR ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Month3_1 , Month3_2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy , QTR ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Month3_1 , Month3_2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy , QTR ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Month2\" , \"Month3\" , \"Month3_1\" , \"Month3_2\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" , \"QTR\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( ( If ( Month3_2 == [ '1' , '2' , '3' ] , \"Q1\" , If ( Month3_2 == [ '4' , '5' , '6' ] , \"Q2\" , If ( Month3_2 == [ '7' , '8' , '9' ] , \"Q3\" , \"Q4\" ) ) ) ) ) .as ( [ QTR ] ) | CollectNewCol ( ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Month3_1 , Month3_2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy , QTR ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , MED_ACCT_PAID_AMT , Month , Month2 , Month3 , Month3_1 , Month3_2 , Pharmacy_Members , Pharmacy_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Total_Medical , Total_Paid_Amount , Total_Pharmacy , QTR ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"MED_ACCT_PAID_AMT\" , \"Month\" , \"Month2\" , \"Month3\" , \"Month3_1\" , \"Month3_2\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" , \"QTR\" ] } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Snowflake_ACIISST_MBR_FRAME725834 | JoinColumns ( newCol = [ \"QTR2\" ] , delimiter = [ \"_\" ] , columns = [ \"QTR\" , \"Month3_1\" ] ) ;\r\n" + 
////				"ifError ( ( Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"3\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ) , ( true ) ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month2 , Month3 , Month3_1 , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month2 , Month3 , Month3_1 , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Month2\" , \"Month3\" , \"Month3_1\" , \"Month3_2\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"QTR2\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month3 , Month3_1 , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month3 , Month3_1 , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Month3\" , \"Month3_1\" , \"Month3_2\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"QTR2\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month3_1 , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month3_1 , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Month3_1\" , \"Month3_2\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"QTR2\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , Month , Month3_2 , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"Month\" , \"Month3_2\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"QTR2\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"QTR2\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ;\r\n" + 
////				"if ( ( Snowflake_ACIISST_MBR_FRAME725834 | HasDuplicates ( QTR2 ) ) , ( ( Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Count ( Account ) , Count ( MBR_CVRG_TYPE_DESC ) , Count ( MBR_CVRG_TYPE_DESC_1 ) , Average ( MED_ACCT_PAID_AMT ) , Average ( Medical_Members ) , Average ( Medical_Paid_PEPM ) , Average ( Medical_Paid_PMPM ) , Average ( Medical_Subscribers ) , Average ( PHARMA_ACCT_PAID_AMT ) , Average ( Pharma_Paid_PEPM ) , Average ( Pharma_Paid_PMPM ) , Average ( Pharmacy_Members ) , Average ( Pharmacy_Subscribers ) , Count ( QTR2 ) , Average ( Total_Medical ) , Average ( Total_Paid_Amount ) , Average ( Total_Pharmacy ) , QTR2 ) .as ( [ Count_of_Account , Count_of_MBR_CVRG_TYPE_DESC , Count_of_MBR_CVRG_TYPE_DESC_1 , Average_of_MED_ACCT_PAID_AMT , Average_of_Medical_Members , Average_of_Medical_Paid_PEPM , Average_of_Medical_Paid_PMPM , Average_of_Medical_Subscribers , Average_of_PHARMA_ACCT_PAID_AMT , Average_of_Pharma_Paid_PEPM , Average_of_Pharma_Paid_PMPM , Average_of_Pharmacy_Members , Average_of_Pharmacy_Subscribers , Count_of_QTR2 , Average_of_Total_Medical , Average_of_Total_Paid_Amount , Average_of_Total_Pharmacy , QTR2 ] ) | Group ( QTR2 ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Count_of_Account\" , \"Count_of_MBR_CVRG_TYPE_DESC\" , \"Count_of_MBR_CVRG_TYPE_DESC_1\" , \"Average_of_MED_ACCT_PAID_AMT\" , \"Average_of_Medical_Members\" , \"Average_of_Medical_Paid_PEPM\" , \"Average_of_Medical_Paid_PMPM\" , \"Average_of_Medical_Subscribers\" , \"Average_of_PHARMA_ACCT_PAID_AMT\" , \"Average_of_Pharma_Paid_PEPM\" , \"Average_of_Pharma_Paid_PMPM\" , \"Average_of_Pharmacy_Members\" , \"Average_of_Pharmacy_Subscribers\" , \"Count_of_QTR2\" , \"Average_of_Total_Medical\" , \"Average_of_Total_Paid_Amount\" , \"Average_of_Total_Pharmacy\" ] , \"validate\" : [ \"QTR2\" ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ) ) , ( ( Frame ( frame = [ Snowflake_ACIISST_MBR_FRAME725834 ] ) | Select ( Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ) .as ( [ Account , MBR_CVRG_TYPE_DESC , MBR_CVRG_TYPE_DESC_1 , MED_ACCT_PAID_AMT , Medical_Members , Medical_Paid_PEPM , Medical_Paid_PMPM , Medical_Subscribers , PHARMA_ACCT_PAID_AMT , Pharma_Paid_PEPM , Pharma_Paid_PMPM , Pharmacy_Members , Pharmacy_Subscribers , QTR2 , Total_Medical , Total_Paid_Amount , Total_Pharmacy ] ) | With ( Panel ( 3 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"3\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Account\" , \"MBR_CVRG_TYPE_DESC\" , \"MBR_CVRG_TYPE_DESC_1\" , \"MED_ACCT_PAID_AMT\" , \"Medical_Members\" , \"Medical_Paid_PEPM\" , \"Medical_Paid_PMPM\" , \"Medical_Subscribers\" , \"PHARMA_ACCT_PAID_AMT\" , \"Pharma_Paid_PEPM\" , \"Pharma_Paid_PMPM\" , \"Pharmacy_Members\" , \"Pharmacy_Subscribers\" , \"QTR2\" , \"Total_Medical\" , \"Total_Paid_Amount\" , \"Total_Pharmacy\" ] , \"validate\" : [ \"QTR2\" ] } , \"layer\" : { \"id\" : \"0\" , \"name\" : \"Layer 0\" , \"addYAxis\" : true , \"addXAxis\" : true , \"z\" : 2 , \"base\" : true } } } ) | Collect ( 2000 ) ) ) ) ;\r\n" + 
////				"Panel ( 3 ) | SetPanelView ( \"pipeline\" ) ;\r\n" + 
////				"META | SetInsightConfig({\"panels\":{\"2\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}},\"3\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"1\":{\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"2\"}}]}]}]}},\"2\":{\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"3\"}}]}]}]}}},\"sheet\":\"2\"});\r\n" + 
////				"";
//		
//		
//		Insight in = new Insight();
////		NounMetadata rFrameNoun = new NounMetadata(new RDataTable("Snowflake_ACIISST_MBR_FRAME725834"), PixelDataType.FRAME);
//		in.getVarStore().put("FRAME238470", new NounMetadata(new H2Frame("FRAME238470"), PixelDataType.FRAME));
////		in.getVarStore().put("Snowflake_ACIISST_MBR_FRAME725834", rFrameNoun);
////		in.setDataMaker((IDataMaker) rFrameNoun.getValue());
//		List<String> encodingList = new Vector<>();
//		Map<String, String> encodedTextToOriginal = new HashMap<>();
//		
//		PipelineTranslation translation = new PipelineTranslation(in);
//		
//		List<String> breakdown = PixelUtility.parsePixel(expression);
//		for(String pixel : breakdown) {
//			try {
//				pixel = PixelPreProcessor.preProcessPixel(pixel.trim(), encodingList, encodedTextToOriginal);
//				System.out.println("Running pixel = " + pixel);
//				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
//	
//				// parsing the pixel - this process also determines if expression is syntactically correct
//				Start tree = p.parse();
//				// apply the translation.
//				tree.apply(translation);
//			} catch(SemossPixelException e) {
//				if(!e.isContinueThreadOfExecution()) {
//					throw e;
//				}
//			} catch (ParserException | LexerException | IOException e) {
//				// we only need to catch invalid syntax here
//				// other exceptions are caught in lazy translation
//				logger.error(Constants.STACKTRACE, e);
//				String eMessage = e.getMessage();
//				if(eMessage.startsWith("[")) {
//					Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
//					Matcher matcher = pattern.matcher(eMessage);
//					if(matcher.find()) {
//						String location = matcher.group(0);
//						location = location.substring(1, location.length()-1);
//						int findIndex = Integer.parseInt(location.split(",")[1]);
//						eMessage += ". Error in syntax around " + pixel.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, pixel.length())).trim();
//					}
//				}
//				logger.info(eMessage);
//			}
//		}
//		
//		Gson gson = GsonUtility.getDefaultGson(true);
//		if (translation != null) {
//			for(int i = 0; i < translation.allRoutines.size(); i++) {
//				if(i == 147) {
//					System.out.println("breaking");
//				}
//				for(int j = 0; j < translation.allRoutines.get(i).size(); j++) {
//					System.out.println(i + "." + j);
//					PipelineOperation op = translation.allRoutines.get(i).get(j);
//					System.out.println(gson.toJson(op));
//					System.out.println(i + "." + j);
//				}
//			}
//			
////			System.out.println(">>>>>>>>>>>>>");
////			System.out.println(">>>>>>>>>>>>>");
////			System.out.println(">>>>>>>>>>>>>");
////			System.out.println(">>>>>>>>>>>>>");
////			System.out.println(">>>>>>>>>>>>>");
////			System.out.println(">>>>>>>>>>>>>");
////			System.out.println(">>>>>>>>>>>>>");
////
////			System.out.println(gson.toJson(translation.allRoutines));
//		}
//	}
}

