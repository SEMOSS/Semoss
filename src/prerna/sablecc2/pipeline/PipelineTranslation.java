package prerna.sablecc2.pipeline;

import java.io.ByteArrayInputStream;
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
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.frame.FrameFactory;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.gson.GsonUtility;

public class PipelineTranslation extends LazyTranslation {

	private static final Logger LOGGER = LogManager.getLogger(PipelineTranslation.class.getName());
	private static Map<String, String> reactorToId = null;
	
	private static Map<AbstractQueryStruct.QUERY_STRUCT_TYPE, String> qsToWidget = new HashMap<AbstractQueryStruct.QUERY_STRUCT_TYPE, String>();
	static {
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE, "pipeline-app");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE, "pipeline-file");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME, "pipeline-frame");
//		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.LAMBDA, value);
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, "pipeline-query");
//		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_FRAME_QUERY, "pipeline-query");
		qsToWidget.put(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY, "pipeline-external");
	}
	
	private static List<String> qsReactors = new Vector<String>();
	static {
		qsReactors.add("Import");
		qsReactors.add("Merge");
		qsReactors.add("FuzzyMerge");
	}
	
	private List<List<PipelineOperation>> allRoutines = new Vector<List<PipelineOperation>>();
	private List<PipelineOperation> curRoutine;
	
	public PipelineTranslation(Insight insight) {
		super(insight);
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
		
		if(PipelineTranslation.qsReactors.contains(reactorId)) {
			// we need to merge the previous QS portions into this reactor
			combineQSComponents(op);
		} else if(PipelineTranslation.reactorToId.containsKey(reactorId)) {
			op.setWidgetId(PipelineTranslation.reactorToId.get(reactorId));
		}
		
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
		
		return op;
    }
    
    private void combineQSComponents(PipelineOperation op) {
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
    			Map<String, Object> dbMap = (Map<String, Object>) routine.getNounInputs().get("database");
    			if(dbMap == null) {
    				dbMap = (Map<String, Object>) routine.getRowInputs().get(0);
    			}
    			
    			if(dbMap != null) {
        			String dbId = (String) dbMap.get("value");
        			qs.setEngineId(dbId);
    			}
    			
    		} else if(opName.equals("Select")) {
    			List<Object> rowInputs = routine.getRowInputs();
    			int size = rowInputs.size();
    			
    			for(int i = 0; i < size; i++) {
    				// the value should already be a query column selector
    				Map<String, Object> rowMap = (Map<String, Object>) rowInputs.get(i);
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
    			
    			// reset reference of qs to the new hard qs after merging the inputs
    			qs = newQs;
    		}
    	}
    	
    	op.addNounInputs("qs", qs);
    	op.setWidgetId(PipelineTranslation.qsToWidget.get(qs.getQsType()));
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
		if(reactorToId == null) {
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			CSVFileHelper helper = new CSVFileHelper();
			helper.parse(baseFolder + "/reactorToWidget.csv");
			
			reactorToId = new HashMap<String, String>();
			
			String[] row = null;
			while( (row = helper.getNextRow()) != null ) {
				reactorToId.put(row[0], row[1]);
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

		TestUtilityMethods.loadDIHelper("C:\\workspace2\\Semoss_Dev\\RDF_Map.prop");
		
		String pixel = "" 
//				+ "AddPanel ( 0 ) ;" 
//				+ "Panel ( 0 ) | AddPanelConfig ( config = [ { \"config\" : { \"type\" : \"STANDARD\" , \"opacity\" : 100 } } ] ) ;" 
//				+ "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;" + 
//				"Panel ( 0 ) | RetrievePanelEvents ( ) ;" 
//				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;" 
//				+ "Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"<encode>{\"app_id\":\"NEWSEMOSSAPP\"}</encode>\" ) ;" 
//				+ "CreateFrame ( frameType = [ GRID ] ) .as ( [ 'FRAME238470' ] ) ;" 
				+ "Database ( database = [ \"f77ba49e-a8a3-41bd-94c5-91d0a3103bbb\" ] ) | Select ( MOVIE_DATES , MOVIE_DATES__Cast_Formed , MOVIE_DATES__DVD_Release , MOVIE_DATES__Director , MOVIE_DATES__Genre , MOVIE_DATES__MovieBudget , MOVIE_DATES__Nominated , MOVIE_DATES__Production_End , MOVIE_DATES__Production_Start , MOVIE_DATES__Revenue_Domestic , MOVIE_DATES__Revenue_International , MOVIE_DATES__RottenTomatoes_Audience , MOVIE_DATES__RottenTomatoes_Critics , MOVIE_DATES__Studio , MOVIE_DATES__Theatre_Release_Date , MOVIE_DATES__Title ) .as ( [ MOVIE_DATES , Cast_Formed , DVD_Release , Director , Genre , MovieBudget , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ] ) | Import ( frame = [ FRAME238470 ] ) ;" 
//				+ "Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;" 
//				+ "Frame ( frame = [ FRAME238470 ] ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 2000 ) ;" 
//				+ "FRAME238470 | Convert ( frameType = [ R ] ) .as ( [ 'FRAME238470' ] ) ;" 
//				+ "Frame ( frame = [ FRAME238470 ] ) | Select ( Cast_Formed , Director , DVD_Release , Genre , MovieBudget , MOVIE_DATES , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ) .as ( [ Cast_Formed , Director , DVD_Release , Genre , MovieBudget , MOVIE_DATES , Nominated , Production_End , Production_Start , Revenue_Domestic , Revenue_International , RottenTomatoes_Audience , RottenTomatoes_Critics , Studio , Theatre_Release_Date , Title ] ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Cast_Formed\" , \"Director\" , \"DVD_Release\" , \"Genre\" , \"MovieBudget\" , \"MOVIE_DATES\" , \"Nominated\" , \"Production_End\" , \"Production_Start\" , \"Revenue_Domestic\" , \"Revenue_International\" , \"RottenTomatoes_Audience\" , \"RottenTomatoes_Critics\" , \"Studio\" , \"Theatre_Release_Date\" , \"Title\" ] } } } ) | Collect ( 2000 ) ;" 
				+ "FRAME238470 | DateExpander ( column = [ \"Cast_Formed\" ] , options = [ \"year\" , \"month\" , \"month-name\" ] ) ;"
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

