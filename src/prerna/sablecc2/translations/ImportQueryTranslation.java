package prerna.sablecc2.translations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.IReactor;
import prerna.reactor.imports.ImportReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class ImportQueryTranslation extends LazyTranslation {
	
	private static final Logger logger = LogManager.getLogger(ImportQueryTranslation.class);

	private Pixel pixelObj = null;
	private Map<Pixel, SelectQueryStruct> importQsMap = new HashMap<>();
	
	public ImportQueryTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(int i = 0; i < copy.size(); i++) {
			PRoutine e = copy.get(i);
			String expression = e.toString();
			if(expression.contains("Import")) {
        		this.resultKey = "$RESULT_" + e.hashCode();

				logger.info("Processing " + Utility.cleanLogString(expression));
				e.apply(this);
			}
		}
	}
	
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
    		
        	// need to find imports
        	if(prevReactor != null && (prevReactor instanceof ImportReactor)) {
    			SelectQueryStruct importQs = (SelectQueryStruct) prevReactor.getNounStore().getNoun(PixelDataType.QUERY_STRUCT.getKey()).get(0);
    			importQsMap.put(this.pixelObj, importQs);
        	}
    	}
    }
	
    /////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////
    
	public void setPixelObj(Pixel pixelObj) {
		this.pixelObj = pixelObj;
	}
	
	/**
	 * Get the import qs in the recipe
	 * @return
	 */
	public Map<Pixel, SelectQueryStruct> getImportQsMap() {
		return importQsMap;
	}
	
	/**
	 * Testing method
	 * @param args
	 */
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper("C:\\workspace3\\Semoss_Dev\\RDF_Map.prop");
//		Gson gson = new GsonBuilder()
//				.disableHtmlEscaping()
//				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
//				.setPrettyPrinting()
//				.create();
//		
//		String[] recipe = new String[]{ 
//				"AddPanel ( 0 ) ; ",
//				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"UnfilterFrame(%3CSelectedColumn%3E)%3B\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"if(IsEmpty(%3CSelectedValues%3E)%2C%20UnfilterFrame(%3CSelectedColumn%3E)%2C%20SetFrameFilter(%3CSelectedColumn%3E%3D%3D%3CSelectedValues%3E))%3B\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ; ",
//				"Panel ( 0 ) | RetrievePanelEvents ( ) ; ",
//				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"%7B%22type%22%3A%22echarts%22%7D\" ) ; ",
//				"Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"%7B%22core_engine%22%3A%22NEWSEMOSSAPP%22%7D\" ) ; ",
//				"Database ( database = [ \"995cf169-6b44-4a42-b75c-af12f9f45c36\" ] ) "
//						+ "| Select ( DIABETES2__AGE, DIABETES2__TEST) .as ( [ AGE, TEST ] ) "
//						+ "| Filter ( ( ( DIABETES2__AGE > [ 20 ] )  OR  ( ( DIABETES2__BP_2S != [ null ] )  AND  ( DIABETES2__BP_2D != [ null ] ) ) ) ) "
//						+ "| Import ( frame = [ CreateFrame ( frameType = [ PY ] , override = [ true ] ) .as ( [ \"Diabetes_FRAME224822\" ] ) ] ) ;", 
//				"Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ; "
//		};
//		
//		Insight in = new Insight();
//
//		int counter = 0;
//		ImportQueryTranslation translation = new ImportQueryTranslation(in);
//		// loop through recipe
//		for(String expression : recipe) {
//			try {
//				translation.setPixelObj(new Pixel("" + (counter++), expression));
//				expression = PixelPreProcessor.preProcessPixel(expression.trim(), new ArrayList<String>(), new HashMap<String, String>());
//				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
//				// parsing the pixel - this process also determines if expression is syntactically correct
//				Start tree = p.parse();
//				// apply the translation.
//				tree.apply(translation);
//			} catch (ParserException | LexerException | IOException e) {
//				e.printStackTrace();
//			}
//		}
//		
//		System.out.println(gson.toJson(translation.getImportQsMap()));
//	}

}
