package prerna.sablecc2.translations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QsFilterParameterizeConverter;
import prerna.query.querystruct.transform.QsToPixelConverter;
import prerna.reactor.IReactor;
import prerna.reactor.imports.ImportReactor;
import prerna.reactor.imports.MergeReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.reactor.qs.source.FileReadReactor;
import prerna.reactor.qs.source.FrameReactor;
import prerna.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

@Deprecated
public class ParameterizeSaveRecipeTranslation extends LazyTranslation {
	
	private static final Logger logger = LogManager.getLogger(ParameterizeSaveRecipeTranslation.class);

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<String>();
	
	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public List<String> encodingList = new Vector<String>();
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();
	
	// set the parameters we care about
	public List<String> inputsToParameterize;
	
	private SelectQueryStruct importQs;
	private String sourceStr;
	private String importStr;
	private Map<String, Map<String, String>> paramToSource = new HashMap<>();
	private Map<String, List<String>> colToComparators = new HashMap<>();

	public ParameterizeSaveRecipeTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
			String expression = e.toString();
			if(expression.contains("Import")) {
        		this.resultKey = "$RESULT_" + e.hashCode();

				logger.info("Processing " + Utility.cleanLogString(expression));
				e.apply(this);

				// check if we have a QS to modify
				if(this.importQs != null) {
					
					// first, need to see if this import requires
					// the filtering
					
					// we will do 2 things
					// look at the selectors
					// and then look at the filters
					
					List<String> foundParams = new Vector<String>();
					List<String> filterQsName = new Vector<String>();
					
					List<IQuerySelector> selectors = importQs.getSelectors();
					for(IQuerySelector select : selectors) {
						for(String colToParam : inputsToParameterize) {
							String alias = select.getAlias();
							String qsName = select.getQueryStructName();
							
							if(colToParam.equals(alias)) {
								foundParams.add(alias);
								filterQsName.add(qsName);
								
								// store the first match
								// for the given param
								if(!this.paramToSource.containsKey(alias)) {
									Map<String, String> inner = new HashMap<String, String>();
									inner.put("qs", qsName);
									inner.put("source", this.sourceStr);
									this.paramToSource.put(alias, inner);
								}
							}
						}
					}
					
					List<IQueryFilter> newFilters = new Vector<>();
					List<String> modifiedParams = new Vector<>();

					// you can have a filter
					// that is not a selector
					// so lets look at filters as well
					List<IQueryFilter> currentImportFilters = importQs.getExplicitFilters().getFilters();
					for(IQueryFilter f : currentImportFilters) {
						IQueryFilter modification = f;
						// i will want to run through EVERY column
						// for the same filter
						// in case it contains it
						for(String colToParam : inputsToParameterize) {
							if(modification.containsColumn(colToParam)) {
								modifiedParams.add(colToParam);
								// find all the smallest parts that are using this
								modification = QsFilterParameterizeConverter.modifyFilter(modification, colToParam, colToComparators);
								
								// also add the param to source
								if(!this.paramToSource.containsKey(colToParam)) {
									List<String> qsNames = new Vector<>();
									QsFilterParameterizeConverter.findSelectorsForAlias(modification, colToParam, qsNames);
									Map<String, String> inner = new HashMap<String, String>();
									inner.put("qs", qsNames.get(0));
									inner.put("source", this.sourceStr);
									this.paramToSource.put(colToParam, inner);
								}
							}
						}
						
						// add the modified filter if it was changed
						newFilters.add(modification);
					}
					
					// if no matches
					// nothing to do
					if(foundParams.isEmpty()) {
						this.pixels.add(expression);
						continue;
					}
					
					// FE prefers a single AND block
					// so adding all the current filters
					// note - this maybe 0 or more
					AndQueryFilter andFilter = new AndQueryFilter();
					andFilter.addFilter(newFilters);
					
					int numParams = foundParams.size();
					for(int paramColIndex = 0; paramColIndex < numParams; paramColIndex++) {
						String colToParam = foundParams.get(paramColIndex);
						if(!modifiedParams.contains(colToParam)) {
							// add new filters
							String qsColName = filterQsName.get(paramColIndex);
							String paramName = foundParams.get(paramColIndex);
							SimpleQueryFilter paramF = new SimpleQueryFilter(
									new NounMetadata(new QueryColumnSelector(qsColName), PixelDataType.COLUMN), 
									"==", 
									new NounMetadata("<" + paramName + "__eq>", PixelDataType.CONST_STRING)
									);
							
							// add these filters into the AND
							andFilter.addFilter(paramF);
							
							// also store this
							List<String> colComparators = null;
							if(colToComparators.containsKey(paramName)) {
								colComparators = colToComparators.get(paramName);
							} else {
								colComparators = new Vector<>();
								colToComparators.put(paramName, colComparators);
							}
							logger.info("Adding new filter for column = " + paramName);
							colComparators.add("==");
						}
					}
					
					// swap the filter lists
					currentImportFilters.clear();
					// if only 1 value in the AND block
					// just grab it and send that filter
					if(andFilter.getFilterList().size() == 1) {
						currentImportFilters.add(andFilter.getFilterList().get(0));
					} else {
						currentImportFilters.add(andFilter);
					}
					
					String newExpr = sourceStr + "|" + QsToPixelConverter.getPixel(this.importQs, false) + " | " + this.importStr + " ;";
					this.pixels.add(newExpr);
					
					// reset
					this.importQs = null;
					this.sourceStr = null;
					this.importStr = null;
				} else {
					this.pixels.add(expression);	
				}
			} else {
				// add to list of expressions
				expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
				this.pixels.add(expression);
			}
		}
	}
	
	/*
	 * The way this works is
	 * We will go through the normal reactor
	 * Once we have a QueryStruct reactor type
	 * We process until we get to an import
	 * Then I use the QueryStruct to generate the pixel
	 */
	
	@Override
	public void inAOperation(AOperation node) {
		super.inAOperation(node);
		
		if(this.curReactor instanceof DatabaseReactor || this.curReactor instanceof FileReadReactor
				|| this.curReactor instanceof GoogleFileRetrieverReactor || this.curReactor instanceof FrameReactor) {
			this.sourceStr = node.toString().trim();
		}
		else if(this.curReactor instanceof ImportReactor || this.curReactor instanceof MergeReactor) {
			this.importStr = node.toString().trim();
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
        	if(prevReactor != null && (prevReactor instanceof ImportReactor || prevReactor instanceof MergeReactor)) {
    			importQs = (SelectQueryStruct) prevReactor.getNounStore().getNoun(PixelDataType.QUERY_STRUCT.getKey()).get(0);
    		}
    	}
    }
	
    /////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////
    
	/**
	 * Get the new pixels
	 * @return
	 */
	public List<String> getPixels() {
		return this.pixels;
	}
	
	public Map<String, List<String>> getColToComparators() {
		return colToComparators;
	}
	
	public Map<String, Map<String, String>> getParamToSource() {
		return this.paramToSource;
	}
	
	public void setInputsToParameterize(List<String> inputsToParameterize) {
		this.inputsToParameterize = inputsToParameterize;
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
//		List<String> params = new Vector<String>();
//		params.add("AGE");
//		params.add("BP_2S");
//		params.add("TEST");
//
//		Insight in = new Insight();
//		ParameterizeSaveRecipeTranslation translation = new ParameterizeSaveRecipeTranslation(in);
//		translation.setInputsToParameterize(params);
//		
//		// loop through recipe
//		for(String expression : recipe) {
//			try {
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
//		System.out.println(gson.toJson(translation.getPixels()));
//		System.out.println(gson.toJson(translation.getParamToSource()));
//	}

}
