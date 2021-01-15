package prerna.sablecc2.translations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStruct.FILL_TYPE;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.ParamStructDetails.LEVEL;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QsFilterParameterizeConverter2;
import prerna.query.querystruct.transform.QsToPixelConverter;
import prerna.sablecc2.LazyTranslation;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.imports.ImportReactor;
import prerna.sablecc2.reactor.imports.MergeReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.qs.source.FileReadReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class ParamStructSaveRecipeTranslation extends LazyTranslation {
	
	private static final Logger logger = LogManager.getLogger(ParamStructSaveRecipeTranslation.class);

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<>();
	
	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public List<String> encodingList = new Vector<>();
	public HashMap<String, String> encodedToOriginal = new HashMap<>();
	
	// set the parameters we care about
	public List<ParamStruct> paramStructs;
	
	private SelectQueryStruct importQs;
	private String sourceStr;
	private String importStr;
	
	public String currentPixelId = "";
	
	public ParamStructSaveRecipeTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		ROUTINE_LOOP : for(PRoutine e : copy) {
			String expression = e.toString();
			if(expression.contains("Import")) {
        		this.resultKey = "$RESULT_" + e.hashCode();

				logger.info("Processing " + Utility.cleanLogString(expression));
				e.apply(this);

				// check if we have a QS to modify
				if(this.importQs == null) {
					// just store
					// add to list of expressions
					expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
					this.pixels.add(expression);
				} else {
					// we have a QS
					
					// first, need to see if this import requires
					// loop through all the params
					// and see if this pixelId is included
					// and which params are required to be applied
					List<ParamStructDetails> thisImportParams = new Vector<>();
					Map<ParamStructDetails, ParamStruct> detailsLookup = new HashMap<>();
					for(ParamStruct struct  : this.paramStructs) {
						for(ParamStructDetails details : struct.getDetailsList()) {
							if(details.getPixelId().equals(this.currentPixelId)) {
								// store this
								thisImportParams.add(details);
								detailsLookup.put(details, struct);
							}
						}
					}
					
					// if no matches
					// nothing to do
					if(thisImportParams.isEmpty()) {
						this.pixels.add(expression);
						continue;
					}
					
					// we have something
					// now we need to see
					// if this is pre-existing
					// or something new
					// this will follow 2 different flows 
					// based on if it is hqs or sqs
					if(this.importQs instanceof HardSelectQueryStruct) {
						logger.info("Parameterizing hard query struct");
						
						HardSelectQueryStruct hqs = (HardSelectQueryStruct) this.importQs;
						String query = hqs.getQuery();
						String finalQuery = null;
						try {
							finalQuery = GenExpressionWrapper.transformQueryWithParams(query, thisImportParams, detailsLookup);
						} catch (Exception e1) {
							logger.error(Constants.STACKTRACE, e);
							// add to list of expressions
							expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
							this.pixels.add(expression);
							continue ROUTINE_LOOP;
						}
						
						String newExpr = sourceStr + "| Query(\"<encode>" + finalQuery + "</encode>\") | " + this.importStr + " ;";
						this.pixels.add(newExpr);
					} else {
						logger.info("Parameterizing pixel select query struct");
						
						// now we will look for if this is a filter already applied in the import
						// or a new filter we are adding
						
						List<IQueryFilter> newFilters = new Vector<>();

						// you can have a filter
						// that is not a selector
						// so lets look at filters as well
						List<Integer> paramIndexFound = new Vector<>();
						List<IQueryFilter> currentImportFilters = importQs.getExplicitFilters().getFilters();
						for(IQueryFilter f : currentImportFilters) {
							IQueryFilter modification = f;
							// i will want to run through EVERY column
							// for the same filter
							// in case it contains it
							for(int i = 0; i < thisImportParams.size(); i++) {
								// we need to do this based on the level
								ParamStructDetails importParam = thisImportParams.get(i);
								modification = QsFilterParameterizeConverter2.modifyFilter(modification, importParam, detailsLookup.get(importParam));
								// if we have returned a new filter object
								// that means it has been modified
								if(modification != f) {
									paramIndexFound.add(i);
									// switch the reference of f in this example
									// so that not all the param index get added
									f = modification;
								}
							}
							
							// add the modified filter if it was changed
							newFilters.add(modification);
						}
						
						// FE prefers a single AND block
						// so adding all the current filters
						// note - this maybe 0 or more
						AndQueryFilter andFilter = new AndQueryFilter();
						andFilter.addFilter(newFilters);
						
						for(int i = 0; i < thisImportParams.size(); i++) {
							if(paramIndexFound.contains(new Integer(i))) {
								continue;
							}
							
							// this is a param we have not found yet
							// add new filters
							ParamStructDetails importParam = thisImportParams.get(i);
							ParamStruct pStruct = detailsLookup.get(importParam);
							String comparator = importParam.getOperator();
							if(comparator == null || comparator.isEmpty()) {
								comparator = "==";
							}
							// this is the replacement
							String replacement = null;
							if(ParamStruct.PARAM_FILL_USE_ARRAY_TYPES.contains(pStruct.getModelDisplay()) 
									|| importParam.getQuote() == QUOTE.NO) {
								replacement = "[<" + pStruct.getParamName() + ">]";
							} else {
								PixelDataType importType = importParam.getType();
								if(importType == PixelDataType.CONST_INT || importType == PixelDataType.CONST_DECIMAL) {
									replacement = "<" + pStruct.getParamName() + ">";
								} else {
									replacement = "\"<" + pStruct.getParamName() + ">\"";
								}
							}
							SimpleQueryFilter paramF = new SimpleQueryFilter(
									new NounMetadata(new QueryColumnSelector(importParam.getTableName() + "__" + importParam.getColumnName()), PixelDataType.COLUMN), 
									comparator, 
									new NounMetadata(replacement, PixelDataType.CONST_STRING)
									);
							
							// add these filters into the AND
							andFilter.addFilter(paramF);
							
							logger.info("Adding new filter for column = " + pStruct.getParamName() );
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
						
						String newExpr = sourceStr + "|" + QsToPixelConverter.getPixel(this.importQs) + " | " + this.importStr + " ;";
						this.pixels.add(newExpr);
					}
					
					// reset
					this.importQs = null;
					this.sourceStr = null;
					this.importStr = null;
				}
			} else {
				// add to list of expressions
				expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodingList, this.encodedToOriginal);
				this.pixels.add(expression);
			}
		}
	}
	
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
    
	public void setCurrentPixelId(String currentPixelId) {
		this.currentPixelId = currentPixelId;
	}
	
	/**
	 * Get the new pixels
	 * @return
	 */
	public List<String> getPixels() {
		return this.pixels;
	}
	
	public void setInputsToParameterize(List<ParamStruct> paramStructs) {
		this.paramStructs = paramStructs;
	}
	
	/**
	 * Testing method
	 * @param args
	 */
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace3\\Semoss_Dev\\RDF_Map.prop");
		Gson gson = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.setPrettyPrinting()
				.create();
		
		String[] recipe = new String[]{
				"AddPanel ( panel = [ 0 ] , sheet = [ \"0\" ] ) ;",
				"Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] ) ;",
				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>(<Frame> | UnfilterFrame(<SelectedColumn>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if((IsEmpty(<SelectedValues>)),(<Frame> | UnfilterFrame(<SelectedColumn>)), (<Frame> | SetFrameFilter(<SelectedColumn>==<SelectedValues>)));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;",
				"Panel ( 0 ) | RetrievePanelEvents ( ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
				"Panel ( 0 ) | SetPanelView ( \"pipeline\" ) ;", 
				"Database ( database = [ \"995cf169-6b44-4a42-b75c-af12f9f45c36\" ] ) | Select ( DIABETES__AGE , DIABETES__BP_1D , DIABETES__BP_1S , DIABETES__BP_2D , DIABETES__BP_2S , DIABETES__CHOL , DIABETES__DIABETES_UNIQUE_ROW_ID , DIABETES__DRUG , DIABETES__END_DATE , DIABETES__FRAME , DIABETES__GENDER , DIABETES__GLYHB , DIABETES__HDL , DIABETES__HEIGHT , DIABETES__HIP , DIABETES__LOCATION , DIABETES__PATIENT , DIABETES__RATIO , DIABETES__STAB_GLU , DIABETES__START_DATE , DIABETES__TIME_PPN , DIABETES__WAIST , DIABETES__WEIGHT ) .as ( [ AGE , BP_1D , BP_1S , BP_2D , BP_2S , CHOL , DIABETES_UNIQUE_ROW_ID , DRUG , END_DATE , FRAME , GENDER , GLYHB , HDL , HEIGHT , HIP , LOCATION , PATIENT , RATIO , STAB_GLU , START_DATE , TIME_PPN , WAIST , WEIGHT ] ) | Filter ( ( ( DIABETES__AGE > [ 50 ] ) ) ) | Distinct ( false ) | Import ( frame = [ CreateFrame ( frameType = [ PY ] , override = [ true ] ) .as ( [ \"Diabetes_FRAME916484\" ] ) ] ) ;",
				"Database ( database = [ \"995cf169-6b44-4a42-b75c-af12f9f45c36\" ] ) | Query (\"<encode>SELECT * FROM DIABETES WHERE AGE in (25,35)</encode>\") | Distinct ( false ) | Import ( frame = [ CreateFrame ( frameType = [ PY ] , override = [ true ] ) .as ( [ \"Diabetes_FRAME555555\" ] ) ] ) ;",
				"META | PositionInsightRecipeStep ( positionMap = [ { \"auto\" : false , \"top\" : 24 , \"left\" : 24 } ] ) ;",
				"META | SetInsightConfig({\"panels\":{\"0\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"0\":{\"order\":0,\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"0\"}}]}]}]}}},\"sheet\":\"0\"});",
		};
		int recipeLength = recipe.length;
		String[] ids = new String[recipeLength];
		for(int i = 0; i < recipeLength; i++) {
			ids[i] = i+"";
		}
		
		List<ParamStruct> params = new Vector<>();
		// param
		{
			ParamStruct pStruct = new ParamStruct();
			params.add(pStruct);
			pStruct.setDefaultValue("MALE");
			pStruct.setParamName("Genre");
			pStruct.setFillType(FILL_TYPE.PIXEL);
			pStruct.setModelQuery("");
			pStruct.setMultiple(true);
			pStruct.setRequired(true);
			pStruct.setSearchable(true);
			pStruct.setModelLabel("Fill in Genre");
			{
				ParamStructDetails details = new ParamStructDetails();
				pStruct.addParamStructDetails(details);
				details.setPixelId("6");
				details.setPixelString(recipe[6]);
				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
				details.setTableName("DIABETES");
				details.setColumnName("GENRE");
				details.setOperator("==");
				details.setLevel(LEVEL.COLUMN);
				details.setQuote(QUOTE.DOUBLE);
			}
		}
		// param
		{
			ParamStruct pStruct = new ParamStruct();
			params.add(pStruct);
			pStruct.setDefaultValue(null);
			pStruct.setParamName("AGE");
			pStruct.setFillType(FILL_TYPE.PIXEL);
			pStruct.setModelQuery("");
			pStruct.setMultiple(true);
			pStruct.setRequired(true);
			pStruct.setSearchable(true);
			pStruct.setModelLabel("Fill in AGE");
			{
				ParamStructDetails details = new ParamStructDetails();
				pStruct.addParamStructDetails(details);
				details.setPixelId("6");
				details.setPixelString(recipe[6]);
				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
				details.setTableName("DIABETES");
				details.setColumnName("AGE");
				details.setOperator(">");
				details.setLevel(LEVEL.OPERATOR);
				details.setQuote(QUOTE.DOUBLE);
			}
		}
		// param
		{
			ParamStruct pStruct = new ParamStruct();
			params.add(pStruct);
			pStruct.setDefaultValue(null);
			pStruct.setParamName("AGE2");
			pStruct.setFillType(FILL_TYPE.PIXEL);
			pStruct.setModelQuery("");
			pStruct.setMultiple(true);
			pStruct.setRequired(true);
			pStruct.setSearchable(true);
			pStruct.setModelLabel("Fill in AGE");
			{
				ParamStructDetails details = new ParamStructDetails();
				pStruct.addParamStructDetails(details);
				details.setPixelId("7");
				details.setPixelString(recipe[7]);
				details.setAppId("995cf169-6b44-4a42-b75c-af12f9f45c36");
				details.setTableName("DIABETES");
				details.setColumnName("AGE");
				details.setOperator("in");
				details.setLevel(LEVEL.OPERATOR);
				details.setQuote(QUOTE.NO);
			}
		}
		
		Insight in = new Insight();
		ParamStructSaveRecipeTranslation translation = new ParamStructSaveRecipeTranslation(in);
		translation.setInputsToParameterize(params);
		
		// loop through recipe
		for(int i = 0; i < recipeLength; i++) {
			String expression = recipe[i];
			String pixelId = ids[i];
			try {
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodingList, translation.encodedToOriginal);
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation.
				translation.setCurrentPixelId(pixelId);
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println(gson.toJson(translation.getPixels()));
	}

}
