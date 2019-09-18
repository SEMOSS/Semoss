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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.GenericReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.imports.ImportReactor;
import prerna.sablecc2.reactor.imports.MergeReactor;
import prerna.sablecc2.reactor.map.AbstractMapReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.qs.source.FileReadReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.qs.source.GoogleFileRetrieverReactor;

public class ParameterizeSaveRecipeTranslation extends LazyTranslation {
	
	private static final Logger LOGGER = LogManager.getLogger(ParameterizeSaveRecipeTranslation.class.getName());

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<String>();
	
	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();
	
	// set the parameters we care about
	public List<String> inputsToParameterize;
	
	private SelectQueryStruct importQs;
	private String sourceStr;
	private String importStr;
	private Map<String, Map<String, String>> paramToSource = new HashMap<String, Map<String, String>>();
	
	public ParameterizeSaveRecipeTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
			String expression = e.toString();
			if(expression.contains("Import") || expression.contains("Merge")) {
				LOGGER.info("Processing " + expression);
				e.apply(this);

				// check if we have a QS to modify
				if(this.importQs != null) {
					
					// first, need to see which params are being imported
					// that we need to filter on
					
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
//							else if(colToParam.equalsIgnoreCase(qsName)) {
//								foundParams.add(alias);
//								filterQsName.add(qsName);
//							}
						}
					}
					
					// if no matches
					// nothing to do
					if(foundParams.isEmpty()) {
						this.pixels.add(expression);
						continue;
					}
					
					int numParams = foundParams.size();
					
					// we must have found something to get here
					// now make sure we do not have any other filters for those params
					List<IQueryFilter> filters = this.importQs.getExplicitFilters().getFilters();
					int numFilters = filters.size();
					
					List<Integer> filtersToDrop = new Vector<Integer>();
					filterLoop : for(int filterIndex = 0; filterIndex < numFilters; filterIndex++) {
						IQueryFilter f = filters.get(filterIndex);
						
						for(int paramColIndex = 0; paramColIndex < numParams; paramColIndex++) {
							String qsColName = filterQsName.get(paramColIndex);
							if(f.containsColumn(qsColName)) {
								// add them so the indices are in decreasing order
								filtersToDrop.add(0, new Integer(filterIndex));
								continue filterLoop;
							}
						}
					}
					
					// now loop through and drop the indices that contain any of the columns
					// we are trying to param
					for(Integer dropIndex : filtersToDrop) {
						filters.remove(dropIndex.intValue());
					}
					
					// now that we have removed the filters
					// we want to construct a new expression
					for(int paramColIndex = 0; paramColIndex < numParams; paramColIndex++) {
						String qsColName = filterQsName.get(paramColIndex);
						String paramName = foundParams.get(paramColIndex);
						SimpleQueryFilter paramF = new SimpleQueryFilter(
								new NounMetadata(new QueryColumnSelector(qsColName), PixelDataType.COLUMN), 
								"==", 
								new NounMetadata("<" + paramName + ">", PixelDataType.CONST_STRING)
								);
						
						filters.add(paramF);
					}
					
					String newExpr = sourceStr + "|" + QsToPixelConverter.getPixel(this.importQs) + " | " + this.importStr + " ;";
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
				expression = PixelUtility.recreateOriginalPixelExpression(expression, this.encodedToOriginal);
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
	
	protected void deInitReactor()
	{
		IReactor thisPrevReactor = curReactor;
		Object parent = curReactor.Out();
		
		//set the curReactor
    	if(parent != null && parent instanceof IReactor) {
    		curReactor = (IReactor)parent;
    	} else {
    		curReactor = null;
    	}
    	
    	boolean isQs = false;
    	boolean isImport = false;
    	boolean requireExec = false;
    	// need to merge generic reactors & maps for proper input setting
    	if(thisPrevReactor !=null && (thisPrevReactor instanceof GenericReactor || thisPrevReactor instanceof AbstractMapReactor)) {
    		requireExec = true;
    	}
    	// need to merge qs
    	else if(thisPrevReactor != null && thisPrevReactor instanceof AbstractQueryStructReactor) {
			isQs = true;
		} 
    	// need to find imports
    	else if(thisPrevReactor != null && (thisPrevReactor instanceof ImportReactor || thisPrevReactor instanceof MergeReactor)) {
			isImport = true;
		}
    	
		// only want to execute for qs
		if(isQs || requireExec) {
			NounMetadata output = thisPrevReactor.execute();
			
			// synchronize the result to the parent reactor
			if(output != null) {
	    		if(curReactor != null && !(curReactor instanceof AssignmentReactor)) {
	    			// add the value to the parent's curnoun
	    			curReactor.getCurRow().add(output);
		    	} else {
		    		//otherwise if we have an assignment reactor or no reactor then add the result to the planner
		    		this.planner.addVariable("$RESULT", output);
		    	}
	    	} else {
	    		this.planner.removeVariable("$RESULT");
	    	}
			
		} else if(isImport) {
			GenRowStruct grs = thisPrevReactor.getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
			this.importQs = (SelectQueryStruct) grs.get(0);
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
	public static void main(String[] args) {
		Gson gson = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.setPrettyPrinting()
				.create();
		
		String[] recipe = new String[]{ "AddPanel ( 0 ) ; ",
				  "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"UnfilterFrame(%3CSelectedColumn%3E)%3B\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"if(IsEmpty(%3CSelectedValues%3E)%2C%20UnfilterFrame(%3CSelectedColumn%3E)%2C%20SetFrameFilter(%3CSelectedColumn%3E%3D%3D%3CSelectedValues%3E))%3B\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ; ",
				  "Panel ( 0 ) | RetrievePanelEvents ( ) ; ",
				  "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"%7B%22type%22%3A%22echarts%22%7D\" ) ; ",
				  "Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"%7B%22core_engine%22%3A%22NEWSEMOSSAPP%22%7D\" ) ; ",
				  "CreateFrame ( Grid ) .as ( [ 'FRAME549443' ] ) ; ",
				  "FileRead(filePath=[\"C:/workspace/Semoss_Dev/Movie_Data2018_03_27_13_08_21_0875.csv\"],dataTypeMap=[{\"Nominated\":\"STRING\",\"Title\":\"STRING\",\"Genre\":\"STRING\",\"Studio\":\"STRING\",\"Director\":\"STRING\",\"Revenue_Domestic\":\"NUMBER\",\"MovieBudget\":\"NUMBER\",\"Revenue_International\":\"NUMBER\",\"RottenTomatoes_Critics\":\"NUMBER\",\"RottenTomatoes_Audience\":\"NUMBER\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"Movie_Data\"])|Select(DND__Nominated, DND__Title, DND__Genre, DND__Studio, DND__Director, DND__Revenue_Domestic, DND__MovieBudget, DND__Revenue_International, DND__RottenTomatoes_Critics, DND__RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience]) | Filter(DND__Genre == \"Drama\") | Filter(DND__MovieBudget > 10) |Import ( ) ; ",
				  "Panel ( 0 ) | SetPanelView ( \"visualization\" ) ; ",
				  "Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ; ",
				  "if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ; ",
				  "Panel ( 0 ) | AddPanelOrnaments ( { \"showMenu\" : true } ) ; ",
				  "Panel ( 0 ) | RetrievePanelOrnaments ( \"showMenu\" ) ; "
		};
		
		List<String> params = new Vector<String>();
		params.add("MovieBudget");
		
		Insight in = new Insight();
		ParameterizeSaveRecipeTranslation translation = new ParameterizeSaveRecipeTranslation(in);
		translation.setInputsToParameterize(params);
		
		// loop through recipe
		for(String expression : recipe) {
			try {
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), new HashMap<String, String>());
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
			
		}
		
		System.out.println(gson.toJson(translation.getPixels()));
		System.out.println(gson.toJson(translation.getParamToSource()));
	}

}
