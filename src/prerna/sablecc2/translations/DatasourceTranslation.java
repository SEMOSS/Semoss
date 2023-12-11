package prerna.sablecc2.translations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.test.TestUtilityMethods;
import prerna.util.Utility;

public class DatasourceTranslation extends AbstractDatasourceModificationTranslation {

	private static final Logger logger = LogManager.getLogger(DatasourceTranslation.class.getName());

	private List<Map<String, Object>> datasourcePixels = new ArrayList<Map<String, Object>>();
	private Map<String, Object> currentSourceStatement = null;
	
	public DatasourceTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		int currentIndex = 0;
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
    		this.resultKey = "$RESULT_" + e.hashCode();

			String expression = e.toString();
			logger.info("Processing " + Utility.cleanLogString(expression));

			boolean process = false;
			for(String iType : importTypes) {
				if(expression.contains(iType)) {
					process = true;
					break;
				}
			}
			
			// only execute pixels that at least contain the import
			if(process) {
				// TODO:
				// THIS IS IN A TRY CATCH BECAUSE WE ARE NOT PROPERLY LOOKING
				// AT IF WE ARE NOT PROPERLY ISOLATING THE STEPS THAT ARE
				// FILE READS
				try {
					e.apply(this);
					// if we ended up finding something to store
					if(this.currentSourceStatement != null) {
						this.currentSourceStatement.put("pixelStepIndex", currentIndex);
						// process to ensure query structs are broken into parts for FE to consume
						postProcessParams(this.currentSourceStatement);
						// add the source to store
						this.datasourcePixels.add(this.currentSourceStatement);
						// at the end, we will null it
						this.currentSourceStatement = null;
					}
				} catch(Exception e1) {
					e1.printStackTrace();
				}
			}
			
			// update the index
			currentIndex++;
		}
	}

	private void postProcessParams(Map<String, Object> sourceInfo) {
		Map<String, List<Object>> paramMap = (Map<String, List<Object>>) sourceInfo.get("params");
		if(paramMap != null) {
			Set<String> keysToRemove = new HashSet<String>();
			Map<String, List<Object>> processedParams = new HashMap<String, List<Object>>();
			for(String key : paramMap.keySet()) {
				// right now, only want to process query structs into a more readable format
				if(key.equals(PixelDataType.QUERY_STRUCT.toString())) {
					List<Object> inputs = paramMap.get(key);
					// i am 99.999999% positve this will always be size 1
					SelectQueryStruct inputQs = (SelectQueryStruct) inputs.get(0);
					// grab the unique set of selectors
					Set<String> uniqueCols = new HashSet<String>();
					List<IQuerySelector> selectors = inputQs.getSelectors();
					for(IQuerySelector selector : selectors) {
						List<QueryColumnSelector> baseColsUsed = selector.getAllQueryColumns();
						for(QueryColumnSelector s : baseColsUsed) {
							uniqueCols.add(s.getQueryStructName());
						}
					}
					List<IQueryFilter> filters = inputQs.getExplicitFilters().getFilters();
					for(IQueryFilter f : filters) {
						uniqueCols.addAll(f.getAllQueryStructNames());
					}
					// add all joins
					Set<IRelation> relations = inputQs.getRelations();
					for (IRelation relationship : relations) {
						if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
							BasicRelationship rel = (BasicRelationship) relationship;
							String up = rel.getFromConcept();
							String down = rel.getToConcept();
							uniqueCols.add(up);
							uniqueCols.add(down);
						} else {
							logger.info("Cannot process relationship of type: " + relationship.getRelationType());
						}
					}
					
//					Map<String, Map<String, List>> relations = inputQs.getRelations();
//					for(String up : relations.keySet()) {
//						// store the up node
//						uniqueCols.add(up);
//						Map<String, List> inner = relations.get(up);
//						for(String jType : inner.keySet()) {
//							List<String> downList = inner.get(jType);
//							for(String down : downList) {
//								// store the down node
//								uniqueCols.add(down);
//							}
//						}
//					}
					
					// we have the list of columns that we would want to find equiv.
					// for that are used in the import
					// wrap in list to hold structure
					List<Object> obj = new ArrayList<Object>();
					obj.addAll(uniqueCols);
					processedParams.put("qsColumns", obj);
					keysToRemove.add(key);
				}
			}
			// now we remove the QS and add the columns
			if(!processedParams.isEmpty()) {
				paramMap.keySet().removeAll(keysToRemove);
				paramMap.putAll(processedParams);
			}
		}
	}

	@Override
	public void inAOperation(AOperation node) {
		super.inAOperation(node);
		
		// looking for data sources
		String reactorId = node.getId().toString().trim();
		if(importTypes.contains(reactorId)) {
			this.currentSourceStatement = new HashMap<String, Object>();
			this.currentSourceStatement.put("expression", node.toString());
			this.currentSourceStatement.put("type", reactorId);
			Map<String, List<Object>> sourceParams = new HashMap<String, List<Object>>();
			this.currentSourceStatement.put("params", sourceParams);
		}
	}
	
	@Override
	public void outAOperation(AOperation node) {
		if(this.currentSourceStatement != null) {
			// store the inputs of the
			Map<String, List<Object>> sourceParams = (Map<String, List<Object>>) this.currentSourceStatement.get("params");
			
			NounStore nouns = this.curReactor.getNounStore();
			Set<String> inputKeys = nouns.getNounKeys();
			boolean single = inputKeys.size() == 1;
			for(String key : inputKeys) {
				if(single && key.equals("all")) {
					GenRowStruct grs = nouns.getNoun(key);
					if(!grs.isEmpty()) {
						sourceParams.put("noKey", grs.getAllValues());
					}
				} else if(!key.equals("all")) {
					GenRowStruct grs = nouns.getNoun(key);
					sourceParams.put(key, grs.getAllValues());
				}
			}
		}
		
		super.outAOperation(node);
	}
	
	public List<Map<String, Object>> getDatasourcePixels() {
		return this.datasourcePixels;
	}
	
	/**
	 * Testing method
	 * @param args
	 */
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper("C:/workspace/Semoss_Dev/RDF_Map.prop");
//		// grab an example recipe
//		
//		String expression = "AddPanel ( 0 ) ; "
//				  + "Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"UnfilterFrame(%3CSelectedColumn%3E)%3B\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"if(IsEmpty(%3CSelectedValues%3E)%2C%20UnfilterFrame(%3CSelectedColumn%3E)%2C%20SetFrameFilter(%3CSelectedColumn%3E%3D%3D%3CSelectedValues%3E))%3B\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ; "
//				  + "Panel ( 0 ) | RetrievePanelEvents ( ) ; "
//				  + "Panel ( 0 ) | SetPanelView ( \"visualization\" , \"%7B%22type%22%3A%22echarts%22%7D\" ) ; "
//				  + "Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"%7B%22core_engine%22%3A%22NEWSEMOSSAPP%22%7D\" ) ; "
//				  + "CreateFrame ( Grid ) .as ( [ 'FRAME549443' ] ) ; "
////				  + "GoogleSheetSource ( id=[ \"1EZbv_mXn_tnguDG02awFwQ30EqGMoWKZBflUVlcLgxY\" ] , sheetNames=[ \"diabetes\" ] , type=[ \".spreadsheet\" ] ) | Import ( ) ;"
//				  + "FileRead(filePath=[\"C:/workspace/Semoss_Dev/Movie_Data2018_03_27_13_08_21_0875.csv\"],dataTypeMap=[{\"Nominated\":\"STRING\",\"Title\":\"STRING\",\"Genre\":\"STRING\",\"Studio\":\"STRING\",\"Director\":\"STRING\",\"Revenue_Domestic\":\"NUMBER\",\"MovieBudget\":\"NUMBER\",\"Revenue_International\":\"NUMBER\",\"RottenTomatoes_Critics\":\"NUMBER\",\"RottenTomatoes_Audience\":\"NUMBER\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"Movie_Data\"])|Select(DND__Nominated, DND__Title, DND__Genre, DND__Studio, DND__Director, DND__Revenue_Domestic, DND__MovieBudget, DND__Revenue_International, DND__RottenTomatoes_Critics, DND__RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience]) | Filter(DND__Genre == \"Drama\") | Filter(DND__MovieBudget > 10) |Import ( ) ; "
//				  + "Database(Movie_RDBMS) | Select(Title, Title__MovieBudget, Studio) | Join((Title, inner.join, Studio)) | Import(); "
//				  + "Panel ( 0 ) | SetPanelView ( \"visualization\" ) ; "
//				  + "Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ; "
////				  + "if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ; "
//				  + "Panel ( 0 ) | AddPanelOrnaments ( { \"showMenu\" : true } ) ; "
//				  + "Panel ( 0 ) | RetrievePanelOrnaments ( \"showMenu\" ) ;";
//		
//		Insight in = new Insight();
//		DatasourceTranslation translation = new DatasourceTranslation(in);
//		try {
//			expression = PixelPreProcessor.preProcessPixel(expression.trim(), new ArrayList<String>(), new HashMap<String, String>());
//			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
//			// parsing the pixel - this process also determines if expression is syntactically correct
//			Start tree = p.parse();
//			// apply the translation.
//			tree.apply(translation);
//		} catch (ParserException | LexerException | IOException e) {
//			e.printStackTrace();
//		}
//		
//		Gson gson = new GsonBuilder()
//				.disableHtmlEscaping()
//				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
//				.setPrettyPrinting()
//				.create();
//		
//		List<Map<String, Object>> sourcePixels = translation.getDatasourcePixels();
//		System.out.println(gson.toJson(sourcePixels));
//	}


}