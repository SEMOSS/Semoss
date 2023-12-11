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
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSRenameColumnConverter;
import prerna.query.querystruct.transform.QsToPixelConverter;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
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
import prerna.util.Utility;

public class ReplaceDatasourceTranslation extends AbstractDatasourceModificationTranslation {

	private static final Logger logger = LogManager.getLogger(ReplaceDatasourceTranslation.class);

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<String>();
	
	private List<Map<String, Object>> replacements = null;
	private int currentIndex = 0;
	
	// a replacement for the given input
	private String origPixelPortion = null;
	private String replacementSourcePixel = null;
	private Map<String, String> headerMods = null;
	
	// contain the query struct
	private SelectQueryStruct importQs;
	private String importStr;
	
	// this is for the special case
	// where the index isn't provided
	// and we just want to replace the first source
	private boolean replaceFirst = false;
	private boolean doneReplaceFirst = false;
	
	public ReplaceDatasourceTranslation(Insight insight) {
		super(insight);
	}
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
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
				e.apply(this);
				
				// if we ended up finding something to store
				if(this.replacementSourcePixel != null && this.origPixelPortion != null) {
					
					String newExpression = null;
					if(this.importQs != null && this.headerMods != null) {
						// need to replace the columns as well
						newExpression = this.replacementSourcePixel 
								+ " | " + QsToPixelConverter.getPixel(QSRenameColumnConverter.convertQs(this.importQs, this.headerMods, true), false)
								+ " | " + this.importStr + ";";
					} else {
						// just replace the source
						newExpression = expression.replace(this.origPixelPortion, this.replacementSourcePixel);
					}
					this.pixels.add(newExpression);

					
					// now we need to null replacement
					this.replacementSourcePixel = null;
					this.origPixelPortion = null;
					this.importQs = null;
					this.headerMods = null;
					this.importStr = null;
					
				} else {
		        	expression = PixelUtility.recreateOriginalPixelExpression(expression, encodingList, encodedToOriginal);
					this.pixels.add(expression);
				}
				
			} else {
	        	expression = PixelUtility.recreateOriginalPixelExpression(expression, encodingList, encodedToOriginal);
				this.pixels.add(expression);
			}
			
			// update the index
			this.currentIndex++;
		}
	}
	
	@Override
	public void inAOperation(AOperation node) {
		super.inAOperation(node);
		defaultIn(node);
		
		// looking for data sources
		String reactorId = node.getId().toString().trim();
		if(importTypes.contains(reactorId)) {
			tryPerformReplacement(node.toString().trim());
		} else if(reactorId.equals("Import") || reactorId.equals("Merge")) {
			this.importStr = node.toString();
		}
	}
	
	private void tryPerformReplacement(String nodeString) {
		if(this.replaceFirst && !this.doneReplaceFirst) {
			// we need to replace 
			Map<String, Object> singleReplacement = this.replacements.get(0);
			
			// set the values for the replacement
			this.origPixelPortion = nodeString;
			this.replacementSourcePixel = (String) singleReplacement.get("pixel");
			this.headerMods = (Map<String, String>) singleReplacement.get("headerMod");
			
			this.doneReplaceFirst = true;
		} else if(!doneReplaceFirst) { // add the else if for performance
			// try and see if there is a replacement that matches
			// based on the index
			for(Map<String, Object> replacementOption : this.replacements) {
				int index = (int) replacementOption.get("index");
				if(index == this.currentIndex) {
					// set the values for the replacement
					this.origPixelPortion = nodeString;
					this.replacementSourcePixel = (String) replacementOption.get("pixel");
					this.headerMods = (Map<String, String>) replacementOption.get("headerMod");

					break;
				}
			}
		}
	}
	
	@Override
	public void outAOperation(AOperation node) {
		super.outAOperation(node);

		if(this.replacementSourcePixel != null && prevReactor != null) {
			// we are doing some kind of replacement
			// so we are going to try to find the import qs
			NounStore nouns = prevReactor.getNounStore();
			GenRowStruct grs = nouns.getNoun(PixelDataType.QUERY_STRUCT.getKey());
			if(grs != null && !grs.isEmpty()) {
				this.importQs = (SelectQueryStruct) grs.get(0);
			} else {
				grs = prevReactor.getCurRow();
				if(grs != null && !grs.isEmpty()) {
					List<Object> qsList = grs.getValuesOfType(PixelDataType.QUERY_STRUCT);
					if(qsList != null && !qsList.isEmpty()) {
						this.importQs = (SelectQueryStruct) qsList.get(0);
					}
				}
			}
		}
	}
	
	/**
	 * Get the new pixels
	 * @return
	 */
	public List<String> getPixels() {
		return this.pixels;
	}
	
	/**
	 * Set the replacements
	 * @param replacements
	 */
	public void setReplacements(List<Map<String, Object>> replacements) {
		this.replacements = replacements;
		
		// accounting for special case
		if(this.replacements.size() == 1) {
			Map<String, Object> singleReplacement = this.replacements.get(0);
			if(!singleReplacement.containsKey("index")) {
				// no index was provided
				// so we will just replace the first source we get
				this.replaceFirst = true;
			}
		}
		
	}
	
	/**
	 * Testing method
	 * @param args
	 */
//	public static void main(String[] args) {
//		Gson gson = new GsonBuilder()
//				.disableHtmlEscaping()
//				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
//				.setPrettyPrinting()
//				.create();
//		
//		// grab an example recipe
//		logger.info("TRYING WITH IMPLICIT REPLACEMENT OF FIRST SOURCE");
//		String expression = "AddPanel ( 0 ) ;Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eUnfilterFrame(\u003cSelectedColumn\u003e);\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eif(IsEmpty(\u003cSelectedValues\u003e), UnfilterFrame(\u003cSelectedColumn\u003e), SetFrameFilter(\u003cSelectedColumn\u003e\u003d\u003d\u003cSelectedValues\u003e));\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;Panel ( 0 ) | RetrievePanelEvents ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" , \"\u003cencode\u003e{\"type\":\"echarts\"}\u003c/encode\u003e\" ) ;Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"\u003cencode\u003e{\"core_engine\":\"NEWSEMOSSAPP\"}\u003c/encode\u003e\" ) ;CreateFrame ( Grid ) .as ( [ \u0027FRAME549443\u0027 ] ) ;GoogleSheetSource ( id \u003d [ \"1EZbv_mXn_tnguDG02awFwQ30EqGMoWKZBflUVlcLgxY\" ] , sheetNames \u003d [ \"diabetes\" ] , type \u003d [ \".spreadsheet\" ] ) | Import ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel \u003d [ \"0\" ] , layout \u003d [ \"Grid\" ] ) | Collect ( 500 ) ;if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;Panel(0)|AddPanelOrnaments({\"showMenu\":true});Panel(0)|RetrievePanelOrnaments(\"showMenu\");";
//		
//		List<Map<String, Object>> testReplacements = new Vector<Map<String, Object>>();
//		Map<String, Object> replacementMap = new HashMap<String, Object>();
//		String replacementPixelToUse = "FileRead(filePath=[\"C:/workspace/Semoss_Dev/Movie_Data2018_03_27_13_08_21_0875.csv\"],dataTypeMap=[{\"Nominated\":\"STRING\",\"Title\":\"STRING\",\"Genre\":\"STRING\",\"Studio\":\"STRING\",\"Director\":\"STRING\",\"Revenue_Domestic\":\"NUMBER\",\"MovieBudget\":\"NUMBER\",\"Revenue_International\":\"NUMBER\",\"RottenTomatoes_Critics\":\"NUMBER\",\"RottenTomatoes_Audience\":\"NUMBER\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"Movie_Data\"])|Select(DND__Nominated, DND__Title, DND__Genre, DND__Studio, DND__Director, DND__Revenue_Domestic, DND__MovieBudget, DND__Revenue_International, DND__RottenTomatoes_Critics, DND__RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience])";
//		replacementMap.put("pixel", replacementPixelToUse);
//		
//		testReplacements.add(replacementMap);
//		
//		Insight in = new Insight();
//		ReplaceDatasourceTranslation translation = new ReplaceDatasourceTranslation(in);
//		translation.setReplacements(testReplacements);
////		try {
////			expression = PixelPreProcessor.preProcessPixel(expression.trim(), new HashMap<String, String>());
////			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
////			// parsing the pixel - this process also determines if expression is syntactically correct
////			Start tree = p.parse();
////			// apply the translation.
////			tree.apply(translation);
////		} catch (ParserException | LexerException | IOException e) {
////			e.printStackTrace();
////		}
//		
//		List<String> sourcePixels = translation.getPixels();
//		System.out.println(gson.toJson(sourcePixels));
//		
//		logger.info("");
//		logger.info("");
//
////		LOGGER.info("TRYING WITH EXPLICIT REPLACEMENT INDEX");
////		expression = "AddPanel ( 0 ) ;Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eUnfilterFrame(\u003cSelectedColumn\u003e);\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eif(IsEmpty(\u003cSelectedValues\u003e), UnfilterFrame(\u003cSelectedColumn\u003e), SetFrameFilter(\u003cSelectedColumn\u003e\u003d\u003d\u003cSelectedValues\u003e));\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;Panel ( 0 ) | RetrievePanelEvents ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" , \"\u003cencode\u003e{\"type\":\"echarts\"}\u003c/encode\u003e\" ) ;Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"\u003cencode\u003e{\"core_engine\":\"NEWSEMOSSAPP\"}\u003c/encode\u003e\" ) ;CreateFrame ( Grid ) .as ( [ \u0027FRAME549443\u0027 ] ) ;GoogleSheetSource ( id \u003d [ \"1EZbv_mXn_tnguDG02awFwQ30EqGMoWKZBflUVlcLgxY\" ] , sheetNames \u003d [ \"diabetes\" ] , type \u003d [ \".spreadsheet\" ] ) | Import ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel \u003d [ \"0\" ] , layout \u003d [ \"Grid\" ] ) | Collect ( 500 ) ;if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;Panel(0)|AddPanelOrnaments({\"showMenu\":true});Panel(0)|RetrievePanelOrnaments(\"showMenu\");";
////		
////		testReplacements = new Vector<Map<String, Object>>();
////		replacementMap = new HashMap<String, Object>();
////		replacementPixelToUse = "FileRead(filePath=[\"C:/workspace/Semoss_Dev/Movie_Data2018_03_27_13_08_21_0875.csv\"],dataTypeMap=[{\"Nominated\":\"STRING\",\"Title\":\"STRING\",\"Genre\":\"STRING\",\"Studio\":\"STRING\",\"Director\":\"STRING\",\"Revenue_Domestic\":\"NUMBER\",\"MovieBudget\":\"NUMBER\",\"Revenue_International\":\"NUMBER\",\"RottenTomatoes_Critics\":\"NUMBER\",\"RottenTomatoes_Audience\":\"NUMBER\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"Movie_Data\"])|Select(DND__Nominated, DND__Title, DND__Genre, DND__Studio, DND__Director, DND__Revenue_Domestic, DND__MovieBudget, DND__Revenue_International, DND__RottenTomatoes_Critics, DND__RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience])";
////		replacementMap.put("pixel", replacementPixelToUse);
////		replacementMap.put("index", 6);
////
////		testReplacements.add(replacementMap);
////		
////		in = new Insight();
////		translation = new ReplaceDatasourceTranslation(in);
////		translation.setReplacements(testReplacements);
////		try {
////			expression = PixelPreProcessor.preProcessPixel(expression.trim(), new HashMap<String, String>());
////			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
////			// parsing the pixel - this process also determines if expression is syntactically correct
////			Start tree = p.parse();
////			// apply the translation.
////			tree.apply(translation);
////		} catch (ParserException | LexerException | IOException e) {
////			e.printStackTrace();
////		}
////		
////		sourcePixels = translation.getPixels();
////		System.out.println(gson.toJson(sourcePixels));
//		
//		
//		logger.info("");
//		logger.info("");
//
//		logger.info("TRYING WITH DATABASE ");
//		expression = "AddPanel ( 0 ) ;Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eUnfilterFrame(\u003cSelectedColumn\u003e);\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eif(IsEmpty(\u003cSelectedValues\u003e), UnfilterFrame(\u003cSelectedColumn\u003e), SetFrameFilter(\u003cSelectedColumn\u003e\u003d\u003d\u003cSelectedValues\u003e));\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;Panel ( 0 ) | RetrievePanelEvents ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" , \"\u003cencode\u003e{\"type\":\"echarts\"}\u003c/encode\u003e\" ) ;Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"\u003cencode\u003e{\"core_engine\":\"NEWSEMOSSAPP\"}\u003c/encode\u003e\" ) ;CreateFrame ( Grid ) .as ( [ \u0027FRAME549443\u0027 ] ) ; Database(abc) | Select(Title, Title__Movie_Budget) | Import ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel \u003d [ \"0\" ] , layout \u003d [ \"Grid\" ] ) | Collect ( 500 ) ;if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;Panel(0)|AddPanelOrnaments({\"showMenu\":true});Panel(0)|RetrievePanelOrnaments(\"showMenu\");";
//		
//		testReplacements = new Vector<Map<String, Object>>();
//		replacementMap = new HashMap<String, Object>();
//		replacementPixelToUse = "Database(NewDatabaseToUse)";
//		replacementMap.put("pixel", replacementPixelToUse);
//		replacementMap.put("index", 6);
//		Map<String, String> headerMod = new HashMap<String, String>();
//		headerMod.put("Title", "System");
//		headerMod.put("Title__Movie_Budget", "System__AvailabilityActual");
//		replacementMap.put("headerMod", headerMod);
//
//		testReplacements.add(replacementMap);
//		
//		in = new Insight();
//		translation = new ReplaceDatasourceTranslation(in);
//		translation.setReplacements(testReplacements);
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
//		sourcePixels = translation.getPixels();
//		System.out.println(gson.toJson(sourcePixels));
//	}

	
}
