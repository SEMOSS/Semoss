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

import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class ReplaceDatasourceTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(ReplaceDatasourceTranslation.class.getName());

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<String>();
	// a replacement for the given input
	private String origPixelPortion = null;
	private String replacementSourcePixel = null;
	
	private List<Map<String, Object>> replacements = null;
	
	private int currentIndex = 0;
	
	// this is for the special case
	// where the index isn't provided
	// and we just want to replace the first source
	private boolean replaceFirst = false;
	private boolean doneReplaceFirst = false;
	
	// create a variable to keep track of the current mapping of the original expression to the encoded expression
	public HashMap<String, String> encodedToOriginal = new HashMap<String, String>();
	
	@Override
	public void caseAConfiguration(AConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
			String expression = e.toString();
			LOGGER.info("Processing " + expression);
			e.apply(this);
			
			// if we ended up finding something to store
			if(this.replacementSourcePixel != null && this.origPixelPortion != null) {
				String newExpression = expression.replace(this.origPixelPortion, this.replacementSourcePixel);
				this.pixels.add(newExpression);
				// now we need to null replacement
				this.replacementSourcePixel = null;
				this.origPixelPortion = null;
			} else {
	        	expression = PixelUtility.recreateOriginalPixelExpression(expression, encodedToOriginal);
				this.pixels.add(expression);
			}
			
			// update the index
			this.currentIndex++;
		}
	}
	
	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);
		
		// looking for data sources
		String reactorId = node.getId().toString().trim();
		if(reactorId.equals("FileRead") || 
				reactorId.equals("GoogleSheetSource")) {
			tryPerformReplacement(node.toString().trim());
		}
	}
	
	private void tryPerformReplacement(String nodeString) {
		if(this.replaceFirst && !this.doneReplaceFirst) {
			// we need to replace 
			Map<String, Object> singleReplacement = this.replacements.get(0);
			
			// set the values for the replacement
			this.origPixelPortion = nodeString;
			this.replacementSourcePixel = (String) singleReplacement.get("pixel");
			
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
					break;
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
	public static void main(String[] args) {
		Gson gson = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.setPrettyPrinting()
				.create();
		
		// grab an example recipe
		LOGGER.info("TRYING WITH IMPLICIT REPLACEMENT OF FIRST SOURCE");
		String expression = "AddPanel ( 0 ) ;Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eUnfilterFrame(\u003cSelectedColumn\u003e);\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eif(IsEmpty(\u003cSelectedValues\u003e), UnfilterFrame(\u003cSelectedColumn\u003e), SetFrameFilter(\u003cSelectedColumn\u003e\u003d\u003d\u003cSelectedValues\u003e));\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;Panel ( 0 ) | RetrievePanelEvents ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" , \"\u003cencode\u003e{\"type\":\"echarts\"}\u003c/encode\u003e\" ) ;Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"\u003cencode\u003e{\"core_engine\":\"NEWSEMOSSAPP\"}\u003c/encode\u003e\" ) ;CreateFrame ( Grid ) .as ( [ \u0027FRAME549443\u0027 ] ) ;GoogleSheetSource ( id \u003d [ \"1EZbv_mXn_tnguDG02awFwQ30EqGMoWKZBflUVlcLgxY\" ] , sheetNames \u003d [ \"diabetes\" ] , type \u003d [ \".spreadsheet\" ] ) | Import ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel \u003d [ \"0\" ] , layout \u003d [ \"Grid\" ] ) | Collect ( 500 ) ;if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;Panel(0)|AddPanelOrnaments({\"showMenu\":true});Panel(0)|RetrievePanelOrnaments(\"showMenu\");";
		
		List<Map<String, Object>> testReplacements = new Vector<Map<String, Object>>();
		Map<String, Object> replacementMap = new HashMap<String, Object>();
		String replacementPixelToUse = "FileRead(filePath=[\"C:/workspace/Semoss_Dev/Movie_Data2018_03_27_13_08_21_0875.csv\"],dataTypeMap=[{\"Nominated\":\"STRING\",\"Title\":\"STRING\",\"Genre\":\"STRING\",\"Studio\":\"STRING\",\"Director\":\"STRING\",\"Revenue_Domestic\":\"NUMBER\",\"MovieBudget\":\"NUMBER\",\"Revenue_International\":\"NUMBER\",\"RottenTomatoes_Critics\":\"NUMBER\",\"RottenTomatoes_Audience\":\"NUMBER\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"Movie_Data\"])|Select(DND__Nominated, DND__Title, DND__Genre, DND__Studio, DND__Director, DND__Revenue_Domestic, DND__MovieBudget, DND__Revenue_International, DND__RottenTomatoes_Critics, DND__RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience])|Import();Panel(0)|SetPanelView(\"visualization\");Frame()|Select(Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience])|Format(type=['table'])|TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Nominated\",\"Title\",\"Genre\",\"Studio\",\"Director\",\"Revenue_Domestic\",\"MovieBudget\",\"Revenue_International\",\"RottenTomatoes_Critics\",\"RottenTomatoes_Audience\"]}}})";
		replacementMap.put("pixel", replacementPixelToUse);
		
		testReplacements.add(replacementMap);
		
		ReplaceDatasourceTranslation translation = new ReplaceDatasourceTranslation();
		translation.setReplacements(testReplacements);
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
		
		List<String> sourcePixels = translation.getPixels();
		System.out.println(gson.toJson(sourcePixels));
		
		LOGGER.info("");
		LOGGER.info("");

		LOGGER.info("TRYING WITH EXPLICIT REPLACEMENT INDEX");
		expression = "AddPanel ( 0 ) ;Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eUnfilterFrame(\u003cSelectedColumn\u003e);\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eif(IsEmpty(\u003cSelectedValues\u003e), UnfilterFrame(\u003cSelectedColumn\u003e), SetFrameFilter(\u003cSelectedColumn\u003e\u003d\u003d\u003cSelectedValues\u003e));\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;Panel ( 0 ) | RetrievePanelEvents ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" , \"\u003cencode\u003e{\"type\":\"echarts\"}\u003c/encode\u003e\" ) ;Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"\u003cencode\u003e{\"core_engine\":\"NEWSEMOSSAPP\"}\u003c/encode\u003e\" ) ;CreateFrame ( Grid ) .as ( [ \u0027FRAME549443\u0027 ] ) ;GoogleSheetSource ( id \u003d [ \"1EZbv_mXn_tnguDG02awFwQ30EqGMoWKZBflUVlcLgxY\" ] , sheetNames \u003d [ \"diabetes\" ] , type \u003d [ \".spreadsheet\" ] ) | Import ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel \u003d [ \"0\" ] , layout \u003d [ \"Grid\" ] ) | Collect ( 500 ) ;if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;Panel(0)|AddPanelOrnaments({\"showMenu\":true});Panel(0)|RetrievePanelOrnaments(\"showMenu\");";
		
		testReplacements = new Vector<Map<String, Object>>();
		replacementMap = new HashMap<String, Object>();
		replacementPixelToUse = "FileRead(filePath=[\"C:/workspace/Semoss_Dev/Movie_Data2018_03_27_13_08_21_0875.csv\"],dataTypeMap=[{\"Nominated\":\"STRING\",\"Title\":\"STRING\",\"Genre\":\"STRING\",\"Studio\":\"STRING\",\"Director\":\"STRING\",\"Revenue_Domestic\":\"NUMBER\",\"MovieBudget\":\"NUMBER\",\"Revenue_International\":\"NUMBER\",\"RottenTomatoes_Critics\":\"NUMBER\",\"RottenTomatoes_Audience\":\"NUMBER\"}],delimiter=[\",\"],newHeaders=[{}],fileName=[\"Movie_Data\"])|Select(DND__Nominated, DND__Title, DND__Genre, DND__Studio, DND__Director, DND__Revenue_Domestic, DND__MovieBudget, DND__Revenue_International, DND__RottenTomatoes_Critics, DND__RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience])|Import();Panel(0)|SetPanelView(\"visualization\");Frame()|Select(Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience).as([Nominated, Title, Genre, Studio, Director, Revenue_Domestic, MovieBudget, Revenue_International, RottenTomatoes_Critics, RottenTomatoes_Audience])|Format(type=['table'])|TaskOptions({\"0\":{\"layout\":\"Grid\",\"alignment\":{\"label\":[\"Nominated\",\"Title\",\"Genre\",\"Studio\",\"Director\",\"Revenue_Domestic\",\"MovieBudget\",\"Revenue_International\",\"RottenTomatoes_Critics\",\"RottenTomatoes_Audience\"]}}})";
		replacementMap.put("pixel", replacementPixelToUse);
		replacementMap.put("index", 6);

		testReplacements.add(replacementMap);
		
		translation = new ReplaceDatasourceTranslation();
		translation.setReplacements(testReplacements);
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
		
		sourcePixels = translation.getPixels();
		System.out.println(gson.toJson(sourcePixels));
	}

	
}
