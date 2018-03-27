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
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AExprColDef;
import prerna.sablecc2.node.AGenRow;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class DatasourceTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(DatasourceTranslation.class.getName());

	private List<Map<String, Object>> datasourcePixels = new Vector<Map<String, Object>>();

	private Map<String, Object> currentSourceStatement = null;
	private List<String> currentSourceParamInput = null;
	
	@Override
	public void caseAConfiguration(AConfiguration node) {
		int currentIndex = 0;
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
			String expression = e.toString();
			LOGGER.info("Processing " + expression);
			e.apply(this);
			
			// if we ended up finding something to store
			if(this.currentSourceStatement != null) {
				currentSourceStatement.put("pixelStepIndex", currentIndex);
				// add the source to store
				this.datasourcePixels.add(currentSourceStatement);
				// at the end, we will null it
				this.currentSourceStatement = null;
			}
			
			// update the index
			currentIndex++;
		}
	}

	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);
		
		// looking for data sources
		String reactorId = node.getId().toString().trim();
		if(reactorId.equals("FileRead")) {
			this.currentSourceStatement = new HashMap<String, Object>();
			this.currentSourceStatement.put("expression", node.toString());
			this.currentSourceStatement.put("type", "FileRead");
		} 
		else if(reactorId.equals("GoogleSheetSource")) {
			this.currentSourceStatement = new HashMap<String, Object>();
			this.currentSourceStatement.put("expression", node.toString());
			this.currentSourceStatement.put("type", "GoogleSheetSource");
		}
	}
	
	////////////////////////////////////////////
	
	/*
	 * START
	 * Section for finding parameters for the datasource
	 */
	
	@Override
	public void outAGeneric(AGeneric node) {
		if(currentSourceStatement != null) {
			Map<String, List<String>> inputs = (Map<String, List<String>>) currentSourceStatement.get("params");
			if(inputs == null) {
				inputs = new HashMap<String, List<String>>();
				this.currentSourceStatement.put("params", inputs);
			}
					
			String paramKey = node.getId().toString().trim();
			inputs.put(paramKey, this.currentSourceParamInput);
			
			// nullify the source param input
			this.currentSourceParamInput = null;
		}
	}
	
	@Override
	public void inAGenRow(AGenRow node) {
		if(this.currentSourceStatement != null) {
			// we are starting a new gen row struct
			this.currentSourceParamInput = new Vector<String>();
		}
	}
	
	@Override
	public void outAExprColDef(AExprColDef node) {
		if(this.currentSourceParamInput != null) {
			this.currentSourceParamInput.add(node.toString().trim());
		}
	}
	
	
	/*
	 * END
	 * Section for finding parameters for the datasource
	 */
	
	////////////////////////////////////////////

	public List<Map<String, Object>> getDatasourcePixels() {
		return this.datasourcePixels;
	}
	
	/**
	 * Testing method
	 * @param args
	 */
	public static void main(String[] args) {
		// grab an example recipe
		
		String expression = "AddPanel ( 0 ) ;Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eUnfilterFrame(\u003cSelectedColumn\u003e);\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"\u003cencode\u003eif(IsEmpty(\u003cSelectedValues\u003e), UnfilterFrame(\u003cSelectedColumn\u003e), SetFrameFilter(\u003cSelectedColumn\u003e\u003d\u003d\u003cSelectedValues\u003e));\u003c/encode\u003e\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;Panel ( 0 ) | RetrievePanelEvents ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" , \"\u003cencode\u003e{\"type\":\"echarts\"}\u003c/encode\u003e\" ) ;Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"\u003cencode\u003e{\"core_engine\":\"NEWSEMOSSAPP\"}\u003c/encode\u003e\" ) ;CreateFrame ( Grid ) .as ( [ \u0027FRAME549443\u0027 ] ) ;GoogleSheetSource ( id \u003d [ \"1EZbv_mXn_tnguDG02awFwQ30EqGMoWKZBflUVlcLgxY\" ] , sheetNames \u003d [ \"diabetes\" ] , type \u003d [ \".spreadsheet\" ] ) | Import ( ) ;Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel \u003d [ \"0\" ] , layout \u003d [ \"Grid\" ] ) | Collect ( 500 ) ;if ( ( HasDuplicates ( Drug ) ) , ( Select ( Drug , Average ( id ) ) .as ( [ Drug , Averageofid ] ) | Group ( Drug ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"Averageofid\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Drug , id ) .as ( [ Drug , id ] ) | With ( Panel ( 0 ) ) | Format ( type \u003d [ \u0027table\u0027 ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Pie\" , \"alignment\" : { \"label\" : [ \"Drug\" ] , \"value\" : [ \"id\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;Panel(0)|AddPanelOrnaments({\"showMenu\":true});Panel(0)|RetrievePanelOrnaments(\"showMenu\");";
		
		DatasourceTranslation translation = new DatasourceTranslation();
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
		
		Gson gson = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
				.setPrettyPrinting()
				.create();
		
		List<Map<String, Object>> sourcePixels = translation.getDatasourcePixels();
		System.out.println(gson.toJson(sourcePixels));
	}


}