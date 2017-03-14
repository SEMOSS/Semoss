package prerna.sablecc2;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc.PKQLRunner;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.node.PScript;
import prerna.sablecc2.Translation;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class PKSLRunner {
	
	public enum STATUS {SUCCESS, ERROR, INPUT_NEEDED}
	
	private STATUS currentStatus = PKSLRunner.STATUS.SUCCESS;
	private Object response = "PKSL processing complete"; //i feel as if this should be an object coming from the reactor...not a class variable
	private String explain = "";
	private HashMap<String,Object> allAdditionalInfo = new HashMap<>();
	private String additionalInfoString = "";
	private Object returnData = null;
	private List<Map> newInsights = new ArrayList<>();
	private boolean dataCleared = false;
	
	private Map<String,String> newColumns = new HashMap<String,String>();
	private Map<String, Map<String,Object>> masterFeMap = new HashMap<String, Map<String,Object>>(); // this holds all active front end data. in the form panelId --> prop --> value

	//	private Map<String, List<Map<String, Object>>> expiredFeMaps =  new HashMap<String, List<Map<String,Object>>>();
	
	private Map<String, Object> activeFeMap; // temporally grabbed out of master
	private List<Map> responseArray = new Vector<Map>();
	private Map<String, Map<String, Object>> varMap = new HashMap<String, Map<String, Object>>();
	LinkedList<PScript> pkqlToRun = new LinkedList<PScript>();
	List<String> unassignedVars = new Vector<String>();
	
//	private Map<String, Object> dataMap = new HashMap<>();
	
	private List<IPkqlMetadata> metadataResponse = new Vector<IPkqlMetadata>();
	
	// there is a getter for this
	// but we never set this... so not used
//	private String newInsightID;
	
	private Map dashboardMap;
	
	
	
	/**
	 * Runs a given pksl expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pkql expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pkql expression on
	 */
	
	private Translation translation;
	private IDataMaker dataMaker;
	private String insightId;
	private PKSLPlanner planner;
	
	public void runPKSL(String expression, IDataMaker frame) {
		
		this.dataMaker = frame;
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), 1024)));
		Start tree;
//		if(planner == null) {
//			planner = new PKSLPlanner();
//		}
		if(translation == null){
			translation = new Translation(frame, this);
		}

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public void setInsightId(String insightId) {
		this.insightId = insightId;
	}
	public String getInsightId() {
		return this.insightId;
	}

	public IDataMaker getDataFrame() {
		if(translation != null) {
			return translation.getDataMaker();
		}
		return null;
	}

	public void setDataFrame(IDataMaker frame) {
		this.dataMaker = frame;
	}

	public Object getResults() {
		int dataId = dataMaker.getDataId();
		return translation.getResults();
	}
	
	public PKSLPlanner getPlanner() {
		return this.planner;
	}
}
