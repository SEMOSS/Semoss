package prerna.sablecc2;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class PKSLRunner {

	/**
	 * Runs a given pksl expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pkql expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pkql expression on
	 */

	private GreedyTranslation translation;
	private IDataMaker dataMaker;
	private String insightId;
	private PKSLPlanner planner;
	private NounMetadata result;

	public static void main(String[] args) throws Exception {
		String pksl = "A = 10; B = \"Apple\";";
		List<String> x = parsePKSL(pksl);
		System.out.println(x);
	}

	/**
	 * 
	 * @param expression
	 * @return
	 * 
	 * Method to take a string and return the parsed value of the pksl
	 */
	public static List<String> parsePKSL(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		Start tree = p.parse();

		AConfiguration configNode = (AConfiguration)tree.getPConfiguration();

		List<String> pksls = new ArrayList<>();
		for(PRoutine script : configNode.getRoutine()) {
			pksls.add(script.toString());
		}
		return pksls;
	}
	
	/**
	 * 
	 * @param expression
	 * @return
	 * 
	 * returns set of reactors that are not implemented
	 * throws exception if pksl cannot be parsed
	 */
	public static Set<String> validatePKSL(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		ValidatorTranslation translation = new ValidatorTranslation();
		// parsing the pkql - this process also determines if expression is syntactically correct
		Start tree = p.parse();
		// apply the translation.
		tree.apply(translation);
		return translation.getUnimplementedReactors();
	}

	public void runPKSL(String expression) {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		if(translation == null){
			translation = new GreedyTranslation(this);
		}

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public void runPKSL(String expression, IDataMaker frame) {
		this.dataMaker = frame;
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		Start tree;
		if(translation == null){
			translation = new GreedyTranslation(this, frame);
		}
		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		} finally {
			translation.postProcess();
		}
		return;
	}
	
	public void runPKSL(String expression, VarStore varStore) {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		if(translation == null){
			translation = new GreedyTranslation(this, varStore);
		}

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public void runPKSL(String expression, VarStore varStore, IDataMaker frame) {
		this.dataMaker = frame;
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		Start tree;
		if(translation == null){
			translation = new GreedyTranslation(this, varStore, frame);
		}
		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException | RuntimeException e) {
			e.printStackTrace();
		} finally {
			translation.postProcess();
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
		return translation.getResults();
	}

	public PKSLPlanner getPlanner() {
		return this.planner;
	}

	public void setResult(NounMetadata result) {
		if(result != null) {
			this.result = result;
		}
	}
	
	public NounMetadata getLastResult() {
		return this.result;
	}
	
}
