package prerna.sablecc2;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.om.Insight;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class PKSLRunner {

	/**
	 * Runs a given pksl expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pkql expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pkql expression on
	 */

	private List<NounMetadata> results = new Vector<NounMetadata>();
	private List<String> pkslExpression = new Vector<String>();
	
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

	public void runPKSL(String expression, Insight insight) {
		expression = preProcessPksl(expression);
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(expression)), expression.length())));
		DepthFirstAdapter translation = new GreedyTranslation(this, insight);

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
	
	/**
	 * Pre-process the pksl to encode values that would not be allowed based on the grammar
	 * @param expression
	 * @return
	 */
	private String preProcessPksl(String expression) {
		String newExpression = expression;
		
		Pattern p = Pattern.compile("<encode>.+?</encode>");
		Matcher m = p.matcher(newExpression);
		while(m.find()) {
			String originalText = m.group(0);
			String encodedText = originalText.replace("<encode>", "").replace("</encode>", "");
			try {
				encodedText = URLEncoder.encode(encodedText, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// well, you are kinda screwed...
				// this replacement will definitley fail
				e.printStackTrace();
			}
			newExpression = newExpression.replace(originalText, encodedText);
		}
		
		return newExpression;
	}

	public void addResult(String pkslExpression, NounMetadata result) {
		this.pkslExpression.add(pkslExpression);
		this.results.add(result);
	}
	
	public List<NounMetadata> getResults() {
		return this.results;
	}
	
	public List<String> getPkslExpressions() {
		return this.pkslExpression;
	}
	
}
