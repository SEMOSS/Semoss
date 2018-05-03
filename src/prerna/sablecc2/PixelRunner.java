package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class PixelRunner {

	/**
	 * Runs a given pixel expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pixel expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pixel expression on
	 */
	
	private Insight insight = null;
	private List<NounMetadata> results = new Vector<NounMetadata>();
	private List<String> pixelExpression = new Vector<String>();
	private List<Boolean> isMeta = new Vector<Boolean>();
	private Map<String, String> encodedTextToOriginal = new HashMap<String, String>();
	private boolean invalidSyntax = false;
	
	public void runPixel(String expression, Insight insight) {
		this.insight = insight;
		expression = PixelPreProcessor.preProcessPixel(expression.trim(), this.encodedTextToOriginal);
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
			DepthFirstAdapter translation = new GreedyTranslation(this, insight);

			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch(SemossPixelException e) {
			addResult(expression, e.getNoun(), false);
			if(!e.isContinueThreadOfExecution()) {
				throw e;
			}
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
			String eMessage = e.getMessage();
			if(eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if(matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length()-1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					
					// trying to figure out the correct location of the error
					// so, we send a META | Job automatically
					// and we want to account for that
					if(expression.startsWith("META | Job(")) {
						int firstSemicolon = expression.indexOf(";");
						String subExpression = expression.substring(firstSemicolon+1);
						eMessage += ". Error in syntax around " + subExpression.substring(Math.max(findIndex - firstSemicolon - 10, 0), Math.min(findIndex - firstSemicolon + 10, subExpression.length())).trim();
					} else {
						eMessage += ". Error in syntax around " + expression.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, expression.length())).trim();
					}
				}
			}
			this.invalidSyntax = true;
			addResult(expression, new NounMetadata(eMessage, PixelDataType.INVALID_SYNTAX, PixelOperationType.INVALID_SYNTAX), false);
		}
		return;
	}
	
	public void addResult(String pixelExpression, NounMetadata result, boolean isMeta) {
		this.pixelExpression.add(pixelExpression);
		this.results.add(result);
		this.isMeta.add(isMeta);
	}
	
	public List<NounMetadata> getResults() {
		return this.results;
	}
	
	public List<String> getPixelExpressions() {
		return this.pixelExpression;
	}
	
	public List<Boolean> isMeta() {
		return this.isMeta;
	}
	
	public Map<String, String> getEncodedTextToOriginal() {
		return this.encodedTextToOriginal;
	}

	public boolean isInvalidSyntax() {
		return invalidSyntax;
	}
	
	public Insight getInsight() {
		return this.insight;
	}
	
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	////////////////////////////////////////////////////////
	
	/*
	 * Other methods here
	 */
	
	public static void main(String[] args) throws Exception {
		String pixel = "A = 10; B = \"Apple\";";
		List<String> x = parsePixel(pixel);
		System.out.println(x);
	}
	
	/**
	 * 
	 * @param expression
	 * @return
	 * 
	 * Method to take a string and return the parsed value of the pixel
	 */
	public static List<String> parsePixel(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
		Start tree = p.parse();

		AConfiguration configNode = (AConfiguration)tree.getPConfiguration();

		List<String> pixelList = new ArrayList<>();
		for(PRoutine script : configNode.getRoutine()) {
			pixelList.add(script.toString());
		}
		return pixelList;
	}	
	
	/**
	 * 
	 * @param expression
	 * @return
	 * 
	 * returns set of reactors that are not implemented
	 * throws exception if pixel cannot be parsed
	 */
	public static Set<String> validatePixel(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
		ValidatorTranslation translation = new ValidatorTranslation();
		// parsing the pixel - this process also determines if expression is syntactically correct
		Start tree = p.parse();
		// apply the translation.
		tree.apply(translation);
		return translation.getUnimplementedReactors();
	}
}
