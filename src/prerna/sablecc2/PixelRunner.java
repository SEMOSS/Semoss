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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cern.colt.Arrays;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class PixelRunner {

	private static final Logger logger = LogManager.getLogger(PixelRunner.class);

	/**
	 * Runs a given pixel expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pixel expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pixel expression on
	 */
	
	protected transient GreedyTranslation translation = null;
	protected Insight insight = null;
	
	protected List<NounMetadata> results = new Vector<>();
	protected List<String> pixelExpression = new Vector<>();
	protected List<Boolean> isMeta = new Vector<>();
	
	protected List<String> encodingList = new Vector<>();
	protected Map<String, String> encodedTextToOriginal = new HashMap<>();
	
	public void runPixel(String expression, Insight insight) {
		this.insight = insight;
		expression = PixelPreProcessor.preProcessPixel(expression.trim(), this.encodingList, this.encodedTextToOriginal);
		
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8")), "UTF-8"), expression.length())));
			translation = new GreedyTranslation(this, insight);

			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch(SemossPixelException e) {
			if(!e.isContinueThreadOfExecution()) {
				throw e;
			}
		} catch (ParserException | LexerException | IOException e) {
			// we only need to catch invalid syntax here
			// other exceptions are caught in lazy translation
			trackInvalidSyntaxError(expression, e);
			logger.error("StackTrace: ", e);
			String eMessage = e.getMessage();
			if(eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if(matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length()-1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + expression.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, expression.length())).trim();
				}
			}
			// treat this as a META so that FE doesn't record it
			addInvalidSyntaxResult(expression, new NounMetadata(eMessage, PixelDataType.INVALID_SYNTAX, PixelOperationType.ERROR, PixelOperationType.INVALID_SYNTAX), false);
		} finally {
			// help clean up
			if (translation != null) {
				PixelPlanner planner = translation.getPlanner();
				planner.dropGraph();
				planner.getVarStore().remove("$RESULT");
			}
			this.encodingList.clear();
			this.encodedTextToOriginal.clear();
		}
	}
	
	/**
	 * Track the error
	 * @param pixel
	 * @param ex
	 */
	private void trackInvalidSyntaxError(String pixel, Exception ex) {
		IUserTracker tracker = UserTrackerFactory.getInstance();
		if(tracker.isActive()) {
			tracker.trackError(this.insight, pixel, "INVALID_SYNTAX", "INVALID_SYNTAX", false, ex);
		}
	}
	
	/**
	 * Store the terminal output of the pixel statement
	 * @param pixelExpression
	 * @param result
	 * @param isMeta
	 */
	public void addResult(String pixelExpression, NounMetadata result, boolean isMeta, Map<String, List<Map>> reactorInput) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList, this.encodedTextToOriginal);
		this.pixelExpression.add(origExpression);
		this.results.add(result);
		this.isMeta.add(isMeta);
		
		// we will start to add to the insight recipe
		// when we have an expression that is returned
		// that is not a meta
		if(!isMeta) {
			Pixel pixel = this.insight.getPixelList().addPixel(origExpression);
			pixel.setStartingFrameHeaders(translation.startingFrameHeaders);
			pixel.setEndingFrameHeaders(InsightUtility.getAllFrameHeaders(this.insight.getVarStore()));
			pixel.setReactorInput(reactorInput);
		}
	}
	
	/**
	 * Store the terminal output of the pixel statement
	 * This is used when trying to modify the pixel directly outside
	 * of the normal pixel execution flow
	 * @param index
	 * @param pixelExpression
	 * @param result
	 * @param isMeta
	 */
	public void addResult(int index, String pixelExpression, NounMetadata result, boolean isMeta) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList, this.encodedTextToOriginal);
		this.pixelExpression.add(index, origExpression);
		this.results.add(index, result);
		this.isMeta.add(index, isMeta);
		
		// we will start to add to the insight recipe
		// when we have an expression that is returned
		// that is not a meta
		if(!isMeta) {
			this.insight.getPixelList().addPixel(origExpression);
		}
	}
	
	/**
	 * Same as addResult but since this is an error we do not want to store it in the pixel recipe
	 * @param pixelExpression
	 * @param result
	 * @param isMeta
	 */
	private void addInvalidSyntaxResult(String pixelExpression, NounMetadata result, boolean isMeta) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList, this.encodedTextToOriginal);
		this.pixelExpression.add(origExpression);
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
	
	public Insight getInsight() {
		return this.insight;
	}
	
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	public void clear() {
		this.results.clear();
		this.pixelExpression.clear();
		this.isMeta.clear();
		this.encodingList.clear();
		this.encodedTextToOriginal.clear();
	}
	
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

	/*
	 * Other methods here
	 */
	
	public static void main(String[] args) throws Exception {
		String pixel = "A = 10; B = \"Apple\";";
		List<String> x = parsePixel(pixel);
		logger.info(Utility.cleanLogString(Arrays.toString(x.toArray())));
	}
	
	/**
	 * 
	 * @param expression
	 * @return
	 * 
	 * Method to take a string and return the parsed value of the pixel
	 */
	public static List<String> parsePixel(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8")), "UTF-8"), expression.length())));
		Start tree = p.parse();

		ARoutineConfiguration configNode = (ARoutineConfiguration)tree.getPConfiguration();

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
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8")), "UTF-8"), expression.length())));
		ValidatorTranslation translation = new ValidatorTranslation();
		// parsing the pixel - this process also determines if expression is syntactically correct
		Start tree = p.parse();
		// apply the translation.
		tree.apply(translation);
		return translation.getUnimplementedReactors();
	}
}
