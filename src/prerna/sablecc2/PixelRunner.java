package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cern.colt.Arrays;
import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.PixelPlanner;
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
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.usertracking.IUserTracker;
import prerna.util.usertracking.UserTrackerFactory;

public class PixelRunner {

	private static final Logger logger = LogManager.getLogger(PixelRunner.class);

	private static List<PixelOperationType> errorOpTypes = new ArrayList<>();
	static {
		errorOpTypes.add(PixelOperationType.ERROR);
		errorOpTypes.add(PixelOperationType.UNEXECUTED_PIXELS);
		errorOpTypes.add(PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED);
		errorOpTypes.add(PixelOperationType.USER_INPUT_REQUIRED);
		errorOpTypes.add(PixelOperationType.LOGGIN_REQUIRED_ERROR);
		errorOpTypes.add(PixelOperationType.ANONYMOUS_USER_ERROR);
		errorOpTypes.add(PixelOperationType.INVALID_SYNTAX);
	}
	
	/**
	 * Runs a given pixel expression (can be multiple if semicolon delimited) on a provided data maker 
	 * @param expression			The sequence of semicolon delimited pixel expressions.
	 * 								If just one expression, still must end with a semicolon
	 * @param frame					The data maker to run the pixel expression on
	 */
	
	protected transient GreedyTranslation translation = null;
	protected Insight insight = null;
	protected boolean maintainErrors = false;
	
	protected List<NounMetadata> results = new ArrayList<>();
	protected List<Pixel> returnPixelList = new ArrayList<>();

	protected List<String> encodingList = new ArrayList<>();
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
			logger.error(Constants.ERROR_MESSAGE, e);
			if(!e.isContinueThreadOfExecution()) {
				throw e;
			}
		} catch (ParserException | LexerException | IOException e) {
			// we only need to catch invalid syntax here
			// other exceptions are caught in lazy translation
			trackInvalidSyntaxError(expression, e);
			logger.error(Constants.STACKTRACE, e);
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
	public void addResult(String pixelExpression, NounMetadata result, boolean isMeta) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList, this.encodedTextToOriginal);
		this.results.add(result);
		
		// we will start to add to the insight recipe
		// when we have an expression that is returned
		// create the pixel via the correct id
		// or if meta - assign random one
		Pixel pixel = null;
		if(!isMeta) {
			PixelList pixelList = this.insight.getPixelList();
			pixel = pixelList.addPixel(origExpression);
			// add if there is an error or warning
			determineErrorOrWarning(pixel, result);
			pixel.setEndingFrameHeaders(InsightUtility.getAllFrameHeaders(this.insight.getVarStore()));
			Pixel.translationMerge(pixel, translation.pixelObj);
			if(pixel.isReturnedError() && !this.maintainErrors) {
				// we actually need to remove this from the pixel list
				// there is also no sync required 
				List<String> removeId = new ArrayList<String>();
				removeId.add(pixel.getId());
				pixelList.removeIds(removeId, false);
			} else {
				pixelList.syncLastPixel();
			}
			// store this pixel
			// in the return pixel list
			this.returnPixelList.add(pixel);
		} else {
			// store this pixel
			// in the return pixel list
			pixel = new Pixel("meta_unstored", origExpression);
			// make sure the pixel is set to meta
			pixel.setMeta(true);
			this.returnPixelList.add(pixel);
			// add if there is an error or warning
			determineErrorOrWarning(pixel, result);
		}
	}
	
	/**
	 * Store the terminal output of the pixel statement
	 * This is used when trying to modify the pixel directly outside
	 * of the normal pixel execution flow
	 * THIS IS ALWAYS A META - DOESN'T ADD TO THE INSIGHT RECIPE
	 * @param index
	 * @param pixelExpression
	 * @param result
	 */
	public void addResult(int index, String pixelExpression, NounMetadata result) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList, this.encodedTextToOriginal);
		Pixel pixel = new Pixel("meta_unstored", origExpression);
		pixel.setMeta(true);
		this.returnPixelList.add(index, pixel);
		this.results.add(index, result);
	}
	
	/**
	 * Same as addResult but since this is an error we do not want to store it in the pixel recipe
	 * @param pixelExpression
	 * @param result
	 * @param isMeta
	 */
	private void addInvalidSyntaxResult(String pixelExpression, NounMetadata result, boolean isMeta) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList, this.encodedTextToOriginal);
		Pixel pixel = new Pixel("meta_unstored", origExpression);
		pixel.setReturnedError(true);
		pixel.setMeta(true);
		this.returnPixelList.add(pixel);
		this.results.add(result);
	}
	
	private void determineErrorOrWarning(Pixel pixel, NounMetadata result) {
		// if the result is a direct error
		if(!Collections.disjoint(errorOpTypes, result.getOpType())) {
			pixel.setReturnedError(true);
			pixel.addErrorMessage(result.getValue() + "");
			return;
		}
		
		// if the result is a direct warning
		if(result.getOpType().contains(PixelOperationType.WARNING)) {
			pixel.setReturnedWarning(true);
			pixel.addWarningMessage(result.getValue() + "");
			return;
		}
		
		// if the result has an additional type
		if(result.getAdditionalReturn() != null) {
			for(NounMetadata addReturn : result.getAdditionalReturn()) {
				// check if add return is an error
				if(!Collections.disjoint(errorOpTypes, addReturn.getOpType())) {
					pixel.setReturnedError(true);
					pixel.addErrorMessage(result.getValue() + "");
					return;
				}
				
				// check if add return is a warning
				if(addReturn.getOpType().contains(PixelOperationType.WARNING)) {
					pixel.setReturnedWarning(true);
					pixel.addWarningMessage(result.getValue() + "");
					return;
				}
			}
		}
	}
	
	public List<NounMetadata> getResults() {
		return this.results;
	}
	
	public List<Pixel> getReturnPixelList() {
		return this.returnPixelList;
	}
	
	public Insight getInsight() {
		return this.insight;
	}
	
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	public boolean isMaintainErrors() {
		return maintainErrors;
	}

	public void setMaintainErrors(boolean maintainErrors) {
		this.maintainErrors = maintainErrors;
	}

	public void clear() {
		this.results.clear();
		this.returnPixelList.clear();
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
//	
//	public static void main(String[] args) throws Exception {
//		String pixel = "A = 10; B = \"Apple\";";
//		List<String> x = parsePixel(pixel);
//		logger.info(Utility.cleanLogString(Arrays.toString(x.toArray())));
//	}
	
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
