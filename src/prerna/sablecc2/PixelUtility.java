package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class PixelUtility {

	private static final Logger LOGGER = LogManager.getLogger(PixelUtility.class.getName());
	
	/**
	 * 
	 * @param pixelExpression
	 * @return
	 * 
	 * Returns true if the pixel is a valid pixel that can be executed
	 * Returns false otherwise
	 * @throws IOException 
	 * @throws LexerException 
	 * @throws ParserException 
	 */
	public static Set<String> validatePixel(String pixelExpression) throws ParserException, LexerException, IOException {
		Set<String> set = PixelRunner.validatePixel(pixelExpression);
		return set;
	}
	
	/**
	 * 
	 * @param pixelExpression
	 * @return
	 * @throws ParserException
	 * @throws LexerException
	 * @throws IOException
	 * 
	 * Returns a list of the parsed pixels from the expression
	 */
	public static List<String> parsePixel(String pixelExpression) throws ParserException, LexerException, IOException {
		return PixelRunner.parsePixel(pixelExpression);
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 * 
	 * Returns the noun for a STRING or NUMBER ONLY
	 * if value is an instanceof another object IllegalArgumentException will be thrown
	 */
	public static NounMetadata getNoun(Object value) {
		NounMetadata noun = null;
		if(value instanceof Number) {
			noun = new NounMetadata(((Number)value).doubleValue(), PixelDataType.CONST_DECIMAL);
		} else if(value instanceof String) {
			
			if(isLiteral((String)value)) {
				//we have a literal
				String literal = removeSurroundingQuotes((String)value);
				noun = new NounMetadata(literal, PixelDataType.CONST_STRING);
			} else {
				// try to convert to a number
				try {
					double doubleValue = Double.parseDouble(value.toString().trim());
					noun = new NounMetadata(doubleValue, PixelDataType.CONST_DECIMAL);
				} catch(NumberFormatException e) {
					// confirmed that it is not a double
					// and that we have a column
					noun = new NounMetadata(value.toString().trim(),PixelDataType.COLUMN);
				}
			}
		} else {
			throw new IllegalArgumentException("Value must be a number or string!");
		}
		
		return noun;
	}
	
	public static String generatePixelString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	public static String generatePixelString(String assignment, Object value) {
		return generatePixelString(assignment, value.toString());	
	}
	
	
	/**
	 * 
	 * @param planner
	 * @param pixelString
	 * 
	 * Adds a pkslString to the planner via lazy translation
	 */
	public static void addPixelToTranslation(DepthFirstAdapter translation, String pixelString) {
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pixelString.getBytes("UTF-8"))))));
			Start tree = p.parse();
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			LOGGER.error("FAILED ON :::: " + pixelString);
			e.printStackTrace();
		} catch(Exception e) {
			LOGGER.error("FAILED ON :::: " + pixelString);
			e.printStackTrace();
		}
	}
	
	/**
	 * @param literal
	 * @return
	 * 
	 * input: 'literal' OR "literal"
	 * output: literal
	 * 
	 * input: 'literal OR "literal
	 * output: literal
	 * 
	 * input: literal' OR literal"
	 * output: literal
	 * 
	 * input: literal
	 * output: literal
	 */
	public static String removeSurroundingQuotes(String literal) {
		literal = literal.trim();
		if(literal.startsWith("\"") || literal.startsWith("'")) {
			literal = literal.substring(1); //remove the first quote
		}
		
		if(literal.endsWith("\"") || literal.endsWith("'")) {
			if(literal.length() == 1) {
				literal = "";
			} else {
				literal = literal.substring(0, literal.length()-1); //remove the end quote
			}
		}
		
		return literal;
	}
	
	/**
	 * 
	 * @param literal
	 * @return
	 * 
	 * Determines if the string is a literal
	 */
	public static boolean isLiteral(String literal) {
		literal = literal.trim();
		return ((literal.startsWith("\"") || literal.startsWith("'")) && (literal.endsWith("\"") || literal.endsWith("'")));
	}
	
	/**
	 * Checks if recipe has a param
	 * @param pixels
	 * @return
	 */
	public static boolean hasParam(String[] pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.hasParam(recipe);
	}
	
	/**
	 * Checks if recipe has a param
	 * @param pixels
	 * @return
	 */
	public static boolean hasParam(List<String> pixels) {
		String recipe = String.join("", pixels);
		return PixelUtility.hasParam(recipe);
	}
	
	/**
	 * Checks if recipe has a param
	 * @param pixel
	 * @return
	 */
	public static boolean hasParam(String pixel) {
		pixel = PixelPreProcessor.preProcessPixel(pixel, new HashMap<String, String>());
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(pixel)), pixel.length())));
		InsightParamTranslation translation = new InsightParamTranslation();
		try {
			// parsing the pixel - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
			return translation.hasParam();
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 
	 * @param expression
	 * @param encodedTextToOriginal
	 * @return
	 * 
	 * Returns the string of the NOT encoded expression
	 * allows us to get the expression in its not encoded form from a mapping of the expression to what it looked like originally
	 */
	public static String recreateOriginalPixelExpression(String expression, Map<String, String> encodedTextToOriginal) {
		if(encodedTextToOriginal == null || encodedTextToOriginal.isEmpty()) {
			return expression;
		}
		// loop through and see if any encodedText portions have been modified
		// if they have, try and replace them back so it looks pretty for the FE
		for(String encodedText : encodedTextToOriginal.keySet()) {
			if(expression.contains(encodedText)) {
				expression = expression.replace(encodedText, encodedTextToOriginal.get(encodedText));
			}
		}
		return expression;
	}
	
	/**
	 * Determine if an operation op type is an error that requires user input and then a re-run of the insight
	 * @param opTypes
	 * @return
	 */
	public static boolean autoExecuteAfterUserInput(List<PixelOperationType> opTypes) {
		if(opTypes.contains(PixelOperationType.GOOGLE_LOGIN_REQUIRED)) {
			return true;
		}
		return false;
	}
}
