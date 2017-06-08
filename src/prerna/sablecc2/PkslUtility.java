package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class PkslUtility {

	private static final Logger LOGGER = LogManager.getLogger(PkslUtility.class.getName());
	
	/**
	 * 
	 * @param pkslExpression
	 * @return
	 * 
	 * Returns true if the pksl is a valid pksl that can be executed
	 * Returns false otherwise
	 * @throws IOException 
	 * @throws LexerException 
	 * @throws ParserException 
	 */
	public static Set<String> validatePksl(String pkslExpression) throws ParserException, LexerException, IOException {
		Set<String> set = PKSLRunner.validatePKSL(pkslExpression);
		return set;
	}
	
	/**
	 * 
	 * @param pkslExpression
	 * @return
	 * @throws ParserException
	 * @throws LexerException
	 * @throws IOException
	 * 
	 * Returns a list of the parsed pksls from the expression
	 */
	public static List<String> parsePksl(String pkslExpression) throws ParserException, LexerException, IOException {
		return PKSLRunner.parsePKSL(pkslExpression);
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
			noun = new NounMetadata(((Number)value).doubleValue(), PkslDataTypes.CONST_DECIMAL);
		} else if(value instanceof String) {
			
			if(isLiteral((String)value)) {
				//we have a literal
				String literal = removeSurroundingQuotes((String)value);
				noun = new NounMetadata(literal, PkslDataTypes.CONST_STRING);
			} else {
				// try to convert to a number
				try {
					double doubleValue = Double.parseDouble(value.toString().trim());
					noun = new NounMetadata(doubleValue, PkslDataTypes.CONST_DECIMAL);
				} catch(NumberFormatException e) {
					// confirmed that it is not a double
					// and that we have a column
					noun = new NounMetadata(value.toString().trim(),PkslDataTypes.COLUMN);
				}
			}
		} else {
			throw new IllegalArgumentException("Value must be a number or string!");
		}
		
		return noun;
	}
	
	public static String generatePKSLString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	public static String generatePKSLString(String assignment, Object value) {
		return generatePKSLString(assignment, value.toString());	
	}
	
	
	/**
	 * 
	 * @param planner
	 * @param pkslString
	 * 
	 * Adds a pkslString to the planner via lazy translation
	 */
	public static void addPkslToTranslation(DepthFirstAdapter translation, List<String> pkslList) {
		/****** For Debugging *******/
		int count = 0;
		int errorCount = 0;
		List<String> errorList = new Vector<String>();
		/****** For Debugging *******/

		LinkedHashSet s = new LinkedHashSet<>(pkslList);
		if(s.size() == pkslList.size()) {
			LOGGER.info("YAY!!!!");
		} else {
			LOGGER.info("SAD!!!");
		}
		
		int numPksls = pkslList.size();
		for(int i = 0; i < numPksls; i++) {
			count++;
			String pkslString = pkslList.get(i);
//			System.out.println(pkslString);
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
				Start tree = p.parse();
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				/****** For Debugging *******/
				errorCount++;
				errorList.add(pkslString);
//				LOGGER.error("FAILED ON :::: " + pkslString);
//				e.printStackTrace();
				/****** For Debugging *******/
			} catch(Exception e) {
				/****** For Debugging *******/
				errorCount++;
				errorList.add(pkslString);
//				LOGGER.error("FAILED ON :::: " + pkslString);
//				e.printStackTrace();
				/****** For Debugging *******/
			}
		}
		
		/****** For Debugging *******/
		LOGGER.info("*************** TOTAL = " + count + "***************" );
		LOGGER.error("*************** FAILED = " + errorCount + "***************" );
		for(int i = 0; i < errorCount; i++) {
			LOGGER.error("\tFAILED WITH :::: " + errorList.get(i));
		}
		/****** For Debugging *******/
	}
	
	/**
	 * 
	 * @param planner
	 * @param pkslString
	 * 
	 * Adds a pkslString to the planner via lazy translation
	 */
	public static void addPkslToTranslation(DepthFirstAdapter translation, String pkslString) {
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
			Start tree = p.parse();
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			LOGGER.error("FAILED ON :::: " + pkslString);
			e.printStackTrace();
		} catch(Exception e) {
			LOGGER.error("FAILED ON :::: " + pkslString);
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
}
