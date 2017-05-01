package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.List;
import java.util.Set;

import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.PKSLPlanner;

public class PkslUtility {

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
	 * Get the appropriate noun from the object value from the iterator
	 */
	public static NounMetadata getNoun(Object value) {	
		//this sucks we have to do this here instead of translation handling it
		//need to figure out how to use sablecc to create the noun instead of us guessing
		NounMetadata noun = null;
		if(value instanceof Number) {
			noun = new NounMetadata(((Number)value).doubleValue(), PkslDataTypes.CONST_DECIMAL);
		} else if(value instanceof String) {
			if(value.toString().trim().startsWith("\"") || value.toString().trim().startsWith("\'")) {
				//we have a literal
				String literal = value.toString().trim();
				literal = literal.substring(1, literal.length()-1).trim();
				noun = new NounMetadata(literal, PkslDataTypes.CONST_STRING);
			} else {
				//we have a column
				noun = new NounMetadata(value.toString().trim(),PkslDataTypes.COLUMN);
			}
		} else {
			//i don't know
		}
		return noun;
	}
	
	public static String generatePKSLString(String assignment, String value) {
		return assignment+" = "+value+";";	
	}
	
	public static String generatePKSLString(String assignment, Object value) {
		return generatePKSLString(assignment, value.toString());	
	}
	
	public static void addPkslToPlanner(PKSLPlanner planner, String pkslString) {
		PlannerTranslation translation = new PlannerTranslation();
		translation.planner = planner;
		System.out.println("reset1");
		try {
			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pkslString.getBytes("UTF-8"))))));
			Start tree = p.parse();
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			System.out.println(pkslString);
			e.printStackTrace();
		} catch(Exception e) {
			System.out.println(pkslString);
			e.printStackTrace();
		}
	}
}
