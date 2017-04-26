package prerna.sablecc2;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.parser.ParserException;

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
}
