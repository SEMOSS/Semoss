package prerna.sablecc2;

import java.io.IOException;
import java.util.List;

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
	 */
	public static boolean validatePksl(String pkslExpression) {
		try {
			parsePksl(pkslExpression);
		} catch (ParserException | LexerException | IOException e) {
			//exception thrown, this is not a valid pksl
			e.printStackTrace();
			return false;
		}
		
		//if no exception we were able to parse
		return true;
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
