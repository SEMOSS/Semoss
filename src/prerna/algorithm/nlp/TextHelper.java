package prerna.algorithm.nlp;

import prerna.util.ArrayUtilityMethods;


public final class TextHelper {
	
	private TextHelper() {
		
	}
	
	public static String splitCamelCase(String s) {
		   return s.replaceAll(
		      String.format("%s|%s|%s",
		         "(?<=[A-Z])(?=[A-Z][a-z])",
		         "(?<=[^A-Z])(?=[A-Z])",
		         "(?<=[A-Za-z])(?=[^A-Za-z])"
		      ),
		      " "
		   );
		}
	
	public static String[] breakCompoundText(String s) {
		s = splitCamelCase(s).replaceAll("_", " ").replaceAll("-", " ");
		return ArrayUtilityMethods.removeAllEmptyValuesAndNulls(s.split(" "));
	}
	
}
