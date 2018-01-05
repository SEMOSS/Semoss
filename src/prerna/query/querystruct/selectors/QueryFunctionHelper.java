package prerna.query.querystruct.selectors;

public class QueryFunctionHelper {

	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String MEAN = "mean";
	public static final String AVERAGE = "average";
	public static final String MEDIAN = "median";
	public static final String SUM = "sum";
	public static final String STDEV = "standarddeviation";
	public static final String COUNT = "count";
	public static final String UNIQUE_COUNT = "uniquecount";
	public static final String CONCAT = "concat";
	public static final String GROUP_CONCAT = "groupconcat";
	public static final String UNIQUE_GROUP_CONCAT = "uniquegroupconcat";

	private QueryFunctionHelper() {
		
	}
	
	/**
	 * Convert the function name to ansi-sql syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToSqlSyntax(String inputFunction) {
		String lowerfunction = inputFunction.toLowerCase();
		if(lowerfunction.equals(MIN)) {
			inputFunction = "MIN";
		} else if(lowerfunction.equals(MAX)) {
			inputFunction = "MAX";
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE)) {
			inputFunction = "AVG";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "MEDIAN";
		} else if(lowerfunction.equals(SUM)) {
			inputFunction = "SUM";
		} else if(lowerfunction.equals(STDEV)) {
			inputFunction = "STDDEV_SAMP";
		} else if(lowerfunction.equals(COUNT)) {
			inputFunction = "COUNT";
		} else if(lowerfunction.equals(UNIQUE_COUNT)) {
			inputFunction = "COUNT";
		} else if(lowerfunction.equals(CONCAT)) {
			inputFunction = "CONCAT";
		} else if(lowerfunction.equals(GROUP_CONCAT)) {
			inputFunction = "GROUP_CONCAT";
		} else if(lowerfunction.equals(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "GROUP_CONCAT";
		}
		
		return inputFunction;
	}
	
	/**
	 * Convert the function name to r data.table syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToRSyntax(String inputFunction) {
		String lowerfunction = inputFunction.toLowerCase();
		if(lowerfunction.equals(MIN)) {
			inputFunction = "min";
		} else if(lowerfunction.equals(MAX)) {
			inputFunction = "max";
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE)) {
			inputFunction = "mean";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "median";
		} else if(lowerfunction.equals(SUM)) {
			inputFunction = "sum";
		} else if(lowerfunction.equals(STDEV)) {
			inputFunction = "sd";
		} else if(lowerfunction.equals(COUNT)) {
			inputFunction = "length";
		} else if(lowerfunction.equals(UNIQUE_COUNT)) {
			inputFunction = "uniqueN";
		} else if(lowerfunction.equals(CONCAT)) {
			inputFunction = "paste";
		} else if(lowerfunction.equals(GROUP_CONCAT)) {
			inputFunction = "paste";
		} else if(lowerfunction.equals(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "paste";
		}
		
		return inputFunction;
	}
	
	/**
	 * Convert the function name to sparql syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToSparqlSyntax(String inputFunction) {
		String lowerfunction = inputFunction.toLowerCase();
		if(lowerfunction.equals(MIN)) {
			inputFunction = "MIN";
		} else if(lowerfunction.equals(MAX)) {
			inputFunction = "MAX";
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE)) {
			inputFunction = "AVG";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = null;
		} else if(lowerfunction.equals(SUM)) {
			inputFunction = "SUM";
		} else if(lowerfunction.equals(STDEV)) {
			inputFunction = null;
		} else if(lowerfunction.equals(COUNT)) {
			inputFunction = "COUNT";
		} else if(lowerfunction.equals(UNIQUE_COUNT)) {
			inputFunction = "COUNT";
		} else if(lowerfunction.equals(CONCAT)) {
			inputFunction = "CONCAT";
		} else if(lowerfunction.equals(GROUP_CONCAT)) {
			inputFunction = "GROUP_CONCAT";
		} else if(lowerfunction.equals(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "GROUP_CONCAT";
		}
		
		return inputFunction;
	}
	
	/**
	 * Try to predict the function type
	 * Default to number
	 * @param inputFunction
	 * @return
	 */
	public static String determineTypeOfFunction(String inputFunction) {
		String lowerfunction = inputFunction.toLowerCase();
		if(lowerfunction.equals(CONCAT) 
				|| lowerfunction.equals(CONCAT) 
				|| lowerfunction.equals(CONCAT)) {
			return "STRING";
		}
		
		// default, it is probably a number
		return "NUMBER";
	}
	
	public static String getPrettyName(String inputFunction) {
		String lowerfunction = inputFunction.toLowerCase();
		if(lowerfunction.equals(MIN)) {
			inputFunction = "Min";
		} else if(lowerfunction.equals(MAX)) {
			inputFunction = "Max";
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE)) {
			inputFunction = "Average";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "Median";
		} else if(lowerfunction.equals(SUM)) {
			inputFunction = "Sum";
		} else if(lowerfunction.equals(STDEV)) {
			inputFunction = "Standard_Deviation";
		} else if(lowerfunction.equals(COUNT)) {
			inputFunction = "Count";
		} else if(lowerfunction.equals(UNIQUE_COUNT)) {
			inputFunction = "Unqiue_Count";
		} else if(lowerfunction.equals(CONCAT)) {
			inputFunction = "Concat";
		} else if(lowerfunction.equals(GROUP_CONCAT)) {
			inputFunction = "Group_Concat";
		} else if(lowerfunction.equals(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "Unique_Group_Concat";
		}
		
		return inputFunction;
	}
	
}
