package prerna.query.querystruct.selectors;

public class QueryFunctionHelper {

	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String MEAN = "mean";
	public static final String UNIQUE_MEAN = "uniquemean";
	public static final String AVERAGE_1 = "average";
	public static final String UNIQUE_AVERAGE_1 = "uniqueaverage";
	public static final String AVERAGE_2 = "avg";
	public static final String UNIQUE_AVERAGE_2 = "uniqueavg";
	public static final String MEDIAN = "median";
	public static final String SUM = "sum";
	public static final String UNIQUE_SUM = "uniquesum";
	public static final String STDEV_1 = "standarddeviation";
	public static final String STDEV_2 = "stdev";
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
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE_1) || lowerfunction.equals(AVERAGE_2) 
				|| lowerfunction.equals(UNIQUE_MEAN) || lowerfunction.equals(UNIQUE_AVERAGE_1) || lowerfunction.equals(UNIQUE_AVERAGE_2)) {
			inputFunction = "AVG";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "MEDIAN";
		} else if(lowerfunction.equals(SUM) || lowerfunction.equals(UNIQUE_SUM)) {
			inputFunction = "SUM";
		} else if(lowerfunction.equals(STDEV_1) || lowerfunction.equals(STDEV_2)) {
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
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE_1) || lowerfunction.equals(AVERAGE_2)
				|| lowerfunction.equals(UNIQUE_MEAN) || lowerfunction.equals(UNIQUE_AVERAGE_1) || lowerfunction.equals(UNIQUE_AVERAGE_2)) {
			inputFunction = "mean";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "median";
		} else if(lowerfunction.equals(SUM) || lowerfunction.equals(UNIQUE_SUM)) {
			inputFunction = "sum";
		} else if(lowerfunction.equals(STDEV_1) || lowerfunction.equals(STDEV_2)) {
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
	 * Convert the function name to r data.table syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToPandasSyntax(String inputFunction) {
		String lowerfunction = inputFunction.toLowerCase();
		if(lowerfunction.equals(MIN)) {
			inputFunction = "min";
		} else if(lowerfunction.equals(MAX)) {
			inputFunction = "max";
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE_1) || lowerfunction.equals(AVERAGE_2)
				|| lowerfunction.equals(UNIQUE_MEAN) || lowerfunction.equals(UNIQUE_AVERAGE_1) || lowerfunction.equals(UNIQUE_AVERAGE_2)) {
			inputFunction = "mean";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "median";
		} else if(lowerfunction.equals(SUM) || lowerfunction.equals(UNIQUE_SUM)) {
			inputFunction = "sum";
		} else if(lowerfunction.equals(STDEV_1) || lowerfunction.equals(STDEV_2)) {
			inputFunction = "std";
		} else if(lowerfunction.equals(COUNT)) {
			inputFunction = "count";
		} else if(lowerfunction.equals(UNIQUE_COUNT)) {
			inputFunction = "nunique";
		} else if(lowerfunction.equals(CONCAT)) {
			inputFunction = "sum";
		} else if(lowerfunction.equals(GROUP_CONCAT)) {
			inputFunction = "count";
		} else if(lowerfunction.equals(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "count";
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
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE_1) || lowerfunction.equals(AVERAGE_2) 
				|| lowerfunction.equals(UNIQUE_MEAN) || lowerfunction.equals(UNIQUE_AVERAGE_1) || lowerfunction.equals(UNIQUE_AVERAGE_2)) {
			inputFunction = "AVG";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = null;
		} else if(lowerfunction.equals(SUM) || lowerfunction.equals(UNIQUE_SUM)) {
			inputFunction = "SUM";
		} else if(lowerfunction.equals(STDEV_1) || lowerfunction.equals(STDEV_2)) {
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
		} else if(lowerfunction.equals(MEAN) || lowerfunction.equals(AVERAGE_1) || lowerfunction.equals(AVERAGE_2)) {
			inputFunction = "Average";
		} else if(lowerfunction.equals(UNIQUE_MEAN) || lowerfunction.equals(UNIQUE_AVERAGE_1) || lowerfunction.equals(UNIQUE_AVERAGE_2)) {
			inputFunction = "UniqueAverage";
		} else if(lowerfunction.equals(MEDIAN)) {
			inputFunction = "Median";
		} else if(lowerfunction.equals(SUM)) {
			inputFunction = "Sum";
		} else if(lowerfunction.equals(UNIQUE_SUM)) {
			inputFunction = "UniqueSum";
		} else if(lowerfunction.equals(STDEV_1) || lowerfunction.equals(STDEV_2)) {
			inputFunction = "StandardDeviation";
		} else if(lowerfunction.equals(COUNT)) {
			inputFunction = "Count";
		} else if(lowerfunction.equals(UNIQUE_COUNT)) {
			inputFunction = "UniqueCount";
		} else if(lowerfunction.equals(CONCAT)) {
			inputFunction = "Concat";
		} else if(lowerfunction.equals(GROUP_CONCAT)) {
			inputFunction = "GroupConcat";
		} else if(lowerfunction.equals(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "UniqueGroupConcat";
		}
		
		return inputFunction;
	}
	
}
