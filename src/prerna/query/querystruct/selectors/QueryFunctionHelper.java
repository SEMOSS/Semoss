package prerna.query.querystruct.selectors;

public class QueryFunctionHelper {

	public static final String MIN = "Min";
	public static final String MAX = "Max";
	public static final String MEAN = "Mean";
	public static final String UNIQUE_MEAN = "UniqueMean";
	public static final String AVERAGE_1 = "Average";
	public static final String UNIQUE_AVERAGE_1 = "UniqueAverage";
	public static final String AVERAGE_2 = "Avg";
	public static final String UNIQUE_AVERAGE_2 = "UniqueAvg";
	public static final String MEDIAN = "Median";
	public static final String SUM = "Sum";
	public static final String UNIQUE_SUM = "UniqueSum";
	public static final String STDEV_1 = "StandardDeviation";
	public static final String STDEV_2 = "stdev";
	public static final String COUNT = "Count";
	public static final String UNIQUE_COUNT = "UniqueCount";
	public static final String CONCAT = "Concat";
	public static final String GROUP_CONCAT = "GroupConcat";
	public static final String UNIQUE_GROUP_CONCAT = "UniqueGroupConcat";
	public static final String LOWER = "Lower";
	public static final String COALESCE = "Coalesce";
	public static final String REGEXP_LIKE = "RegexLike";
	public static final String SUBSTRING = "Substring";
	public static final String DATE_FORMAT = "DateFormat";
	
	// Date functions
	public static final String DATE_ADD = "DateAdd";
	public static final String MONTH_NAME = "MonthName";
	public static final String DAY_NAME = "DayName";
	public static final String QUARTER = "Quarter";
	public static final String WEEK = "Week";
	public static final String YEAR = "Year";
	
	private QueryFunctionHelper() {
		
	}
	
	/**
	 * DUE TO THE LARGE VARIANCE IN SQL 
	 * THIS IS PUSHED TO THE {{@link prerna.util.sql.SQLQueryUtil}}
	 * AND IS USED BY METHOD {{@link prerna.util.sql.SQLQueryUtil#getSqlFunctionSyntax()}}
	 * 
	 * INDIVIDUAL QUERY UTIL IMPLEMENTATIONS CAN OVERRIDE THE SUBMETHODS FOR THEIR VERSION
	 * OF THE QUERY FUNCTION
	 * 
	 */
	
//	/**
//	 * Convert the function name to ansi-sql syntax
//	 * @param inputFunction
//	 * @return
//	 */
//	public static String convertFunctionToSqlSyntax(String inputFunction) {
//		if(inputFunction.equalsIgnoreCase(MIN)) {
//			inputFunction = "MIN";
//		} else if(inputFunction.equalsIgnoreCase(MAX)) {
//			inputFunction = "MAX";
//		} else if(inputFunction.equalsIgnoreCase(MEAN) || inputFunction.equalsIgnoreCase(AVERAGE_1) || inputFunction.equalsIgnoreCase(AVERAGE_2) 
//				|| inputFunction.equalsIgnoreCase(UNIQUE_MEAN) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_1) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_2)) {
//			inputFunction = "AVG";
//		} else if(inputFunction.equalsIgnoreCase(MEDIAN)) {
//			inputFunction = "MEDIAN";
//		} else if(inputFunction.equalsIgnoreCase(SUM) || inputFunction.equalsIgnoreCase(UNIQUE_SUM)) {
//			inputFunction = "SUM";
//		} else if(inputFunction.equalsIgnoreCase(STDEV_1) || inputFunction.equalsIgnoreCase(STDEV_2)) {
//			inputFunction = "STDDEV_SAMP";
//		} else if(inputFunction.equalsIgnoreCase(COUNT)) {
//			inputFunction = "COUNT";
//		} else if(inputFunction.equalsIgnoreCase(UNIQUE_COUNT)) {
//			inputFunction = "COUNT";
//		} else if(inputFunction.equalsIgnoreCase(CONCAT)) {
//			inputFunction = "CONCAT";
//		} else if(inputFunction.equalsIgnoreCase(GROUP_CONCAT)) {
//			inputFunction = "GROUP_CONCAT";
//		} else if(inputFunction.equalsIgnoreCase(UNIQUE_GROUP_CONCAT)) {
//			inputFunction = "GROUP_CONCAT";
//		} else if(inputFunction.equalsIgnoreCase(LOWER)) {
//			inputFunction = "LOWER";
//		} else if(inputFunction.equalsIgnoreCase(COALESCE)) {
//			inputFunction = "COALESCE";
//		}
//		
//		return inputFunction;
//	}
	
	/**
	 * Convert the function name to r data.table syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToRSyntax(String inputFunction) {
		if(inputFunction.equalsIgnoreCase(MIN)) {
			inputFunction = "min";
		} else if(inputFunction.equalsIgnoreCase(MAX)) {
			inputFunction = "max";
		} else if(inputFunction.equalsIgnoreCase(MEAN) || inputFunction.equalsIgnoreCase(AVERAGE_1) || inputFunction.equalsIgnoreCase(AVERAGE_2)
				|| inputFunction.equalsIgnoreCase(UNIQUE_MEAN) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_1) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_2)) {
			inputFunction = "mean";
		} else if(inputFunction.equalsIgnoreCase(MEDIAN)) {
			inputFunction = "median";
		} else if(inputFunction.equalsIgnoreCase(SUM) || inputFunction.equalsIgnoreCase(UNIQUE_SUM)) {
			inputFunction = "sum";
		} else if(inputFunction.equalsIgnoreCase(STDEV_1) || inputFunction.equalsIgnoreCase(STDEV_2)) {
			inputFunction = "sd";
		} else if(inputFunction.equalsIgnoreCase(COUNT)) {
			inputFunction = "length";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_COUNT)) {
			inputFunction = "uniqueN";
		} else if(inputFunction.equalsIgnoreCase(CONCAT)) {
			inputFunction = "paste";
		} else if(inputFunction.equalsIgnoreCase(GROUP_CONCAT)) {
			inputFunction = "paste";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "paste";
		} else if(inputFunction.equalsIgnoreCase(LOWER)) {
			inputFunction = "tolower";
		}
		
		return inputFunction;
	}
	
	/**
	 * Convert the function name to r data.table syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToPandasSyntax(String inputFunction) {
		if(inputFunction.equalsIgnoreCase(MIN)) {
			inputFunction = "min";
		} else if(inputFunction.equalsIgnoreCase(MAX)) {
			inputFunction = "max";
		} else if(inputFunction.equalsIgnoreCase(MEAN) || inputFunction.equalsIgnoreCase(AVERAGE_1) || inputFunction.equalsIgnoreCase(AVERAGE_2)
				|| inputFunction.equalsIgnoreCase(UNIQUE_MEAN) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_1) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_2)) {
			inputFunction = "mean";
		} else if(inputFunction.equalsIgnoreCase(MEDIAN)) {
			inputFunction = "median";
		} else if(inputFunction.equalsIgnoreCase(SUM) || inputFunction.equalsIgnoreCase(UNIQUE_SUM)) {
			inputFunction = "sum";
		} else if(inputFunction.equalsIgnoreCase(STDEV_1) || inputFunction.equalsIgnoreCase(STDEV_2)) {
			inputFunction = "std";
		} else if(inputFunction.equalsIgnoreCase(COUNT)) {
			inputFunction = "count";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_COUNT)) {
			inputFunction = "nunique";
		} else if(inputFunction.equalsIgnoreCase(CONCAT)) {
			inputFunction = "sum";
		} else if(inputFunction.equalsIgnoreCase(GROUP_CONCAT)) {
			inputFunction = "count";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "count";
		} else if(inputFunction.equalsIgnoreCase(LOWER)) {
			inputFunction = "str.lower";
		} else if(inputFunction.equalsIgnoreCase(SUBSTRING)) {
			inputFunction = "str.slice";
		} else if(inputFunction.equalsIgnoreCase(DAY_NAME)) {
			inputFunction = "dt.weekday_name";
		} else if(inputFunction.equalsIgnoreCase(MONTH_NAME)) {
			inputFunction = "dt.month_name()";
		} else if(inputFunction.equalsIgnoreCase(YEAR)) {
			inputFunction = "dt.year";
		} else if(inputFunction.equalsIgnoreCase(QUARTER)) {
			inputFunction = "dt.quarter";
		} else if(inputFunction.equalsIgnoreCase(WEEK)) {
			inputFunction = "dt.week";
		}
		
		return inputFunction;
	}
	

	
	/**
	 * Convert the function name to sparql syntax
	 * @param inputFunction
	 * @return
	 */
	public static String convertFunctionToSparqlSyntax(String inputFunction) {
		if(inputFunction.equalsIgnoreCase(MIN)) {
			inputFunction = "MIN";
		} else if(inputFunction.equalsIgnoreCase(MAX)) {
			inputFunction = "MAX";
		} else if(inputFunction.equalsIgnoreCase(MEAN) || inputFunction.equalsIgnoreCase(AVERAGE_1) || inputFunction.equalsIgnoreCase(AVERAGE_2) 
				|| inputFunction.equalsIgnoreCase(UNIQUE_MEAN) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_1) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_2)) {
			inputFunction = "AVG";
		} else if(inputFunction.equalsIgnoreCase(MEDIAN)) {
			inputFunction = null;
		} else if(inputFunction.equalsIgnoreCase(SUM) || inputFunction.equalsIgnoreCase(UNIQUE_SUM)) {
			inputFunction = "SUM";
		} else if(inputFunction.equalsIgnoreCase(STDEV_1) || inputFunction.equalsIgnoreCase(STDEV_2)) {
			inputFunction = null;
		} else if(inputFunction.equalsIgnoreCase(COUNT)) {
			inputFunction = "COUNT";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_COUNT)) {
			inputFunction = "COUNT";
		} else if(inputFunction.equalsIgnoreCase(CONCAT)) {
			inputFunction = "CONCAT";
		} else if(inputFunction.equalsIgnoreCase(GROUP_CONCAT)) {
			inputFunction = "GROUP_CONCAT";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "GROUP_CONCAT";
		} else if(inputFunction.equalsIgnoreCase(LOWER)) {
			inputFunction = "LCASE";
		} else if(inputFunction.equalsIgnoreCase(COALESCE)) {
			inputFunction = "COALESCE";
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
		if(inputFunction.equalsIgnoreCase(CONCAT) 
				|| inputFunction.equalsIgnoreCase(GROUP_CONCAT) 
				|| inputFunction.equalsIgnoreCase(UNIQUE_GROUP_CONCAT)
				|| inputFunction.equalsIgnoreCase(LOWER)
				|| inputFunction.equalsIgnoreCase(COALESCE)
				|| inputFunction.equalsIgnoreCase(SUBSTRING)
				|| inputFunction.equalsIgnoreCase(MONTH_NAME)
				|| inputFunction.equalsIgnoreCase(DAY_NAME)) {
			return "STRING";
		} else if(inputFunction.equalsIgnoreCase(COUNT) 
				|| inputFunction.equalsIgnoreCase(UNIQUE_COUNT) ) {
			return "INT";
		}
		
		// default, it is probably a number
		return "NUMBER";
	}
	
	public static String getPrettyName(String inputFunction) {
		if(inputFunction.equalsIgnoreCase(MIN)) {
			inputFunction = "Min";
		} else if(inputFunction.equalsIgnoreCase(MAX)) {
			inputFunction = "Max";
		} else if(inputFunction.equalsIgnoreCase(MEAN) || inputFunction.equalsIgnoreCase(AVERAGE_1) || inputFunction.equalsIgnoreCase(AVERAGE_2)) {
			inputFunction = "Average";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_MEAN) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_1) || inputFunction.equalsIgnoreCase(UNIQUE_AVERAGE_2)) {
			inputFunction = "UniqueAverage";
		} else if(inputFunction.equalsIgnoreCase(MEDIAN)) {
			inputFunction = "Median";
		} else if(inputFunction.equalsIgnoreCase(SUM)) {
			inputFunction = "Sum";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_SUM)) {
			inputFunction = "UniqueSum";
		} else if(inputFunction.equalsIgnoreCase(STDEV_1) || inputFunction.equalsIgnoreCase(STDEV_2)) {
			inputFunction = "StandardDeviation";
		} else if(inputFunction.equalsIgnoreCase(COUNT)) {
			inputFunction = "Count";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_COUNT)) {
			inputFunction = "UniqueCount";
		} else if(inputFunction.equalsIgnoreCase(CONCAT)) {
			inputFunction = "Concat";
		} else if(inputFunction.equalsIgnoreCase(GROUP_CONCAT)) {
			inputFunction = "GroupConcat";
		} else if(inputFunction.equalsIgnoreCase(UNIQUE_GROUP_CONCAT)) {
			inputFunction = "UniqueGroupConcat";
		} else if(inputFunction.equalsIgnoreCase(LOWER)) {
			inputFunction = "Lower";
		} else if(inputFunction.equalsIgnoreCase(COALESCE)) {
			inputFunction = "Coalesce";
		} else if(inputFunction.equalsIgnoreCase(REGEXP_LIKE)) {
			inputFunction = "RegexLike";
		} else if(inputFunction.equalsIgnoreCase(SUBSTRING)) {
			inputFunction = "Substring";
		} else if(inputFunction.equalsIgnoreCase(DATE_FORMAT)) {
			inputFunction = "DateFormat";
		}
		
		return inputFunction;
	}
	
}
