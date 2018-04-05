package prerna.query.querystruct.filters;

import java.util.Set;

public interface IQueryFilter {

	enum QUERY_FILTER_TYPE {AND, OR, SIMPLE, SUBQUERY};
	
	/**
	 * Get the type of the filter
	 * @return
	 */
	QUERY_FILTER_TYPE getQueryFilterType();
	
	/**
	 * Get all columns used by the filter
	 * @return
	 */
	Set<String> getAllUsedColumns();
	
	/**
	 * Get all columns used by the filter
	 * @return
	 */
	Set<String> getAllQueryStructColumns();
	
	/**
	 * See if the filter is using a specific column
	 * @param column
	 * @return
	 */
	boolean containsColumn(String column);
	
	/**
	 * Make a copy of the filter
	 * @return
	 */
	IQueryFilter copy();

	String getStringRepresentation();

	Object getSimpleFormat();
	
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	/////////////////////// STATIC METHODS /////////////////////////
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////

	/**
	 * Method to provide the reverse of a given comparator
	 * @param comparator
	 * @return
	 */
	public static String getReverseComparator(String comparator) {
		if(comparator.equals("==")) {
			return "!=";
		} else if(comparator.equals("!=") || comparator.equals("<>")) {
			return "==";
		} else if(comparator.equals(">")) {
			return "<";
		} else if(comparator.equals(">=")) {
			return "<=";
		} else if(comparator.equals("<")) {
			return ">";
		} else if(comparator.equals("<=")) {
			return ">=";
		} else if(comparator.equals("?like")) {
			// ughhhh... return the same thing
			return "?like";
		}
		return null;
	}
	
	/**
	 * Method to provide the reverse of a given numeric comparator
	 * Otherwise, return the same value
	 * @param comparator
	 * @return
	 */
	public static String getReverseNumericalComparator(String comparator) {
		if(comparator.equals(">")) {
			return "<";
		} else if(comparator.equals(">=")) {
			return "<=";
		} else if(comparator.equals("<")) {
			return ">";
		} else if(comparator.equals("<=")) {
			return ">=";
		}
		return comparator;
	}
	
	public static boolean comparatorIsNumeric(String comparator) {
		if(comparator.equals(">")) {
			return true;
		} else if(comparator.equals(">=")) {
			return true;
		} else if(comparator.equals("<")) {
			return true;
		} else if(comparator.equals("<=")) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorNotNumeric(String comparator) {
		if(comparator.equals("==")) {
			return true;
		} else if(comparator.equals("!=")) {
			return true;
		} else if(comparator.equals("<>")) {
			return true;
		}
		
		return false;
	}
	
	public static boolean comparatorIsSameSide(String comparator1, String comparator2) {
		if(comparator1.equals(">")) {
			if(comparator2.equals(">") || comparator2.equals(">=")) {
				return true;
			}
		} else if(comparator1.equals(">=")) {
			if(comparator2.equals(">") || comparator2.equals(">=")) {
				return true;
			}
		} else if(comparator1.equals("<")) {
			if(comparator2.equals("<") || comparator2.equals("<=")) {
				return true;
			}
		} else if(comparator1.equals("<=")) {
			if(comparator2.equals("<") || comparator2.equals("<=")) {
				return true;
			}
		} else if(comparator1.equals("==") && comparator2.equals("==")) {
			return true;
		} else if(comparator1.equals("!=") && comparator2.equals("!=")) {
			return true;
		}
		return false;
	}

}
