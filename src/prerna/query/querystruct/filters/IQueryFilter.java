package prerna.query.querystruct.filters;

import java.util.List;
import java.util.Set;

import com.google.gson.TypeAdapter;

import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.gson.AndQueryFilterAdapter;
import prerna.util.gson.OrQueryFilterAdapter;
import prerna.util.gson.SelectQueryStructAdapter;
import prerna.util.gson.SimpleQueryFilterAdapter;

public interface IQueryFilter {

	enum QUERY_FILTER_TYPE {AND, OR, SIMPLE, SUBQUERY, FUNCTION, BETWEEN};
	
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
	 * Get all the Query Column Selectors
	 * @return
	 */
	List<QueryColumnSelector> getAllQueryColumns();
	
	/**
	 * Get all columns used by the filter
	 * @return
	 */
	Set<String> getAllQueryStructColumns();
	
	/**
	 * Get all tables used by the filter
	 * @return
	 */
	Set<String> getAllUsedTables();
	
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
	 * Get a simple name for each comparator
	 * @param comparator
	 * @return
	 */
	public static String getDisplayNameForComparator(String comparator) {
		// Added IN and '=' to accommodate query needs
		if(comparator.equals("==") || comparator.equalsIgnoreCase("IN")) {
			return "In";
		} else if(comparator.equals("!=") || comparator.equals("<>")) {
			return "Not In";
		} else if(comparator.equals(">")) {
			return "Greater Than ( > )";
		} else if(comparator.equals(">=")) {
			return "Greater Than Equal ( >= )";
		} else if(comparator.equals("<")) {
			return "Less Than ( < )";
		} else if(comparator.equals("<=")) {
			return "Less Than Equal ( <= )";
		} else if(comparator.equals("?like")) {
			return "Like";
		} else if(comparator.equals("?nlike")) {
			return "Not Like";
		} else if(comparator.equals("?begins")) {
			return "Begins With";
		} else if(comparator.equals("?nbegins")) {
			return "Not Begins With";
		} else if(comparator.equals("?ends")) {
			return "Ends With";
		} else if(comparator.equals("?nends")) {
			return "Not Ends With";
		} else if (comparator.equals("=")) {
			return "Equals ( = )";
		}
		return null;
	}
	
	/**
	 * Get a simple name for each comparator
	 * @param comparator
	 * @return
	 */
	public static String getSimpleNameForComparator(String comparator) {
		if(comparator.equals("==")) {
			return "eq";
		} else if(comparator.equals("!=") || comparator.equals("<>")) {
			return "neq";
		} else if(comparator.equals(">")) {
			return "gt";
		} else if(comparator.equals(">=")) {
			return "gte";
		} else if(comparator.equals("<")) {
			return "lt";
		} else if(comparator.equals("<=")) {
			return "lte";
		} else if(comparator.equals("?like")) {
			return "nlike";
		} else if(comparator.equals("?nlike")) {
			return "like";
		} else if(comparator.equals("?begins")) {
			return "nbegins";
		} else if(comparator.equals("?nbegins")) {
			return "begins";
		} else if(comparator.equals("?ends")) {
			return "nends";
		} else if(comparator.equals("?nends")) {
			return "ends";
		}
		return null;
	}
	
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
			return "?nlike";
		} else if(comparator.equals("?nlike")) {
			return "?like";
		} else if(comparator.equals("?begins")) {
			return "?nbegins";
		} else if(comparator.equals("?nbegins")) {
			return "?begins";
		} else if(comparator.equals("?ends")) {
			return "?nends";
		} else if(comparator.equals("?nends")) {
			return "?ends";
		}
		return null;
	}
	
	public static boolean isRegexComparator(String comparator) {
		if(comparator.equals("?like") || comparator.equals("?nlike")
				|| comparator.equals("?begins") || comparator.equals("?nbegins")
				|| comparator.equals("?ends") || comparator.equals("?nends")
				) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorsDirectlyConflicting(String comparator1, String comparator2) {
		if(comparator1.equals("==") && (comparator2.equals("!=") || comparator2.equals("<>")) ) {
			return true;
		} else if(comparator2.equals("==") && (comparator1.equals("!=") || comparator1.equals("<>")) ) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorsRegexConflicting(String comparator1, String comparator2) {
		if(comparator1.equals("==") && comparator2.equals("?nlike")) {
			return true;
		} else if(comparator2.equals("==") && comparator1.equals("?nlike")) {
			return true;
		} else if(comparator1.equals("?like") && (comparator2.equals("!=") || comparator2.equals("<>")) ) {
			return true;
		} else if(comparator2.equals("?like") && (comparator1.equals("!=") || comparator1.equals("<>")) ) {
			return true;
		} else if(comparator1.equals("?like") && comparator2.equals("?nlike")) {
			return true;
		} else if(comparator2.equals("?like") && comparator1.equals("?nlike")) {
			return true;
		}
		return false;
	}
	
	public static boolean newComparatorOvershadowsExisting(String existingComparator, String newComparator) {
		if(existingComparator.equals("==") && newComparator.equals("?like")) {
			return true;
		} else if( (existingComparator.equals("!=") || existingComparator.equals("<>")) && newComparator.equals("?nlike")) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorsRequireOrStatement(String existingComparator, String newComparator) {
		if(existingComparator.equals("==") && newComparator.equals("?like")) {
			return true;
		} else if(newComparator.equals("==") && existingComparator.equals("?like")) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorsCanCombine(String existingComparator, String newComparator) {
		if(existingComparator.equals("==") && newComparator.equals("==")) {
			return true;
		} else if( (newComparator.equals("!=") || newComparator.equals("<>")) && 
				(existingComparator.equals("!=") || existingComparator.equals("<>")) ) {
			return true;
		} else if(existingComparator.equals("?like") && newComparator.equals("?like")) {
			return true;
		} else if(existingComparator.equals("?nlike") && newComparator.equals("?nlike")) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorsAreConflicting(String comparator1, String comparator2) {
		if( (comparator1.equals("==") || comparator1.equals("?like")) && (comparator2.equals("!=") || comparator2.equals("<>") || comparator2.equals("?nlike"))) {
			return true;
		} else if( (comparator2.equals("==") || comparator2.equals("?like")) && (comparator1.equals("!=") || comparator1.equals("<>") || comparator1.equals("?nlike"))) {
			return true;
		}
		return false;
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
		if(comparator.equals(">") || comparator.equals(">=")
				|| comparator.equals("<") || comparator.equals("<=")
				) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorNotNumeric(String comparator) {
		if(comparator.equals("==")
				|| comparator.equals("!=")
				|| comparator.equals("<>")
				|| comparator.equals("?like")
				|| comparator.equals("?nlike")
				|| comparator.equals("?begins")
				|| comparator.equals("?nbegins")
				|| comparator.equals("?ends")
				|| comparator.equals("?nends")
				) {
			return true;
		}
		return false;
	}
	
	public static boolean comparatorIsValidSQL(String comparator) {
		if(comparator.equals("=")
				|| comparator.equals("in")
				|| comparator.equals("!=")
				|| comparator.equals("<>")
				|| comparator.equals(">")
				|| comparator.equals(">=")
				|| comparator.equals("<")
				|| comparator.equals("<=")
				|| comparator.equals("like")
				|| comparator.equals("and")
				|| comparator.equals("or")
				) {
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
	
	public static boolean comparatorIsEquals(String comparator) {
		// we assume empty as equals
		return comparator == null || comparator.equals("") 
				|| comparator.equals("==") || comparator.equals("=");
	}

	public static boolean comparatorIsNotEquals(String comparator) {
		// we assume empty as equals
		return comparator.equals("!=") || comparator.equals("<>");
	}
	
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

	/*
	 * 
	 * Methods around serialization
	 * 
	 */

	
	static TypeAdapter getAdapterForFilter(QUERY_FILTER_TYPE type) {
		if(type == QUERY_FILTER_TYPE.SIMPLE) {
			return new SimpleQueryFilterAdapter();
		} else if(type == QUERY_FILTER_TYPE.OR) {
			return new OrQueryFilterAdapter();
		} else if(type == QUERY_FILTER_TYPE.AND) {
			return new AndQueryFilterAdapter();
		} else if(type == QUERY_FILTER_TYPE.SUBQUERY) {
			return new SelectQueryStructAdapter();
		}
		
		return null;
	}
	
	/**
	 * Convert string to SELECTOR_TYPE
	 * @param s
	 * @return
	 */
	static QUERY_FILTER_TYPE convertStringToFilterType(String s) {
		if(s.equals(QUERY_FILTER_TYPE.SIMPLE.toString())) {
			return QUERY_FILTER_TYPE.SIMPLE;
		} else if(s.equals(QUERY_FILTER_TYPE.OR.toString())) {
			return QUERY_FILTER_TYPE.OR;
		} else if(s.equals(QUERY_FILTER_TYPE.AND.toString())) {
			return QUERY_FILTER_TYPE.AND;
		} else if(s.equals(QUERY_FILTER_TYPE.SUBQUERY.toString())) {
			return QUERY_FILTER_TYPE.SUBQUERY;
		}
		
		return null;
	}
}
