package prerna.query.querystruct.filters;

import java.util.Set;

import com.google.gson.TypeAdapter;

import prerna.util.gson.AndQueryFilterAdapter;
import prerna.util.gson.OrQueryFilterAdapter;
import prerna.util.gson.SelectQueryStructAdapter;
import prerna.util.gson.SimpleQueryFilterAdapter;

public interface IQueryFilter {

	enum QUERY_FILTER_TYPE {AND, OR, SIMPLE, SUBQUERY, FUNCTION};
	
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
