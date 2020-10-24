package prerna.query.interpreters;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.AbstractQueryStruct;

public interface IQueryInterpreter {
	
	/*
	 * Always define these as lower case for consistency
	 */
	
	String SEARCH_COMPARATOR = "?like";
	String NOT_SEARCH_COMPARATOR = "?nlike";
	String BEGINS_COMPARATOR = "?begins";
	String NOT_BEGINS_COMPARATOR = "?nbegins";
	String ENDS_COMPARATOR = "?ends";
	String NOT_ENDS_COMPARATOR = "?nends";

	void setQueryStruct(AbstractQueryStruct qs);

	String composeQuery();

	void setDistinct(boolean isDistinct);
	
	boolean isDistinct();
	
	void setLogger(Logger logger);
	
	static List<String> getAllSearchComparators() {
		return QuerySideEffect.searchComparators;
	}
	
	static List<String> getPosSearchComparators() {
		return QuerySideEffect.posSearchComparators;
	}
	static List<String> getNegSearchComparators() {
		return QuerySideEffect.negSearchComparators;
	}
}

class QuerySideEffect {
	
	static List<String> searchComparators = new Vector<>();
	static {
		searchComparators.add(IQueryInterpreter.SEARCH_COMPARATOR);
		searchComparators.add(IQueryInterpreter.NOT_SEARCH_COMPARATOR);
		searchComparators.add(IQueryInterpreter.BEGINS_COMPARATOR);
		searchComparators.add(IQueryInterpreter.NOT_BEGINS_COMPARATOR);
		searchComparators.add(IQueryInterpreter.ENDS_COMPARATOR);
		searchComparators.add(IQueryInterpreter.NOT_ENDS_COMPARATOR);
	}
	
	static List<String> posSearchComparators = new Vector<>();
	static {
		posSearchComparators.add(IQueryInterpreter.SEARCH_COMPARATOR);
		posSearchComparators.add(IQueryInterpreter.BEGINS_COMPARATOR);
		posSearchComparators.add(IQueryInterpreter.ENDS_COMPARATOR);
	}
	
	static List<String> negSearchComparators = new Vector<>();
	static {
		negSearchComparators.add(IQueryInterpreter.NOT_SEARCH_COMPARATOR);
		negSearchComparators.add(IQueryInterpreter.NOT_BEGINS_COMPARATOR);
		negSearchComparators.add(IQueryInterpreter.NOT_ENDS_COMPARATOR);
	}
}