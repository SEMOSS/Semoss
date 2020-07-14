package prerna.query.interpreters;

import org.apache.log4j.Logger;

import prerna.query.querystruct.AbstractQueryStruct;

public interface IQueryInterpreter {
	
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
	
//	void setAdditionalTypes(Map<String, String> additionalTypes);
}
