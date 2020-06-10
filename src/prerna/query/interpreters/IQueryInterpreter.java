package prerna.query.interpreters;

import java.util.Map;

import org.apache.log4j.Logger;

import prerna.query.querystruct.AbstractQueryStruct;

public interface IQueryInterpreter {
	
	String SEARCH_COMPARATOR = "?like";
	String NOT_SEARCH_COMPARATOR = "?nlike";

	void setQueryStruct(AbstractQueryStruct qs);

	String composeQuery();

	void setDistinct(boolean isDistinct);
	
	boolean isDistinct();
	
	void setLogger(Logger logger);
	
//	void setAdditionalTypes(Map<String, String> additionalTypes);
}
