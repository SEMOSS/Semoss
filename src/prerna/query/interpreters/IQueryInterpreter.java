package prerna.query.interpreters;

import java.util.Map;

import org.apache.log4j.Logger;

import prerna.query.querystruct.SelectQueryStruct;

public interface IQueryInterpreter {
	
	String SEARCH_COMPARATOR = "?like";

	void setQueryStruct(SelectQueryStruct qs);

	String composeQuery();

	void setDistinct(boolean isDistinct);
	
	boolean isDistinct();
	
	void setLogger(Logger logger);
	
	void setAdditionalTypes(Map<String, String> additionalTypes);
}
