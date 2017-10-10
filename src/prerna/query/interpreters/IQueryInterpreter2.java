package prerna.query.interpreters;

import org.apache.log4j.Logger;

import prerna.query.querystruct.QueryStruct2;

public interface IQueryInterpreter2 {
	
	String SEARCH_COMPARATOR = "?like";

	void setQueryStruct(QueryStruct2 qs);

	String composeQuery();

	void setDistinct(boolean isDistinct);
	
	boolean isDistinct();
	
	void setLogger(Logger logger);
}
