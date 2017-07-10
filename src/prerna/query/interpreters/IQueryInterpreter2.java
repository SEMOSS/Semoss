package prerna.query.interpreters;

public interface IQueryInterpreter2 {
	
	String SEARCH_COMPARATOR = "?like";

	void setQueryStruct(QueryStruct2 qs);

	String composeQuery();

	void setPerformCount(int performCount);
	
	int isPerformCount();
	
	void clear();
}
