package prerna.rdf.query.builder;

import prerna.ds.QueryStruct;

public interface IQueryInterpreter {

	void setQueryStruct(QueryStruct qs);

	String composeQuery();

}
