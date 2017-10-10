package prerna.query.interpreters.verifiers;

import prerna.query.querystruct.QueryStruct2;

public interface IQueryStructValidator {

	boolean isValid();
	
	QueryStruct2 getQueryableQueryStruct();
	
	QueryStruct2 getNonQueryableQueryStruct();
}
