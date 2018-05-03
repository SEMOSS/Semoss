package prerna.query.interpreters.verifiers;

import prerna.query.querystruct.SelectQueryStruct;

public interface IQueryStructValidator {

	boolean isValid();
	
	SelectQueryStruct getQueryableQueryStruct();
	
	SelectQueryStruct getNonQueryableQueryStruct();
}
