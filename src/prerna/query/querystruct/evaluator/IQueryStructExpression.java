package prerna.query.querystruct.evaluator;

import prerna.query.querystruct.selectors.QueryFunctionHelper;

public interface IQueryStructExpression {

	void processData(Object obj);
	
	Object getOutput();

	static IQueryStructExpression getExpression(String functionName) {
		functionName = functionName.toLowerCase();
		if(functionName.equals(QueryFunctionHelper.COUNT)) {
			return new QueryCountExpression();
		} else if(functionName.equals(QueryFunctionHelper.GROUP_CONCAT)) {
			return new QueryGroupConcatExpression();
		} else if(functionName.equals(QueryFunctionHelper.MAX)) {
			return new QueryMaxExpression();
		} else if(functionName.equals(QueryFunctionHelper.MEAN)) {
			return new QueryAverageExpression();
		} else if(functionName.equals(QueryFunctionHelper.MEDIAN)) {
			return new QueryMedianExpression();
		} else if(functionName.equals(QueryFunctionHelper.MIN)) {
			return new QueryMinExpression();
		} else if(functionName.equals(QueryFunctionHelper.STDEV)) {
			return new QueryStandardDeviationExpression();
		} else if(functionName.equals(QueryFunctionHelper.SUM)) {
			return new QuerySumExpression();
		} else if(functionName.equals(QueryFunctionHelper.UNIQUE_COUNT)) {
			return new QueryUniqueCountExpression();
		} else if(functionName.equals(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			return new QueryUniqueGroupConcatExpression();
		}
		
		return null;
	}
	
}
