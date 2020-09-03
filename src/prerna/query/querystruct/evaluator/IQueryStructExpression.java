package prerna.query.querystruct.evaluator;

import prerna.query.querystruct.selectors.QueryFunctionHelper;

public interface IQueryStructExpression {

	void processData(Object obj);
	
	Object getOutput();

	static IQueryStructExpression getExpression(String functionName) {
		functionName = functionName.toLowerCase();
		if(functionName.equalsIgnoreCase(QueryFunctionHelper.COUNT)) {
			return new QueryCountExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.GROUP_CONCAT)) {
			return new QueryGroupConcatExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.MAX)) {
			return new QueryMaxExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.MEAN) 
				|| functionName.equalsIgnoreCase(QueryFunctionHelper.AVERAGE_1) 
				|| functionName.equalsIgnoreCase(QueryFunctionHelper.AVERAGE_2)) {
			return new QueryAverageExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.MEDIAN)) {
			return new QueryMedianExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.MIN)) {
			return new QueryMinExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.STDEV_1)) {
			return new QueryStandardDeviationExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.SUM)) {
			return new QuerySumExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_COUNT)) {
			return new QueryUniqueCountExpression();
		} else if(functionName.equalsIgnoreCase(QueryFunctionHelper.UNIQUE_GROUP_CONCAT)) {
			return new QueryUniqueGroupConcatExpression();
		}
		
		return null;
	}
	
}
