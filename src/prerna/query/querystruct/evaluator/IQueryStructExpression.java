package prerna.query.querystruct.evaluator;

import prerna.query.querystruct.selectors.QueryAggregationEnum;

public interface IQueryStructExpression {

	void processData(Object obj);
	
	Object getOutput();

	static IQueryStructExpression getExpression(QueryAggregationEnum aggType) {
		if(aggType == QueryAggregationEnum.COUNT) {
			return new QueryCountExpression();
		} else if(aggType == QueryAggregationEnum.GROUP_CONCAT) {
			return new QueryGroupConcatExpression();
		} else if(aggType == QueryAggregationEnum.MAX) {
			return new QueryMaxExpression();
		} else if(aggType == QueryAggregationEnum.MEAN) {
			return new QueryAverageExpression();
		} else if(aggType == QueryAggregationEnum.MEDIAN) {
			return new QueryMedianExpression();
		} else if(aggType == QueryAggregationEnum.MIN) {
			return new QueryMinExpression();
		} else if(aggType == QueryAggregationEnum.STANDARD_DEVIATION) {
			return new QueryStandardDeviationExpression();
		}else if(aggType == QueryAggregationEnum.SUM) {
			return new QuerySumExpression();
		}else if(aggType == QueryAggregationEnum.UNIQUE_COUNT) {
			return new QueryUniqueCountExpression();
		}else if(aggType == QueryAggregationEnum.UNIQUE_GROUP_CONCAT) {
			return new QueryUniqueGroupConcatExpression();
		}
		
		return null;
	}
	
}
