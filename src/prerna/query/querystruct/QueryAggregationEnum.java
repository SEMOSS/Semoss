package prerna.query.querystruct;

public enum QueryAggregationEnum {

	/*
	 * To handle the differences in syntax between various aggregation rouintes
	 */
	
	// 1) expression name
	// 2) base sql
	// 3) r
	// 4) sparql
	
	MIN("Min", 
			"MIN", 
			"min",
			"min",
			"NUMBER"
			),
	
	MAX("Max",
			"MAX", 
			"max",
			"max",
			"NUMBER"
			),
	
	MEAN("Average",
			"AVG",
			"mean",
			"avg",
			"NUMBER"
			),
		
	MEDIAN("Median",
			"MEDIAN", 
			"median",
			"INVALID_AGGREGATION_ROUTINE",
			"NUMBER"
			),
	
	SUM("Sum",
			"SUM", 
			"sum",
			"sum",
			"NUMBER"
			),
	
	STANDARD_DEVIATION("StandardDeviation",
			"STDDEV_SAMP", 
			"sd",
			"INVALID_AGGREGATION_ROUTINE",
			"NUMBER"
			),
	
	COUNT("Count",
			"COUNT", 
			"length",
			"count",
			"NUMBER"
			),
	
	UNIQUE_COUNT("UniqueCount",
			"COUNT", 
			"uniqueN",
			"count",
			"NUMBER"
			),
	
	CONCAT("Concat",
			"CONCAT",
			"paste",
			"concat",
			"STRING"
			),
	
	GROUP_CONCAT("GroupConcat",
			"GROUP_CONCAT", 
			"paste",
			"group_concat",
			"STRING"
			),
	
	UNIQUE_GROUP_CONCAT("UniqueGroupConcat",
			"GROUP_CONCAT",
			"paste",
			"group_concat",
			"STRING"
			);
	
	public static final String INVALID_HEADER = "INVALID_AGGREGATION_ROUTINE";

	private final String expressionName;
	private final String baseSqlSyntaxs;
	private final String rSyntax;
	private final String sparqlSyntax;
	private final String dataType;
	
	private QueryAggregationEnum(String expressionName, 
			String baseSqlSyntaxs, 
			String rSyntax,
			String sparqlSyntax,
			String dataType) {
		this.expressionName = expressionName;
		this.baseSqlSyntaxs = baseSqlSyntaxs;
		this.rSyntax = rSyntax;
		this.sparqlSyntax = sparqlSyntax;
		this.dataType = dataType;
	}
	
	public String getExpressionName() {
		return this.expressionName;
	}
	
	public String getBaseSqlSyntax() {
		return this.baseSqlSyntaxs;
	}
	
	public String getRSyntax() {
		return this.rSyntax;
	}
	
	public String getSparqlSyntax() {
		return this.sparqlSyntax;
	}
	
	public String getDataType() {
		return this.dataType;
	}
	
	public static final boolean isValid(String mathExpression) {
		if(INVALID_HEADER.equals(mathExpression)) {
			return false;
		} 
		return true;
	}
}
