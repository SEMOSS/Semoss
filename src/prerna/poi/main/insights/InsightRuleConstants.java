package prerna.poi.main.insights;

public interface InsightRuleConstants {

	// specific parameter keys
	String QUESTION_KEY = "@QUESTION";
	String OUTPUT_KEY = "@OUTPUT";
	
	// constraints
	String ENTROPY_DENSITY_MAX = "EntropyD_Max";
	String ENTROPY_DENSITY_MIN = "EntropyD_Min";
	String DATA_TYPE = "TYPE";
	String CLASS = "CLASS";
	
	// output types
	String PIE_CHART = "PIE_CHART";
	String BAR_CHART = "BAR_CHART";
	String SCATTER_PLOT = "SCATTER_PLOT";
	String HEAT_MAP = "HEAT_MAP";
	String PARALLEL_COORDINATES = "PARALLEL_COORDINATES";
	String BUBBLE_CHART = "BUBBLE_CHART";
	
	// aggregation types
	String AGGREGATION = "AGGREGATION";
	String COUNT = "COUNT";
	String AVERAGE = "AVERAGE";
	String SUM = "SUM";
	String MAX = "MAX";
	String MIN = "MIN";

}
