package prerna.poi.main.insights;

public interface InsightRuleConstants {

	// specific parameter keys
	String QUESTION_KEY = "@QUESTION";
	String OUTPUT_KEY = "@OUTPUT";
	String PROPERTY_KEY = "@PROPERTY";
	String CONCEPT_KEY = "@CONCEPT";
	String PROPERTY_VALUE = "PROPERTY";
	String CONCEPT_VALUE = "CONCEPT";
	
	// constraints
	String ENTROPY_DENSITY_MAX = "ENTROPY_D_MAX";
	String ENTROPY_DENSITY_MIN = "ENTROPY_D_MIN";
	String DATA_TYPE = "TYPE";
	String CLASS = "CLASS";
	
	// aggregation types
	String AGGREGATION = "AGGREGATION";
	String COUNT = "COUNT";
	String AVERAGE = "AVERAGE";
	String SUM = "SUM";
	String MAX = "MAX";
	String MIN = "MIN";

}
