package prerna.poi.main.insights;

public interface InsightRuleConstants {

	// specific parameter keys
	String QUESTION_KEY = "@QUESTION";
	String OUTPUT_KEY = "@OUTPUT";
	String PERSPECTIVE_KEY = "@PERSPECTIVE";
	String PROPERTY_VALUE = "PROPERTY";
	String CONCEPT_VALUE = "CONCEPT";
	String CENTRAL_CONCEPT_VALUE = "CENTRALCONCEPT";
	
	// constraints
	String ENTROPY_DENSITY_MAX = "ENTROPY_D_MAX";
	String ENTROPY_DENSITY_MIN = "ENTROPY_D_MIN";
	String DATA_TYPE = "TYPE";
	String CLASS = "CLASS";
	
	// aggregation types
	String AGGREGATION = "AGGREGATION";
	String COUNT = "COUNT";
	String AVERAGE = "AVG";
	String SUM = "SUM";
	String MAX = "MAX";
	String MIN = "MIN";

}
