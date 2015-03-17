package prerna.poi.main.insights;

public final class InsightRuleConstants {

	private InsightRuleConstants() {
		
	}
	
	// specific parameter keys
	public static final String QUESTION_KEY = "@QUESTION";
	public static final String OUTPUT_KEY = "@OUTPUT";
	public static final String PERSPECTIVE_KEY = "@PERSPECTIVE";
	public static final String PROPERTY_VALUE = "PROPERTY";
	public static final String CONCEPT_VALUE = "CONCEPT";
	public static final String CENTRAL_CONCEPT_VALUE = "CENTRALCONCEPT";
	
	// constraints
	public static final String ENTROPY_DENSITY_MAX = "ENTROPY_D_MAX";
	public static final String ENTROPY_DENSITY_MIN = "ENTROPY_D_MIN";
	public static final String DATA_TYPE = "TYPE";
	public static final String CLASS = "CLASS";
	
	// aggregation types
	public static final String AGGREGATION = "AGGREGATION";
	public static final String COUNT = "COUNT";
	public static final String AVERAGE = "AVG";
	public static final String SUM = "SUM";
	public static final String MAX = "MAX";
	public static final String MIN = "MIN";

}
