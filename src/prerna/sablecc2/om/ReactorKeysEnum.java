package prerna.sablecc2.om;

public enum ReactorKeysEnum {

	ADDITIONAL_PIXELS("additionalPixels",					"Additional pixels to be executed in addition to the pixel steps saved within the insight"),
	ALL_NUMERIC_KEY("allNumeric", 							"Indicates if only numeric headers should be returned"),
	ALIAS("alias", 											"An alias to assign for an operation or output"),
	ARRAY("array", 											"An array of input values"),
	ATTRIBUTES("attributes", 								"List of columns used as properties/characteristics to describe an instance/object"),
	BREAKS("breaks", 										"Number of divisions"),
	CLONE_PANEL_KEY("cloneId", 								"Id to assign the new clone panel"),
	CLUSTER_KEY("numClusters", 								"Number of clusters"),
	COLUMN("column", 										"Name of the column header"),
	COLUMNS("columns", 										"List of column headers"),
	COMMENT_KEY("comment", 									"This key can represent a map containing the data for a given comment or it can represent the id for an existing comment"),
	CONCEPT("concept", 										"Concept name within an engine"),
	CONCEPTS("concepts", 									"List of concept names within an engine"),
	CONNECTION_STRING_KEY("connectionString", 				"JDBC connection string to connect to an external database"),
	CRITERIA("criteria", 									"The criteria to be evaluated"),
	DB_DRIVER_KEY("dbDriver",								"Name of the JDBC driver.  Not all JDBC drivers are open source so make sure you include the driver within the classpath of SEMOSS"),
	DATA_TYPE("dataType", 									"Data type of the column (STRING, NUMBER, DATE)"),
	DATA_TYPES("dataTypeMap", 								"Map of column name to the column data types"),
	DEFAULT_VALUE_KEY("defaultValue", 						"A default value to use for null columns"),
	DELIMITER("delimiter", 									"Delimiter to be used"),
	ENGINE("engine", 										"Name of the datasource"),
	EVENTS_KEY("events", 									"The events map input"),
	FILENAME("fileName", 									"The name of the file"),
	FILE_PATH("path", 										"The file path"),
	FILTERS("filters", 										"Filters automatically persisted on queries affecting this frame or panel"),
	FILTER_WORD("filterWord", 								"Regex to apply for searches"),
	FRAME("frame", 											"The frame"),
	FRAME_TYPE("frameType", 								"Type of frame to generate - grid (sql based frame), graph (frame based on tinkerpop), r (data sits within r, must have r installed to use), native (leverages the database to execute queries)"),
	HEADER_NAMES("newHeaders", 								"The new header names"),
	IMAGE_URL("imageUrl", 									"The image URL"),
	INCLUDE_META_KEY("meta", 								"Boolean indication of whether to retrieve metadata"),
	INDEX("index", 											"A specified index"),
	INSIGHT_ID("id", 										"Unique id of the insight"),
	INSIGHT_IDS("insights", 								"The ids of the insights"),
	INSIGHT_NAME("insightName", 							"Name of the insight"),
	INSIGHT_ORNAMENT("insightOrnament", 					"The insight ornament map"),
	INSTANCE_KEY("instance", 								"Column representing the objects being used to perform the operation"),
	JOB_ID("jobId", 										"The id of the job"),
	JOINS("joins", 											"The joins on the frame"),
	KEY_NOUN("key", 										"The unique key"),
	LAYOUT_KEY("layout", 									"The layout"),
	LIMIT("limit", 											"Limit to add for the query results"),
	LOGICAL_NAME("logicalNames", 							"The column alias to be added to the master database"),
	NEW_COLUMN("newCol", 									"The name of the new column being created"),
	NEW_VALUE("newValue", 									"The new value used to replace an existing value"),
	NUMERIC_VALUES("numValues", 							"The numeric values to be used in the operation"),
	OFFSET("offset", 										"Offset to add for the query results"),
	OLD_ID_KEY("oldIds", 									"The old ids"),
	OPTIONS("options", 										"The options map"),
	ORNAMENTS_KEY("ornaments", 								"The panel ornaments"),
	PANEL("panel", 											"The id of the panel"), 								
	PANEL_LABEL_KEY("panelLabel", 							"The label for the panel"),
	PANEL_POSITION_KEY("position", 							"The panel position map"),
	PANEL_VIEW_KEY("panelView", 							"The panel view"),
	PANEL_VIEW_OPTIONS_KEY("panelViewOptions", 				"The panel view options"),
	PARAM_KEY("params", 									"The parameters for the insight map"),
	PASSWORD("password", 									"Password used in conjunction with the username for access to a service"),
	PLANNER("planner", 										"The planner"),
	REACTOR("reactor", 										"The reactor"),
	RECIPE("recipe", 										"The recipe"),
	REGEX("regex", 											"The regular expression"),
	SESSION_ID("sessionId", 								"The id of the session"),
	SORT("sort", 											"The sort direction"),
	STATEMENT("statement", 									"The statement to evaluate"),
	SUM_RANGE("sumRange", 									"The sum range"),
	TASK("task", 											"The task object (can retrieve the object by using Task(taskId) with taskId is the unique id for the task)"),
	TASK_ID("taskId", 										"The unique id of the task within the insight"),
	TRAVERSAL_KEY("traversal", 								"The traversal"),
	USE_FRAME_FILTERS("useFrameFilters", 					"A boolean indication to use frame filters"), 								
	USERNAME("username", 									"Unique identifier for the user to access a service"),
	QUERY_KEY("query", 										"The query string to be executed on the database"),
	QUERY_STRUCT("qs", 										"QueryStruct object that contains selectors, fitlers, and joins"),
	VALUE("value", 											"The value"),
	VALUES("values", 										"The values"),
	VARIABLE("variable", 									"The variable");

	
	private String key;
	private String description;
	
	private ReactorKeysEnum(String key, String description) {
		this.key = key;
		this.description = description;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public static String getDescriptionFromKey(String key) {
		for(ReactorKeysEnum e : ReactorKeysEnum.values()) {
			if(e.key.equals(key)) {
				return e.description;
			}
		}
		// if we cannot find the description above
		// it is not a standardized key
		// so just return null
		return null;
	}
}
