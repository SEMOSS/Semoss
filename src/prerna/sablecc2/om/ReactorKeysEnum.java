package prerna.sablecc2.om;

public enum ReactorKeysEnum {

	ADDITIONAL_PIXELS("additionalPixels",					"Additional pixels to be executed in addition to the pixel steps saved within the insight"),
	ALL_NUMERIC_KEY("allNumeric", 							"Indicates if only numeric headers should be returned"),
	ALIAS("alias", 											"An alias to assign for an operation or output"),
	APP("app", 												"Name of the app on local version"),
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
	CRITERIA("criteria", 									"Criteria to be evaluated"),
	DB_DRIVER_KEY("dbDriver",								"Name of the JDBC driver.  Not all JDBC drivers are open source so make sure you include the driver within the classpath of SEMOSS"),
	DATA_TYPE("dataType", 									"Data type of the column (STRING, NUMBER, DATE)"),
	DATA_TYPES("dataTypeMap", 								"Map of column name to the column data types"),
	DEFAULT_VALUE_KEY("defaultValue", 						"A default value to use for null columns"),
	DELIMITER("delimiter", 									"Delimiter to be used"),
	ENGINE("engine", 										"Name of the datasource"),
	EVENTS_KEY("events", 									"Events map input"),
	FILENAME("fileName", 									"Name of the file"),
	FILE_PATH("path", 										"File path"),
	FILTERS("filters", 										"Filters automatically persisted on queries affecting this frame or panel"),
	FILTER_WORD("filterWord", 								"Regex to apply for searches"),
	FRAME("frame", 											"The frame"),
	FRAME_TYPE("frameType", 								"Type of frame to generate - grid (sql based frame), graph (frame based on tinkerpop), r (data sits within r, must have r installed to use), native (leverages the database to execute queries)"),
	HEADER_NAMES("newHeaders", 								"New header names"),
	IMAGE_URL("imageUrl", 									"Image URL"),
	INCLUDE_META_KEY("meta", 								"Boolean indication of whether to retrieve metadata"),
	INDEX("index", 											"A specified index"),
	INSIGHT_ID("id", 										"Unique id of the insight"),
	INSIGHT_NAME("insightName", 							"Name of the insight"),
	INSIGHT_ORNAMENT("insightOrnament", 					"Insight ornament map"),
	INSTANCE_KEY("instance", 								"Column representing the objects being used to perform the operation"),
	JOB_ID("jobId", 										"Id of the job"),
	JOINS("joins", 											"Joins on the frame"),
	LAYOUT_KEY("layout", 									"The layout"),
	LIMIT("limit", 											"Limit to add for the query results"),
	LOGICAL_NAME("logicalNames", 							"Column alias to be added to the master database"),
	NEW_COLUMN("newCol", 									"Name of the new column being created"),
	NEW_VALUE("newValue", 									"New value used to replace an existing value"),
	NUMERIC_VALUE("numValue", 								"Numeric value to be used in the operation"),
	NUMERIC_VALUES("numValues", 							"Numeric values to be used in the operation"),
	OFFSET("offset", 										"Offset to add for the query results"),
	OPTIONS("options", 										"Map of option values"),
	ORNAMENTS_KEY("ornaments", 								"Panel ornaments"),
	PANEL("panel", 											"Id of the panel"), 								
	PANEL_LABEL_KEY("panelLabel", 							"Label for the panel"),
	PANEL_POSITION_KEY("position", 							"Panel position map"),
	PANEL_VIEW_KEY("panelView", 							"Text that specifies what is displayed in the panel (e.g., \"visualization\""),
	PANEL_VIEW_OPTIONS_KEY("panelViewOptions", 				"The panel view options"),
	PARAM_KEY("params", 									"The parameters for the insight map"),
	PASSWORD("password", 									"Password used in conjunction with the username for access to a service"),
	PLANNER("planner", 										"The planner"),
	REACTOR("reactor", 										"Reactor name"),
	RECIPE("recipe", 										"Recipe that shows the sequence of pixels resulting in the insight"),
	REGEX("regex", 											"Regular expression (sequence of characters)"),
	REPOSITORY("repository", 								"Name of the repository. Usually namespaced to be username\\appname"),
	SESSION_ID("sessionId", 								"Id of the session"),
	SORT("sort", 											"Sort direction: ascending (\"asc\") or decending (\"desc\")"),
	STATEMENT("statement", 									"Statement to be evaluated"),
	SUM_RANGE("sumRange", 									"Range that values to sum over"),
	SYNC_DATABASE("syncDatabase", 							"True/False value to detetermine if the database should be published with the app"),
	TASK("task", 											"Task object (can retrieve the object by using Task(taskId) with taskId is the unique id for the task)"),
	TASK_ID("taskId", 										"Unique id of the task within the insight"),
	TRAVERSAL_KEY("traversal", 								"The traversal"),
	USE_FRAME_FILTERS("useFrameFilters", 					"A boolean indication to use frame filters"), 								
	USERNAME("username", 									"Unique identifier for the user to access a service"),
	QUERY_KEY("query", 										"Query string to be executed on the database"),
	QUERY_STRUCT("qs", 										"QueryStruct object that contains selectors, filters, and joins"),
	VALUE("value", 											"The instance value in a column, or the numeric or string value used in a operation"),
	VALUES("values", 										"Numeric or string values used as input text or inputs to an operation"),
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
