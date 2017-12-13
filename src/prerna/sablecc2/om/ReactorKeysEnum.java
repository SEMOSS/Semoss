package prerna.sablecc2.om;

public enum ReactorKeysEnum {

	ADDITIONAL_PIXELS("additionalPixels","Additional pixels to be used"),
	ALL_NUMERIC_KEY("allNumeric", "Indicates if only numeric headers should be returned"),
	ALIAS("alias", "The alias"),
	ATTRIBUTES("attributes", "The list of attribute columns"),
	BREAKS("breaks", "The number of breaks"),
	CLONE_PANEL_KEY("cloneId", "The id of the new panel"),
	CLUSTER_KEY("numClusters", "The number of clusters"),
	COLUMN("column", "The name of the column"),
	COLUMNS("columns", "The list of columns"),
	COMMENT_KEY("comment", "The comment inputs"),
	CONCEPT("concept", "The concept"),
	CONCEPTS("concepts", "The concepts"),
	CONNECTION_STRING_KEY("connectionString", "The connection string for connecting to the db"),
	DB_DRIVER_KEY("dbDriver","The jdbc driver"), //TODO should we list the specific options?
	DATA_TYPE("dataType", "The data type of the column"),
	DATA_TYPES("dataTypeMap", "The map of column names to the column data types"),
	DEFAULT_VALUE_KEY("defaultValue", "The default value"),
	DELIMITER("delimiter", "The delimiter to be used"),
	DELIMITERS("delimiters", "The delimiters to be used"),
	ENGINE("engine", "The name of the engine"),
	EVENTS_KEY("events", "The events map input"),
	FILENAME("fileName", "The name of the file"),
	FILE_PATH("path", "The file path"),
	FILTERS("filters", "The filters on the frame"),
	FILTER_WORD("filterWord", "The filter word"),
	FRAME("frame", "The frame"),
	FRAME_TYPE("frameType", "The type of frame"),
	HEADER_NAMES("newHeaders", "The new header names"),
	IMAGE_URL("imageUrl", "The image URL"),
	INDEX("index", "A specified index"),
	INSIGHT_ID("id", "The id of the insight"),
	INSIGHT_IDS("insights", "The ids of the insights"),
	INSIGHT_NAME("insightName", "The name of the insight"),
	INSIGHT_ORNAMENT("insightOrnament", "The insight ornament map"),
	INSTANCE_KEY("instance", "The instance column"),
	JOB_ID("jobId", "The id of the job"),
	JOINS("joins", "The joins on the frame"),
	KEY_NOUN("key", "The unique key"),
	LAYOUT_KEY("layout", "The layout"),
	LIMIT("limit", "The limit"),
	LOGICAL_NAME("logicalNames", "The column alias to be added to the master database"),
	NEW_COLUMN("newCol", "The name of the new column being created"),
	NEW_VALUE("newValue", "The new value used to replace an existing value"),
	NUMERIC_VALUE("numValue", "The numeric value to be used"),
	NUMERIC_VALUES("numValues", "The numeric values to be used in the operation"),
	OFFSET("offset", "The offset"),
	OLD_ID_KEY("oldIds", "The old ids"),
	ORNAMENTS_KEY("ornaments", "The panel ornaments"),
	PANEL("panel", "The id of the panel"), 
	PANEL_LABEL_KEY("panelLabel", "The label for the panel"),
	PANEL_POSITION_KEY("position", "The panel position map"),
	PANEL_VIEW_KEY("panelView", "The panel view"),
	PANEL_VIEW_OPTIONS_KEY("panelViewOptions", "The panel view options"),
	PARAM_KEY("params", "The parameters for the insight map"),
	PASSWORD("password", "The password"),
	PLANNER("planner", "The planner"),
	RECIPE("recipe", "The recipe"),
	REGEX("regex", "The regular expression"),
	SESSION_ID("sessionId", "The id of the session"),
	SORT("sort", "The sort direction"),
	STORE_NOUN("store", "The store name"),
	TRAVERSAL_KEY("traversal", "The traversal"),
	USERNAME("username", "The username"),
	QUERY_KEY("query", "The query string to be executed on the database"),
	QUERY_STRUCT("qs", "The QueryStruct condition"),
	VALUE("value", "The value"),
	VALUES("values", "The values"),
	VARIABLE("variable", "The variable");
	
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
