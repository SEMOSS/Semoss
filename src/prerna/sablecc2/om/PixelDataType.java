package prerna.sablecc2.om;

import prerna.algorithm.api.SemossDataType;

public enum PixelDataType {
	
	CONST_DECIMAL ("CONST_DECIMAL", null), 					// constant double
	CONST_INT ("CONST_INT", null), 							// constant int
	CONST_STRING ("CONST_STRING", null), 					// constant string
	CONST_DATE ("CONST_DATE", null), 						// constant date
	CONST_TIMESTAMP ("CONST_TIMESTAMP", null), 				// constant timestamp
	CONST_DAY ("CONST_DAY", null), 							// constant day
	CONST_WEEK ("CONST_WEEK", null), 						// constant week
	CONST_MONTH ("CONST_MONTH", null), 						// constant month
	CONST_YEAR ("CONST_YEAR", null), 						// constant year

	SUB_QUERY_EXPRESSION("SUB_QUERY_EXPRESSION", null),
	NULL_VALUE ("NULL_VALUE", null), 						// null input
	COLUMN ("COLUMN", null), 								// column name in database or frame
	
	TASK ("TASK", ReactorKeysEnum.TASK),					// task
	TASK_LIST ("TASK_LIST", null),							// task

	REMOVE_VARIABLE ("REMOVE_VARIABLE", null),				// variable to be removed
	REMOVE_TASK ("REMOVE_TASK", null),						// task to be removed
	REMOVE_LAYER ("REMOVE_LAYER", null),					// task of a layer to remove
	DROP_INSIGHT ("DROP_INSIGHT", null),					// insight to be removed

	// insight sheet
	SHEET ("SHEET", ReactorKeysEnum.SHEET),
	
	// insight panel
	PANEL ("PANEL", ReactorKeysEnum.PANEL),
	PANEL_CLONE_MAP ("PANEL_CLONE_MAP", null),
	
	// file reference
	FILE_REFERENCE ("FILE_REFERENCE", null),
	
	// frame
	FRAME ("FRAME", ReactorKeysEnum.FRAME),
	FRAME_MAP ("FRAME_MAP", null),
	
	// filter/sort return
	BOOLEAN_METADATA("BOOLEAN_METADATA", null),
	
	// upload map
	UPLOAD_RETURN_MAP ("UPLOAD_RETURN_MAP", null),
	
	// specific ornament map 
	// reacted to in collect reactor
	// to ensure ornament data is sent properly
	ORNAMENT_MAP ("ORNAMENT_MAP", null),
	
	// param struct
	PARAM_STRUCT ("PARAM_STRUCT", ReactorKeysEnum.PARAM_STRUCT),
	
	// param struct
	PREAPPLIED_PARAM_STRUCT ("PREAPPLIED_PARAM_STRUCT", ReactorKeysEnum.PREDEFINED_PARAM_STRUCT),
	
	SQLE ("SQLE", null), 													// sql expression
	E ("E", null), 															// some other expression
	FILTER ("FILTER", ReactorKeysEnum.FILTERS), 							// filter object
	COMPARATOR ("COMPARATOR", null),										// comparator by itself
	JOIN ("JOIN", ReactorKeysEnum.JOINS),									// join object
	CODE ("CODE", null),													// code block
	VARIABLE ("VARIABLE", null),											// name of existing variable
	QUERY_STRUCT ("QUERY_STRUCT", ReactorKeysEnum.QUERY_STRUCT),			// qs
	RAW_DATA_SET ("DATA", null),											// raw data - usually from job
	FORMATTED_DATA_SET ("FORMATTED_DATA_SET", null),						// formatted data - for FE
	ITERATOR ("ITERATOR", null),											// iterator
	EXPORT ("EXPORT", null),
	LAMBDA ("LAMBDA", null),
	ALIAS ("ALIAS", ReactorKeysEnum.ALIAS),
	IN_MEM_STORE ("STORE", null),
	PLANNER ("PLANNER", null), 
	CACHED_CLASS ("CACHED_CLASS", null), 
	VECTOR ("VECTOR", null),
	MAP ("MAP", null),
	BOOLEAN ("BOOLEAN", null),
	ERROR ("ERROR", null),
	INVALID_SYNTAX("INVALID_SYNTAX", null),
	R_CONNECTION("R_CONNECTION", null),
	R_ENGINE("R_ENGINE", null),
	
	STORAGE("STORAGE", ReactorKeysEnum.STORAGE),

	EMAIL_SESSION("EMAIL_SESSION", ReactorKeysEnum.EMAIL_SESSION),
	
	PIXEL_OBJECT("PIXEL_OBJECT", null),
	CUSTOM_DATA_STRUCTURE ("CUSTOM_DATA_STRUCTURE", null),
	
	// running cached and new insights
	CACHED_PIXEL_RUNNER ("CACHED_PIXEL_RUNNER", null),
	PIXEL_RUNNER ("PIXEL_RUNNER", null),
	
	// param values on open insight
	PARAM_VALUES_MAP("PARAM_VALUES_MAP", ReactorKeysEnum.PARAM_VALUES_MAP);
	
	private final String strValue;
	private final ReactorKeysEnum reactorEnum;
	
	private PixelDataType(String strValue, ReactorKeysEnum reactorEnum) {
		this.strValue = strValue;
		this.reactorEnum = reactorEnum;
	}
	
	public String toString() {
		return this.strValue;
	}
	
	/**
	 * Get the enum key value if not null
	 * Otherwise get the string value
	 * @return
	 */
	public String getKey() {
		if(this.reactorEnum != null) {
			return this.reactorEnum.getKey();
		}
		return toString();
	}
	
	/**
	 * Convert between {@link prerna.algorithm.api.SemossDataType} and PixelDataType
	 * @param type
	 * @return
	 */
	public static PixelDataType convertFromSemossDataType(SemossDataType type) {
		if(type == SemossDataType.BOOLEAN) {
			return BOOLEAN;
		} else if(type == SemossDataType.INT) {
			return CONST_INT;
		} else if(type == SemossDataType.DOUBLE) {
			return CONST_DECIMAL;
		} else if(type == SemossDataType.STRING || type == SemossDataType.FACTOR) {
			return CONST_STRING;
		} else if(type == SemossDataType.DATE) {
			return CONST_DATE;
		} else if(type == SemossDataType.TIMESTAMP) {
			return CONST_TIMESTAMP;
		}

		return null;
	}
}


