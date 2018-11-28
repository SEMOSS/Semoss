package prerna.sablecc2.om;

public enum PixelDataType {
	
	CONST_DECIMAL ("CONST_DECIMAL"), 				// constant double
	CONST_INT ("CONST_INT"), 						// constant int
	CONST_STRING ("CONST_STRING"), 					// constant string
	CONST_DATE ("CONST_DATE"), 						// constant date
	CONST_TIMESTAMP ("CONST_TIMESTAMP"), 			// constant timestamp
	NULL_VALUE ("NULL_VALUE"), 						// null input
	COLUMN ("COLUMN"), 								// column name in database or frame
	
	TASK ("TASK"),									// task
	TASK_LIST ("TASK_LIST"),						// task

	REMOVE_VARIABLE ("REMOVE_VARIABLE"),			// variable to be removed
	REMOVE_TASK ("REMOVE_TASK"),					// task to be removed
	DROP_INSIGHT ("DROP_INSIGHT"),				// insight to be removed

	// insight panel
	PANEL ("PANEL"),
	PANEL_CLONE_MAP ("PANEL_CLONE_MAP"),
	
	SQLE ("SQLE"), 									// sql expression
	E ("E"), 										// some other expression
	FILTER ("FILTER"), 								// filter object
	COMPARATOR ("COMPARATOR"),						// comparator by itself
	JOIN ("JOIN"),									// join object
	CODE ("CODE"),									// code block
	VARIABLE ("VARIABLE"),							// name of existing variable
	QUERY_STRUCT ("QUERY_STRUCT"),					// qs
	RAW_DATA_SET ("DATA"),							// raw data - usually from job
	FORMATTED_DATA_SET ("FORMATTED_DATA_SET"),		// formatted data - for FE
	ITERATOR ("ITERATOR"),							// iterator
	EXPORT ("EXPORT"),
	LAMBDA ("LAMBDA"),
	ALIAS ("ALIAS"),
	FRAME ("FRAME"),
	IN_MEM_STORE ("STORE"),
	PLANNER ("PLANNER"), 
	CACHED_CLASS ("CACHED_CLASS"), 
	VECTOR ("VECTOR"),
	MAP ("MAP"),
	BOOLEAN ("BOOLEAN"),
	ERROR ("ERROR"),
	INVALID_SYNTAX("INVALID_SYNTAX"),
	R_CONNECTION("R_CONNECTION"),
	R_ENGINE("R_ENGINE"),
	CUSTOM_DATA_STRUCTURE ("CUSTOM_DATA_STRUCTURE"),
	
	// running cached and new insights
	CACHED_PIXEL_RUNNER ("CACHED_PIXEL_RUNNER"),
	PIXEL_RUNNER ("PIXEL_RUNNER");
	
	private final String strValue;
	
	private PixelDataType(String strValue) {
		this.strValue = strValue;
	}
	
	public String toString() {
		return this.strValue;
	}
}


