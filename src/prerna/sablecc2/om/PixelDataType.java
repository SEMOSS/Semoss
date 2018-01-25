package prerna.sablecc2.om;

public enum PixelDataType {
	
	CONST_DECIMAL ("CONST_DECIMAL"), 				// constant double
	CONST_INT ("CONST_INT"), 						// constant int
	CONST_STRING ("CONST_STRING"), 					// constant string
	CONST_DATE ("CONST_DATE"), 						// constant date

	COLUMN ("COLUMN"), 								// column name in table
	SQLE ("SQLE"), 									// sql expression
	E ("E"), 										// some other expression
	FILTER ("FILTER"), 								// filter object
	COMPARATOR ("COMPARATOR"),						// comparator by itself
	JOIN ("JOIN"),									// join object
	CODE ("CODE"),									// code block
	VARIABLE ("VARIABLE"),							// name of existing variable
	QUERY_STRUCT ("QUERYSTRUCT"),					// qs
	RAW_DATA_SET ("DATA"),							// raw data - usually from job
	FORMATTED_DATA_SET ("FDATA"),					// formatted data - for FE
	TASK ("TASK"),									// task
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
	PANEL ("PANEL"),
	R_CONNECTION("R_CONNECTION"),
	R_ENGINE("R_ENGINE"),
	CUSTOM_DATA_STRUCTURE ("CUSTOM_DATA_STRUCTURE"),
	VIZ_RECOMMENDATION ("VIZ_RECOMMENDATION");

	private final String strValue;
	
	private PixelDataType(String strValue) {
		this.strValue = strValue;
	}
	
	public String toString() {
		return this.strValue;
	}
}


