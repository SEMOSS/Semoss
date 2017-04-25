package prerna.sablecc2.om;

public enum PkslDataTypes {
	
	CONST_DECIMAL ("CONST_DECIMAL"), 				// constant number
	CONST_STRING ("CONST_STRING"), 					// constant string
	COLUMN ("COLUMN"), 								// column name in table
	SQLE ("SQLE"), 									// sql expression
	E ("E"), 										// some other expression
	FILTER ("FILTER"), 								// filter object
	COMPARATOR ("COMPARATOR"),						// comparator by itself
	JOIN ("JOIN"),									// join object
	RCODE ("RCODE"),								// r code block
	VARIABLE ("VARIABLE"),							// name of existing variable
	QUERY_STRUCT ("QUERYSTRUCT"),					// qs
	RAW_DATA_SET ("DATA"),							// raw data - usually from job
	FORMATTED_DATA_SET ("FDATA"),					// formatted data - for FE
	JOB ("JOB"),									// job
	ITERATOR ("ITERATOR"),							// iterator
	EXPORT ("EXPORT"),
	LAMBDA ("LAMBDA"),
	ALIAS("ALIAS"),
	FRAME("FRAME"),
	IN_MEM_STORE("STORE"),
	PLANNER("PLANNER"), 
	CACHED_CLASS("CACHED_CLASS"), 
	VECTOR("VECTOR");
	
	private final String strValue;
	
	private PkslDataTypes(String strValue) {
		this.strValue = strValue;
	}
	
	public String toString() {
		return this.strValue;
	}
}


