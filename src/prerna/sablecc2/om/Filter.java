package prerna.sablecc2.om;

public class Filter {

	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '?like'
	private String selector = null; //the column we want to filter
	private GenRowStruct values = null; //the values to bind the filter on
	
	//should we use standard strings for filter comparators across everywhere? if so we need this somewhere else
//	public static final String EQUALS = "=";
//	public static final String NOT_EQUALS = "=";
//	public static final String LESS_THAN = "=";
//	public static final String GREATER_THAN = "=";
//	public static final String LESS_THAN_EQUALS = "=";
//	public static final String GREATER_THAN_EQUALS = "=";
//	public static final String LIKE = "?like";
	
	public Filter(String lCol, String comparator, GenRowStruct qualifiers)
	{
		selector = lCol;
		values = qualifiers;
		this.comparator = comparator;
	}

	public String getSelector() {
		return selector;
	}
	
	public GenRowStruct getValues() {
		return values;
	}
	
	public String getComparator() {
		return this.comparator;
	}
	
}
