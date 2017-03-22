package prerna.sablecc2.om;

public class Filter {

	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '?like'
	private GenRowStruct lComparison = null; //the column we want to filter
	private GenRowStruct rComparison = null; //the values to bind the filter on
	
	//should we use standard strings for filter comparators across everywhere? if so we need this somewhere else
//	public static final String EQUALS = "=";
//	public static final String NOT_EQUALS = "=";
//	public static final String LESS_THAN = "=";
//	public static final String GREATER_THAN = "=";
//	public static final String LESS_THAN_EQUALS = "=";
//	public static final String GREATER_THAN_EQUALS = "=";
//	public static final String LIKE = "?like";
	
	public Filter(GenRowStruct lComparison, String comparator, GenRowStruct rComparison)
	{
		this.lComparison = lComparison;
		this.rComparison = rComparison;
		this.comparator = comparator;
	}

	public GenRowStruct getLComparison() {
		return lComparison;
	}
	
	public GenRowStruct getRComparison() {
		return rComparison;
	}
	
	public String getComparator() {
		return this.comparator;
	}
}
