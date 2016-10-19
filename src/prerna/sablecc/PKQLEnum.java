package prerna.sablecc;

public class PKQLEnum {
	
//	public enum PKQLToken {NUMBER, DECIMAL, COL_CSV, COL_DEF, WHERE, FILTER, SELECTOR, API, CODE}
	public enum PKQLReactor {COL_RENAME, COL_ADD, IMPORT_DATA, MATH_FUN, EXPR, VIZ, VAR, INPUT, NETWORK_CONNECT, NETWORK_DISCONNECT, DATA_FRAME_HEADER, DATA_FRAME_DUPLICATES}
//	public enum PKQLAlgorithm {SUM, AVERAGE, STANDARD_DEVIATION, MEDIAN, MIN, MAX, CONCAT}

	public static final String COMPARATOR = "COMPARATOR";


	public static final String EXPR_ROW = "EXPR_ROW";


	public static final String VAR_TERM = "VAR_TERM";


	public static final String INPUT = "INPUT";


	public static final String JAVA_OP = "JAVA_OP";

	public static final String DATA_FRAME = "DATA_FRAME";
	public static final String DATA_FRAME_HEADER = "DATA_FRAME_HEADER";
	public static final String DATA_FRAME_DUPLICATES = "DATA_FRAME_DUPLICATES";
	public static final String DATA_TYPE = "DATA_TYPE";
	public static final String DATA_CONNECT = "DATA_CONNECT";
	
	public static final String DATABASE_LIST = "DATABASE_LIST";
	public static final String DATABASE_CONCEPTS = "DATABASE_CONCEPTS";
	public static final String DATABASE_METAMODEL = "DATABASE_METAMODEL";

	public static final String MATH_FUN = "MATH_FUN";
	// this is to override default sum reactor
	// example of use is using R for calculation
	public static final String ABSOLUTE = "ABSOLUTE";
	public static final String SUM = "SUM";
	public static final String MAX = "MAX";
	public static final String MIN = "MIN";
	public static final String AVERAGE = "AVERAGE";
	public static final String STANDARD_DEVIATION = "STANDARDDEVIATION";
	public static final String MEDIAN = "MEDIAN";
	public static final String COUNT = "COUNT";
	public static final String ROUND = "ROUND";
	public static final String CONCAT = "CONCAT";

	public static final String NUMBER = "NUMBER";
	public static final String DECIMAL = "DECIMAL";
	public static final String ALPHA = "ALPHA";
	public static final String EXPR_TERM = "EXPR_TERM";
	public static final String EXPR_SCRIPT = "EXPR_SCRIPT";
	public static final String MATH_PARAM = "MATH_PARAM";
	public static final String COL_CSV = "COL_CSV";
	public static final String COL_DEF = "COL_DEF";
	public static final String WHERE = "WHERE";
	public static final String FILTER = "FILTERS";
	public static final String SELECTOR = "SELECTORS";
	public static final String RELATION = "RELATION";
	public static final String GROUP_BY = "GROUP_BY";
	public static final String PROC_NAME = "PROC_NAME";
	public static final String API = "API";
	public static final String CSV_TABLE = "CSV_TABLE";
	public static final String PASTED_DATA = "PASTED_DATA";
	public static final String DASHBOARD_JOIN	 = "DASHBOARD_JOIN";
	public static final String DASHBOARD_ADD = "DASHBOARD_ADD";
	public static final String DASHBOARD_REMOVE = "DASHBOARD_REMOVE";
	public static final String ROW_CSV = "ROW_CSV";
	public static final String IMPORT_DATA = "IMPORT_DATA";
	public static final String REMOVE_DATA = "REMOVE_DATA";
	public static final String QUERY_DATA = "QUERY_DATA";
	public static final String VIZ = "VIZ";
	public static final String WORD_OR_NUM = "WORD_OR_NUM";
	public static final String REL_TYPE = "REL_TYPE";
	public static final String REL_DEF = "REL_DEF";
	public static final String JOINS = "JOINS";
	public static final String TABLE_JOINS = "TABLE_JOINS";
	public static final String FROM_COL = "FROM_COL";
	public static final String TO_COL = "TO_COL";
	public static final String COL_ADD = "COL_ADD";
	public static final String G = "G";
	public static final String FILTER_DATA = "FILTER_DATA";
	public static final String UNFILTER_DATA = "UNFILTER_DATA";
	public static final String OPEN_DATA = "OPEN_DATA";
	public static final String COL_SPLIT = "COL_SPLIT";
	public static final String REGEX = "REGEX";
	public static final String COL_RENAME = "COL_RENAME";
	public static final String JOIN_PARAM = "JOIN_PARAM";
	public static final String COL_TABLE = "COL_TABLE";
	public static final String DATA_CONNECTDB = "DATA_CONNECTDB";
	public static final String VAR_PARAM = "VAR_PARAM";
	public static final String TERM = "TERM";
	public static final String MAP_OBJ = "MAP_OBJECT";
	public static final String KEY_VALUE_PAIR = "KEY_VALUE";
	public static final String NETWORK_CONNECT = "NETWORK_CONNECT";
	public static final String NETWORK_DISCONNECT = "NETWORK_DISCONNECT";
	public static final String HISTOGRAM = "HISTOGRAM";
	
	public static final String QUERY_API = "QUERY_API";
	public static final String CSV_API = "CSV_API";
	public static final String R_API = "R_API";
	public static final String WEB_API = "WEB_API";
	public static final String NATIVE_QUERY_API = "NATIVE_QUERY_API";
	public static final String SEARCH_QUERY_API = "SEARCH_QUERY_API";
	
}
