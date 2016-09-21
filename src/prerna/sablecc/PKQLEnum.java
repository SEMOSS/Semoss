package prerna.sablecc;

public class PKQLEnum {
	
	public enum PKQLToken {NUMBER, DECIMAL, COL_CSV, COL_DEF, WHERE, FILTER, SELECTOR, API, CODE}
	public enum PKQLReactor {COL_ADD, IMPORT_DATA, MATH_FUN, EXPR, VIZ, VAR, INPUT, NETWORK_CONNECT, NETWORK_DISCONNECT}
	public enum PKQLAlgorithm {SUM, AVERAGE, STANDARD_DEVIATION, MEDIAN, MIN, MAX, CONCAT}

	public static final String COMPARATOR = "COMPARATOR";


	public static final String EXPR_ROW = "EXPR_ROW";


	public static final String VAR_TERM = "VAR_TERM";


	public static final String INPUT = "INPUT";


	public static final String JAVA_OP = "JAVA_OP";

	public static String DATA_FRAME = "DATA_FRAME";
	public static String DATA_TYPE = "DATA_TYPE";
	public static String DATA_CONNECT = "DATA_CONNECT";
	
	public static String DATABASE_LIST = "DATABASE_LIST";
	public static String DATABASE_CONCEPTS = "DATABASE_CONCEPTS";

	public static String NUMBER = "NUMBER";
	public static String DECIMAL = "DECIMAL";
	public static String ALPHA = "ALPHA";
	public static String EXPR_TERM = "EXPR_TERM";
	public static String EXPR_SCRIPT = "EXPR_SCRIPT";
	public static String MATH_FUN = "MATH_FUN";
	public static String MATH_PARAM = "MATH_PARAM";
	public static String MOD_MATH_FUN = "MOD_MATH_FUN";
	public static String COL_CSV = "COL_CSV";
	public static String COL_DEF = "COL_DEF";
	public static String WHERE = "WHERE";
	public static String FILTER = "FILTERS";
	public static String SELECTOR = "SELECTORS";
	public static String RELATION = "RELATION";
	public static String GROUP_BY = "GROUP_BY";
	public static String PROC_NAME = "PROC_NAME";
	public static String API = "API";
	public static String CSV_TABLE = "CSV_TABLE";
	public static String PASTED_DATA = "PASTED_DATA";
	public static String DASHBOARD_JOIN	 = "DASHBOARD_JOIN";
	public static String DASHBOARD_ADD = "DASHBOARD_ADD";
	public static String ROW_CSV = "ROW_CSV";
	public static String IMPORT_DATA = "IMPORT_DATA";
	public static String REMOVE_DATA = "REMOVE_DATA";
	public static String VIZ = "VIZ";
	public static String WORD_OR_NUM = "WORD_OR_NUM";
	public static String REL_TYPE = "REL_TYPE";
	public static String REL_DEF = "REL_DEF";
	public static String JOINS = "JOINS";
	public static String TABLE_JOINS = "TABLE_JOINS";
	public static String FROM_COL = "FROM_COL";
	public static String TO_COL = "TO_COL";
	public static String COL_ADD = "COL_ADD";
	public static String G = "G";
	public static String FILTER_DATA = "FILTER_DATA";
	public static String UNFILTER_DATA = "UNFILTER_DATA";
	public static String OPEN_DATA = "OPEN_DATA";
	public static String COL_SPLIT = "COL_SPLIT";
	public static String REGEX = "REGEX";
	public static String COL_RENAME = "COL_RENAME";
	public static String JOIN_PARAM = "JOIN_PARAM";
	public static String COL_TABLE = "COL_TABLE";
	public static String DATA_CONNECTDB = "DATA_CONNECTDB";
	public static String VAR_PARAM = "VAR_PARAM";
	public static String TERM = "TERM";
	public static String MAP_OBJ = "MAP_OBJECT";
	public static String KEY_VALUE_PAIR = "KEY_VALUE";
	public static final String NETWORK_CONNECT = "NETWORK_CONNECT";
	public static final String NETWORK_DISCONNECT = "NETWORK_DISCONNECT";
	
	public static String QUERY_API = "QUERY_API";
	public static String CSV_API = "CSV_API";
	public static String R_API = "R_API";
	public static String WEB_API = "WEB_API";
	
}
