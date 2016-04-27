package prerna.sablecc;

public class PKQLEnum {
	
	public enum PKQLToken {NUMBER, DECIMAL, COL_CSV, COL_DEF, WHERE, FILTER, SELECTOR, API, CODE}
	public enum PKQLReactor {COL_ADD, IMPORT_DATA, MATH_FUN, EXPR, R_OP, VIZ}
	public enum PKQLAlgorithm {SUM, AVERAGE, STANDARD_DEVIATION, MEDIAN, MIN, MAX, CONCAT}

	public static final String COMPARATOR = "COMPARATOR";


	public static final String EXPR_ROW = "EXPR_ROW";

	public static String NUMBER = "NUMBER";
	public static String DECIMAL = "DECIMAL";
	public static String ALPHA = "ALPHA";
	public static String EXPR_TERM = "EXPR_TERM";
	public static String EXPR_SCRIPT = "EXPR_SCRIPT";
	public static String MATH_FUN = "MATH_FUN";
	public static String SUM_MATH_FUN = "SUM_MATH_FUN";
	public static String COL_CSV = "COL_CSV";
	public static String COL_DEF = "COL_DEF";
	public static String WHERE = "WHERE";
	public static String FILTER = "FILTERS";
	public static String SELECTOR = "SELECTORS";
	public static String RELATION = "RELATION";
	public static String GROUP_BY = "GROUP_BY";
	public static String PROC_NAME = "PROC_NAME";
	public static String API = "API";
	public static String ROW_CSV = "ROW_CSV";
	public static String IMPORT_DATA = "IMPORT_DATA";
	public static String REMOVE_DATA = "REMOVE_DATA";
	public static String VIZ = "VIZ";
	public static String WORD_OR_NUM = "WORD_OR_NUM";
	public static String REL_TYPE = "REL_TYPE";
	public static String REL_DEF = "REL_DEF";
	public static String JOINS = "JOINS";
	public static String FROM_COL = "FROM_COL";
	public static String TO_COL = "TO_COL";
	public static String COL_ADD = "COL_ADD";
	public static String G = "G";
	public static String FILTER_DATA = "FILTER_DATA";
	public static String UNFILTER_DATA = "UNFILTER_DATA";
//	public static String RELATION = "RELATION";

}
