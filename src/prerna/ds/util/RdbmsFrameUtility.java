package prerna.ds.util;

import java.util.UUID;

import prerna.ds.AbstractTableDataFrame;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RdbmsFrameUtility {

	private static final String H2FRAME = "H2FRAME";
	
	public static String cleanInstance(String value) {
		return value.replace("'", "''");
	}
	
	public static String cleanHeader(String header) {
		header = header.replaceAll("[#%!&()@#$'./-]*\"*", ""); // replace all the useless shit in one go
		header = header.replaceAll("\\s+", "_");
		header = header.replaceAll(",", "_");
		if (Character.isDigit(header.charAt(0)))
			header = "c_" + header;
		return header;
	}
	
	public static String getComparatorStringValue(AbstractTableDataFrame.Comparator comparator) {
		
		if(AbstractTableDataFrame.Comparator.EQUAL.equals(comparator)) {
			return "=";
		} else if(AbstractTableDataFrame.Comparator.GREATER_THAN.equals(comparator)) {
			return ">";
		} else if(AbstractTableDataFrame.Comparator.GREATER_THAN_EQUAL.equals(comparator)) {
			return ">=";
		} else if(AbstractTableDataFrame.Comparator.LESS_THAN.equals(comparator)) {
			return "<";
		} else if(AbstractTableDataFrame.Comparator.LESS_THAN_EQUAL.equals(comparator)) {
			return "<=";
		} else if(AbstractTableDataFrame.Comparator.NOT_EQUAL.equals(comparator)) {
			return "!=";
		} else {
			return null;
		}
	}
	
	public static AbstractTableDataFrame.Comparator getStringComparatorValue(String comparator) {
		if("=".equals(comparator)) {
			return AbstractTableDataFrame.Comparator.EQUAL;
		} else if(">".equals(comparator)) {
			return AbstractTableDataFrame.Comparator.GREATER_THAN;
		} else if(">=".equals(comparator)) {
			return AbstractTableDataFrame.Comparator.GREATER_THAN_EQUAL;
		} else if("<".equals(comparator)) {
			return AbstractTableDataFrame.Comparator.LESS_THAN;
		} else if("<=".equals(comparator)) {
			return AbstractTableDataFrame.Comparator.LESS_THAN_EQUAL;
		} else if("!=".equals(comparator)) {
			return AbstractTableDataFrame.Comparator.NOT_EQUAL;
		} else {
			return null;
		}
	}
	
	// get a new unique table name
	public static String getNewTableName() {
		String name = H2FRAME + getUUID();
		return name;
	}
	
	public static String getUUID() {
		String uuid = UUID.randomUUID().toString();
		uuid = uuid.replaceAll("-", "_");
		// table names will be upper case because that is how it is set in information schema
		return uuid.toUpperCase();
	}
	
	public static int getLimitSize() {
		String limitSize = (String) DIHelper.getInstance().getProperty(Constants.H2_IN_MEM_SIZE);
		int LIMIT_SIZE;
		if (limitSize == null) {
			LIMIT_SIZE = 10_000;
		} else {
			try {
				int val = Integer.parseInt(limitSize.trim());
				if(val < 0) {
					LIMIT_SIZE = Integer.MAX_VALUE;
				} else {
					LIMIT_SIZE = val;//Integer.parseInt(limitSize.trim());
				}
			} catch(Exception e) {
				LIMIT_SIZE = 10_000;
			}
		}
		return LIMIT_SIZE;
	}
}
