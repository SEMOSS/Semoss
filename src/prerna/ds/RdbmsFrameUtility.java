package prerna.ds;

public class RdbmsFrameUtility {

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
}
