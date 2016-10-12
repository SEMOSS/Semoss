package prerna.ds.R;

public class RSyntaxHelper {

	/**
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createStringRColVec(Object[] row) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			str.append("\"").append(row[i]).append("\"");
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	
	/**
	 * Converts a R column type to numeric
	 * @param tableName				The name of the R table
	 * @param colName				The name of the column
	 * @return						The r script to execute
	 */
	public static String alterColumnTypeToNumeric(String tableName, String colName) {
		// will generate a string similar to
		// "datatable$Revenue_International <- as.numeric(as.character(datatable$Revenue_International))"
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("$").append(colName).append(" <- ").append("as.numeric(as.character(")
			.append(tableName).append("$").append(colName).append("))");
		return builder.toString();
	}
	
}
