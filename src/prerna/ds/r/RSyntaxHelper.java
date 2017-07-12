package prerna.ds.r;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;

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
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createStringRColVec(Integer[] row) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			str.append(row[i]);
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	
	/**
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createStringRColVec(Double[] row) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			str.append(row[i]);
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
	
	/**
	 * Generate the syntax to perform a fRead to ingest a file
	 * @param tableName
	 * @param absolutePath
	 * @return
	 */
	public static String getFReadSyntax(String tableName, String absolutePath) {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append(" <- fread(\"").append(absolutePath.replace("\\", "/")).append("\")");
		return builder.toString();
	}
	
	public static String getExcelReadSheetSyntax(String tableName, String absolutePath, String sheetName) {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append(" <- as.data.table(read.xlsx2(\"").append(absolutePath.replace("\\", "/")).append("\", sheetName=\"").append(sheetName).append("\"))");
		return builder.toString();
	}
	
	/**
	 * 
	 * @param leftTableName
	 * @param rightTableName
	 * @param joinType
	 * @param joinCols
	 * @return
	 */
	public static String getMergeSyntax(String leftTableName, String rightTableName, String joinType, List<Map<String, String>> joinCols) {
		/*
		 * joinCols = [ {leftTable.Title -> rightTable.Movie} , {leftTable.Genre -> rightTable.Genre}  ]
		 */
		
		StringBuilder builder = new StringBuilder();
		
		
		
		
		return builder.toString();
	}
	
	/**
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createRColVec(Object[] row, IMetaData.DATA_TYPES[] dataType) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			if(dataType[i] == IMetaData.DATA_TYPES.STRING) {
				str.append("\"").append(row[i]).append("\"");
			} else {
				str.append(row[i]);
			}
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	
	public static void main(String[] args) {
		String leftTableName = "x";
		String rightTableName = "y";
		String joinType = "inner.join"; // left.outer.join, right.outer.join, outer.join
		List<Map<String, String>> joinCols = new Vector<Map<String, String>>();
		Map<String, String> join1 = new HashMap<String, String>();
		join1.put("", "");
		joinCols.add(join1);
		
		// when you get to multi
//		Map<String, String> join2 = new HashMap<String, String>();
//		join2.put("", "");
//		joinCols.add(join2);
		
		System.out.println(getMergeSyntax(leftTableName, rightTableName, joinType, joinCols));
	}
}
