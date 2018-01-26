package prerna.ds.r;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;

public class RSyntaxHelper {

	private RSyntaxHelper() {
		
	}
	
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
	 * Converts a R column type to date
	 * @param tableName				The name of the R table
	 * @param colName				The name of the column
	 * @return						The r script to execute
	 */
	public static String alterColumnTypeToDate(String tableName, String colName) {
		// will generate a string similar to
		// "datatable$Birthday <- as.Date(as.character(datatable$Birthday), format = '%m/%d/%Y')"
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("$").append(colName).append(" <- ").append("as.Date(as.character(")
		.append(tableName).append("$").append(colName).append("), format = '%Y-%m-%d')");
		return builder.toString();
	}

	public static String alterColumnTypeToDateTime(String tableName, String colName) {
		// will generate a string similar to
		// "datatable$Birthday <- as.Date(as.character(datatable$Birthday), format = '%m/%d/%Y')"
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("$").append(colName).append(" <- ").append("as.Date(as.character(")
		.append(tableName).append("$").append(colName).append("), format = '%Y-%m-%d %H:%M:%S')");
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
	public static String getMergeSyntax(String returnTable, String leftTableName, String rightTableName, String joinType, List<Map<String, String>> joinCols) {
		/*
		 * joinCols = [ {leftTable.Title -> rightTable.Movie} , {leftTable.Genre -> rightTable.Genre}  ]
		 */

		StringBuilder builder = new StringBuilder();
		if (joinType.equals("inner.join")) {
			builder.append(returnTable).append(" <- ").append("merge(").append(leftTableName).append(", ")
			.append(rightTableName).append(", ").append("by.x").append(" = c(");
			getLeftMergeCols(builder, joinCols);
			builder.append("), by.y").append(" = c(");
			getRightMergeCols(builder, joinCols);
			builder.append("), all = FALSE)");
		} else if (joinType.equals("left.outer.join")) {
			builder.append(returnTable).append(" <- ").append("merge(").append(leftTableName).append(", ")
			.append(rightTableName).append(", ").append("by.x").append(" = c(");
			getLeftMergeCols(builder, joinCols);
			builder.append("), by.y").append(" = c(");
			getRightMergeCols(builder, joinCols);
			builder.append("), all.x")
			.append(" = TRUE,").append(" all.y").append(" = FALSE)");
		} else if (joinType.equals("right.outer.join")) {
			builder.append(returnTable).append(" <- ").append("merge(").append(leftTableName).append(", ")
			.append(rightTableName).append(", ").append("by.x").append(" = c("); 
			getLeftMergeCols(builder, joinCols);
			builder.append("), by.y").append(" = c(");
			getRightMergeCols(builder, joinCols);
			builder.append("), all.x").append(" = FALSE,").append(" all.y").append(" = TRUE)");
		} else if (joinType.equals("outer.join")) {
			builder.append(returnTable).append(" <- ").append("merge(").append(leftTableName).append(", ")
			.append(rightTableName).append(", ").append("by.x").append(" = c(");
			getLeftMergeCols(builder, joinCols);
			builder.append("), by.y").append(" = c(");
			getRightMergeCols(builder, joinCols);
			builder.append("), all.x").append(" = FALSE,").append(" all.y").append(" = FALSE)");
		}

		return builder.toString();
	}

	/**
	 * Get the correct r syntax for the left table join columns
	 * This uses the join information keys since it is {leftCol -> rightCol}
	 * @param builder
	 * @param colNames
	 */
	public static void getLeftMergeCols(StringBuilder builder, List<Map<String, String>> colNames){
		// iterate through the map
		boolean firstLoop = true;
		int numJoins = colNames.size();
		for(int i = 0; i < numJoins; i++) {
			Map<String, String> joinMap = colNames.get(i);
			// just in case an empty map is passed
			// we do not want to modify the firstLoop boolean
			if(joinMap.isEmpty()) {
				continue;
			}
			if(!firstLoop) {
				builder.append(",");
			}
			// this should really be 1
			// since each join between 2 columns is its own map
			Set<String> leftTableCols = joinMap.keySet();
			// keep track of where to add a ","
			int counter = 0;
			int numCols = leftTableCols.size();
			for(String leftColName : leftTableCols){
				builder.append("\"").append(leftColName).append("\"");
				if(counter+1 != numCols) {
					builder.append(",");
				}
				counter++;
			}
			firstLoop = false;
		}
	}

	/**
	 * Get the correct r syntax for the left table join columns
	 * This uses the join information values since it is {leftCol -> rightCol} 
	 * @param builder
	 * @param colNames
	 */
	public static void getRightMergeCols(StringBuilder builder, List<Map<String, String>> colNames){
		// iterate through the map
		boolean firstLoop = true;
		int numJoins = colNames.size();
		for(int i = 0; i < numJoins; i++) {
			Map<String, String> joinMap = colNames.get(i);
			// just in case an empty map is passed
			// we do not want to modify the firstLoop boolean
			if(joinMap.isEmpty()) {
				continue;
			}
			if(!firstLoop) {
				builder.append(",");
			}
			// this should really be 1
			// since each join between 2 columns is its own map
			Collection <String> rightTableCols = joinMap.values();
			// keep track of where to add a ","
			int counter = 0;
			int numCols = rightTableCols.size();
			for(String rightColName : rightTableCols){
				builder.append("\"").append(rightColName).append("\"");
				if(counter+1 != numCols) {
					builder.append(",");
				}
				counter++;
			}
			firstLoop = false;
		}
	}

	/**
	 * Convert a java object[] into a r column vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createRColVec(Object[] row, SemossDataType[] dataType) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.length;
		for(; i < size; i++) {
			if(dataType[i] == SemossDataType.STRING) {
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
	
	/**
	 * Convert a r vector from a java vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createRColVec(List<Object> row, SemossDataType dataType) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.size();
		for(; i < size; i++) {
			if(SemossDataType.STRING == dataType) {
				str.append("\"").append(row.get(i)).append("\"");
			} else if(SemossDataType.INT == dataType || SemossDataType.DOUBLE == dataType) {
				str.append(row.get(i).toString());
			} else if(SemossDataType.DATE == dataType) {
				str.append("as.Date(\"").append(row.get(i).toString()).append("\", format='%Y-%m-%d');");
			} else {
				// just in case this is not defined yet...
				// see the type of the value and add it in based on that
				if(dataType == null) {
					if(row.get(i) instanceof String) {
						str.append("\"").append(row.get(i)).append("\"");
					} else {
						str.append(row.get(i));
					}
				} else {
					str.append(row.get(i));
				}
			}
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}

	public static String formatFilterValue(Object value, SemossDataType dataType) {
		if(SemossDataType.STRING == dataType) {
			return "\"" + value + "\"";
		} else if(SemossDataType.INT == dataType || SemossDataType.DOUBLE == dataType) {
			return value.toString();
		} else if(SemossDataType.DATE == dataType) {
			return "as.Date(\"" + value.toString() + "\", format='%Y-%m-%d')";
		} else {
			// just in case this is not defined yet...
			// see the type of the value and add it in based on that
			if(dataType == null) {
				if(value instanceof String) {
					return "\"" + value + "\"";
				} else {
					return value + "";
				}
			} else {
				return value + "";
			}
		}
	}

	public static void main(String[] args) {
		// testing inner
		System.out.println("testing inner...");
		System.out.println("testing inner...");
		System.out.println("testing inner...");
		System.out.println("testing inner...");
		System.out.println("testing inner...");

		String returnTable = "tableToTest";
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

		System.out.println(getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols));

		System.out.println("testing left...");
		System.out.println("testing left...");
		System.out.println("testing left...");
		System.out.println("testing left...");
		System.out.println("testing left...");

		returnTable = "tableToTest";
		joinType = "left.outer.join"; // left.outer.join, right.outer.join, outer.join

		System.out.println(getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols));
	}
}
