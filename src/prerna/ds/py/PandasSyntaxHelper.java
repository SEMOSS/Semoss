package prerna.ds.py;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.Join;

public class PandasSyntaxHelper {

	private PandasSyntaxHelper() {
		
	}

	/**
	 * Execute a .py file
	 * @param fileLocation
	 * @return
	 */
	public static String execFile(String fileLocation) {
		return "execfile('" + fileLocation.replaceAll("\\\\+", "/") + "')";
	}
	
	/**
	 * Get the syntax to load a csv file
	 * @param fileLocation
	 * @param tableName
	 * @param tableName2 
	 */
	public static String getCsvFileRead(String pandasImportVar, String fileLocation, String tableName) {
		String readCsv = tableName + " = " + pandasImportVar + ".read_csv('" + fileLocation.replaceAll("\\\\+", "/") + "')";
		return readCsv;
	}
	
	/**
	 * 
	 * @param leftTableName
	 * @param rightTableName
	 * @param joinType
	 * @param joinCols
	 * @return
	 */
	public static String getMergeSyntax(String pandasFrameVar, String returnTable, String leftTableName, String rightTableName, String joinType, List<Map<String, String>> joinCols) {
		/*
		 * joinCols = [ {leftTable.Title -> rightTable.Movie} , {leftTable.Genre -> rightTable.Genre}  ]
		 */

		StringBuilder builder = new StringBuilder();
		builder.append(returnTable).append(" = ").append(pandasFrameVar).append(".merge(").append(leftTableName).append(", ")
			.append(rightTableName).append(", left_on=[");
		getMergeColsSyntax(builder, joinCols);
		builder.append("], right_on=[");
		getMergeColsSyntax(builder, joinCols);
		
		if (joinType.equals("inner.join")) {
			builder.append("], how=\"inner\")");
		} else if (joinType.equals("left.outer.join")) {
			builder.append("], how=\"left\")");
		} else if (joinType.equals("right.outer.join")) {
			builder.append("], how=\"right\")");
		} else if (joinType.equals("outer.join")) {
			builder.append("], how=\"outer\")");
		}

		return builder.toString();
	}

	/**
	 * Get the correct r syntax for the table join columns
	 * This uses the join information keys since it is {leftCol -> rightCol}
	 * @param builder
	 * @param colNames
	 */
	public static void getMergeColsSyntax(StringBuilder builder, List<Map<String, String>> colNames){
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
			Set<String> tableCols = joinMap.keySet();
			// keep track of where to add a ","
			int counter = 0;
			int numCols = tableCols.size();
			for(String colName : tableCols){
				builder.append("\"").append(colName).append("\"");
				if(counter+1 != numCols) {
					builder.append(",");
				}
				counter++;
			}
			firstLoop = false;
		}
	}
	
	public static String alterColumnName(String tableName, String oldHeader, String newHeader) {
		return tableName + ".rename(columns={'" + oldHeader + "':'" + newHeader + "'}, inplace=True)";
	}
	
	/**
	 * Generate an alter statement to add new columns, taking into consideration joins
	 * and new column alias's
	 * @param tableName
	 * @param existingColumns
	 * @param newColumns
	 * @param joins
	 * @param newColumnAlias
	 * @return
	 */
	public static String alterMissingColumns(String tableName, String[] curHeaders, Map<String, SemossDataType> newColumnsToTypeMap, List<Join> joins, Map<String, String> newColumnAlias) {
		List<String> newColumnsToAdd = new Vector<String>();
		List<SemossDataType> newColumnsToAddTypes = new Vector<SemossDataType>();
		
		// get all the join columns
		List<String> joinColumns = new Vector<String>();
		for(Join j : joins) {
			String columnName = j.getQualifier();
			if(columnName.contains("__")) {
				columnName = columnName.split("__")[1];
			}
			joinColumns.add(columnName);
		}
		
		for(String newColumn : newColumnsToTypeMap.keySet()) {
			SemossDataType newColumnType = newColumnsToTypeMap.get(newColumn);
			// modify the header
			if(newColumn.contains("__")) {
				newColumn = newColumn.split("__")[1];
			}
			// if its a join column, ignore it
			if(joinColumns.contains(newColumn)) {
				continue;
			}
			// not a join column
			// check if it has an alias
			// and then add
			if(newColumnAlias.containsKey(newColumn)) {
				newColumnsToAdd.add(newColumnAlias.get(newColumn));
			} else {
				newColumnsToAdd.add(newColumn);
			}
			// and store the type at the same index 
			// in its list
			newColumnsToAddTypes.add(newColumnType);
		}
		
		//TODO: account for column types
		
		StringBuilder command = new StringBuilder(tableName).append(".reindex(columns=[");
		// add current headers
		for(int i = 0; i < curHeaders.length; i++) {
			command.append("\"").append(curHeaders[i]).append("\",");
		}
		
		// add the new headers
		int numNew = newColumnsToAdd.size();
		for(int i = 0; i < numNew; i++) {
			String newCol = newColumnsToAdd.get(i);
			SemossDataType newColType = newColumnsToAddTypes.get(i);
			
			command.append("\"").append(newCol).append("\"");
			if( (i+1) < numNew) {
				command.append(",");
			}
			
//			if(newColType == SemossDataType.DOUBLE) {
//				command.append(newColSyntax).append(" <- as.numeric(").append(newColSyntax).append(");");
//			} else if(newColType == SemossDataType.INT) {
//				command.append(newColSyntax).append(" <- as.integer(").append(newColSyntax).append(");");
//			} else {
//				command.append(newColSyntax).append(" <- as.character(").append(newColSyntax).append(");");
//			}
		}
		
		command.append("])");
		return command.toString();
	}
	
	// gets the number of rows in a given data frame
	public static String getDFLength(String tableName)
	{
		String dfLength = "len(" + tableName + ".index)";
		return dfLength;
	}
	
	// gets all the columns as an array
	public static String getColumns(String tableName)
	{
		String cols = "list(" + tableName + ")";
		return cols;
	}
	
	// get all types
	// this needs to be used in combination with get columns
	public static String getTypes(String tableName)
	{
		String types = tableName + ".dtypes.tolist()";
		return types;
	}
	
	// gets the record
	public static String getColumnChange(String tableName, String colName, String type)
	{
		String typeChanger = tableName + "['" + colName + "'] = " + tableName + "['" + colName + "'].astype('" + type + "')";
		return typeChanger;
	}
	
	/**
	 * Create a pandas vector from a java vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	public static String createPandasColVec(List<Object> row, SemossDataType dataType) {
		StringBuilder str = new StringBuilder("([");
		int i = 0;
		int size = row.size();
		for(; i < size; i++) {
			if(SemossDataType.STRING == dataType) {
				str.append("'").append(row.get(i)).append("'");
			} else if(SemossDataType.INT == dataType || SemossDataType.DOUBLE == dataType) {
				str.append(row.get(i).toString());
			} else if(SemossDataType.DATE == dataType) {
				str.append("as.Date(\"").append(row.get(i).toString()).append("\", format='%Y-%m-%d');");
			} else {
				// just in case this is not defined yet...
				// see the type of the value and add it in based on that
				if(dataType == null) {
					if(row.get(i) instanceof String) {
						str.append("'").append(row.get(i)).append("'");
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
		str.append("])");
		return str.toString();
	}

	public static String formatFilterValue(Object value, SemossDataType dataType) {
		if(SemossDataType.STRING == dataType) {
			return "'" + value + "'";
		} else if(SemossDataType.INT == dataType || SemossDataType.DOUBLE == dataType) {
			return value.toString();
		} else if(SemossDataType.DATE == dataType) {
			return "as.Date(\"" + value.toString() + "\", format='%Y-%m-%d')";
		} else {
			// just in case this is not defined yet...
			// see the type of the value and add it in based on that
			if(dataType == null) {
				if(value instanceof String) {
					return "'" + value + "'";
				} else {
					return value + "";
				}
			} else {
				return value + "";
			}
		}
	}

	public static String cleanFrameHeaders(String frameName, String[] colNames) {
		
		HeadersException headerChecker = HeadersException.getInstance();
		String [] pyColNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		StringBuilder colRen = new StringBuilder("{");
		for (int i = 0; i < colNames.length; i++) {
			colRen.append("'").append(colNames[i]).append("':'").append(pyColNames[i]).append("'");
			if (i < colNames.length - 1) {
				colRen.append(", ");
			}
		}
		colRen.append("}");
		String script = frameName + ".rename(columns=" + colRen + ", inplace=True)" ;
		return script;
	}
	

}
