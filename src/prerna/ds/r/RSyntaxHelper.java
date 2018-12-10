package prerna.ds.r;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class RSyntaxHelper {
	
	private static Map<String,String> javaRDatTimeTranslationMap = new HashMap<String,String>();
	
	static {
		javaRDatTimeTranslationMap.put("y2", "%y");		//yr without century (2 digits)
		javaRDatTimeTranslationMap.put("Y2", "%y");		//yr without century (2 digits)
		javaRDatTimeTranslationMap.put("y4", "%Y");		//full yr (4 digits)
		javaRDatTimeTranslationMap.put("Y4", "%Y");		//full yr (4 digits)
//		javaRDatTimeTranslationMap.put("G", value); 	//Era
		javaRDatTimeTranslationMap.put("M1", "%m"); 	//Numerical month
		javaRDatTimeTranslationMap.put("M2", "%m"); 	//Numerical month
		javaRDatTimeTranslationMap.put("M3", "%b"); 	//Abbreviated month name
		javaRDatTimeTranslationMap.put("M+", "%B"); 	//Full month name
		javaRDatTimeTranslationMap.put("d", "%d"); 		//Day in month
		javaRDatTimeTranslationMap.put("D", "%j"); 		//Day in year
//		javaRDatTimeTranslationMap.put("w", value); 	//Week in year
//		javaRDatTimeTranslationMap.put("W", value); 	//Week in month
//		javaRDatTimeTranslationMap.put("F", value); 	//Day of week in month
		javaRDatTimeTranslationMap.put("E3", "%a"); 	//Abbreviate day name in week
		javaRDatTimeTranslationMap.put("E", "%A");		//Full day name in week
		javaRDatTimeTranslationMap.put("u", "%u"); 		//Day number of week (1 = Monday, ..., 7 = Sunday)
		javaRDatTimeTranslationMap.put("a", "%p"); 		//AM/PM --- need to be used with %I not %H
		javaRDatTimeTranslationMap.put("H", "%H"); 		//Hour in day (0-23)
//		javaRDatTimeTranslationMap.put("k", value); 	//Hour in day (1-24)
//		javaRDatTimeTranslationMap.put("K", value); 	//Hour in am/pm (0-11)
		javaRDatTimeTranslationMap.put("h", "%I"); 		//Hour in am/pm (1-12)
		javaRDatTimeTranslationMap.put("m", "%M"); 		//Minute in hour
		javaRDatTimeTranslationMap.put("s", "%S"); 		//Second in minute
		javaRDatTimeTranslationMap.put("S", "%OS"); 	//Millisecond
//		javaRDatTimeTranslationMap.put("z", "%Z"); 		//tz (Pacific Standard Time; PST; GMT-08:00) ???? - TODO needs to be handled via the tz param
		javaRDatTimeTranslationMap.put("Z", "%z"); 		//tz (-0800)
		javaRDatTimeTranslationMap.put("X", "%z"); 		//tz (-08; -0800; -08:00)
	}
	
	private RSyntaxHelper() {
		
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
	
	/**
	 * Convert a list to a r column vector of strings
	 * @param values
	 * @return
	 */
	public static String createStringRColVec(List<String> values) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = values.size();
		for(; i < size; i++) {
			str.append("\"").append(values.get(i)).append("\"");
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
	 * Convert a java object[] into a r ordered factor col
	 * @param row					The object[] to convert
	 * @param orderedLevels			The ordering of the factor
	 * @return						String containing the equivalent r column vector
	 */
	public static String createOrderedRFactor(Object[] row, String orderedLevels) {
		StringBuilder str = new StringBuilder();
		String vectorParameter = createStringRColVec(row);
		Object[] orderedLevelsSplit = orderedLevels.split("+++");
		if (orderedLevelsSplit.length < 2) {
			throw new RuntimeException("Ordered levels of a factor must contain 2 or more items.");
		} else {
			String orderLevelsVector = createStringRColVec(orderedLevelsSplit);
			str.append("factor(" + vectorParameter + ", ordered = TRUE, levels = " + orderLevelsVector + ");");

		}
		return str.toString();
	}
	
	/**
	 * Convert a java object[] into a r ordered factor col
	 * @param row					The object[] to convert
	 * @param orderedLevels			The ordering of the factor
	 * @param orderedLevelLabels	Corresponding labels of the ordered levels of the factor
	 * @return						String containing the equivalent r column vector
	 */
	public static String createOrderedRFactor(Object[] row, String orderedLevels, String orderedLevelLabels) {
		StringBuilder str = new StringBuilder();
		String vectorParameter = createStringRColVec(row);
		Object[] orderedLevelsSplit = orderedLevels.split("+++");
		Object[] orderedLevelLabelsSplit = orderedLevelLabels.split("+++");
		if (orderedLevelsSplit.length != orderedLevelLabelsSplit.length) {
			throw new RuntimeException("Counts of ordered levels and the corresponding labels must be equal.");
		} else if (orderedLevelsSplit.length < 2) {
			throw new RuntimeException("Ordered levels/labels of a factor must contain 2 or more items.");
		} else {
			String orderLevelsVector = createStringRColVec(orderedLevelsSplit);
			str.append("factor(" + vectorParameter + ", ordered = TRUE, levels = " + orderLevelsVector + "), labels = " + orderedLevelLabelsSplit +");");
		}
		return str.toString();
	}
	

	public static String getOrderedLevelsFromRFactorCol(String tableName, String colName) {
		StringBuilder str = new StringBuilder();
		str.append("paste(levels("+ tableName + "$" + colName + "), collapse = '+++');");		
		return str.toString();
	}
	
	////////////////////////////////////////////////
	// Data frame changes
	/////////////////////////////////////////////////
	public static String cleanFrameHeaders(String frameName, String[] colNames) {
		HeadersException headerChecker = HeadersException.getInstance();
		colNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		for (int i = 0; i < colNames.length; i++) {
			rColNames += "\"" + colNames[i] + "\"";
			if (i < colNames.length - 1) {
				rColNames += ", ";
			}
		}
		String script = "colnames(" + frameName + ") <- c(" + rColNames + ")";
		return script;
	}
	
	public static String alterColumnName(String tableName, String oldHeader, String newHeader) {
		StringBuilder str = new StringBuilder();
		str.append("colnames("+tableName+")[which(names("+tableName+") == \""+oldHeader+"\")] <- \""+newHeader+"\";");
		return str.toString();
	}
	
	public static String alterColumnTypeToCharacter(String tableName, String colName) {
		// will generate a string similar to
		// "datatable$Revenue_International <- as.numeric(as.character(datatable$Revenue_International))"
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append("$").append(colName).append(" <- ").append("as.character(")
		.append(tableName).append("$").append(colName).append(")");
		return builder.toString();
	}
	
	public static String alterColumnTypeToCharacter(String tableName, List<String> colName) {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName + "[,(c('" + StringUtils.join(colName,"','") + "')) := lapply(.SD, as.character), .SDcols = c('" + StringUtils.join(colName,"','") + "')]");
		return builder.toString();
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
	
	public static String alterColumnTypeToNumeric(String tableName, List<String> colName) {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName + "[,(c('" + StringUtils.join(colName,"','") + "')) := lapply(.SD, as.numeric), .SDcols = c('" + StringUtils.join(colName,"','") + "')]");
		return builder.toString();
	}

	/**
	 * Converts a R column type to date
	 * @param tableName				The name of the R table
	 * @param colName				The name of the column
	 * @return						The r script to execute
	 */
	public static String alterColumnTypeToDate(String tableName, String format, String colName) {
		if(format == null) {
			format = "%Y-%m-%d";
		}
		
		String[] parsedFormat = format.split("\\|");
		// will generate a string similar to
		// "datatable$Birthday <- as.Date(as.character(datatable$Birthday), format = '%m/%d/%Y')"
		StringBuilder builder = new StringBuilder();
		builder.append(tableName + "$" + colName +  "<- as.Date(" 
				+ tableName + "$" + colName + ", format = '" + parsedFormat[0] +"')");
		return builder.toString();
	}
	
	public static String alterColumnTypeToDate(String tableName, String format, List<String> cols) {
		if(format == null) {
			format = "%Y-%m-%d";
		}
		
		String[] parsedFormat = format.split("\\|");
		StringBuilder builder = new StringBuilder();
		//parse out the milliseconds options
		builder.append(tableName + "[,(c('" + StringUtils.join(cols,"','") + "')) := "
				+ "lapply(.SD, function(x) as.Date(x, format='" + parsedFormat[0] + "')), "
				+ ".SDcols = c('" + StringUtils.join(cols,"','") + "')]");
		return builder.toString();
	}
	
	public static List<String> alterColumnTypeToDate_Excel(String tableName, List<String> cols) {
		List<String> dateExcelR= new ArrayList<String>();
		StringBuilder builder = new StringBuilder();
		String convertedDateCols_R = "convertedDateCols" + Utility.getRandomString(6);
		String convertedDateColsList_R = "convertedDateColsList" + Utility.getRandomString(6);
		
		//append the function that will handle dates that have been translated into a numerical value during read.xlsx2 upload
		builder.append("clean_convertToDate_function <- function(x){	cleanCol <- gsub('^\\\\s*$', 'NA', " + tableName + "[[x]]) "
				+ "%>% .[!is.na(.) & . != 'NA' & . != 'null' & . != 'NULL'];" 
				+ "if (all(!is.na(as.numeric(cleanCol)))) as.Date('1900-01-01') + as.numeric(" + tableName + "[[x]]) - 2 else 'NOTHING' };");
		//call the function above and sort columns by whether they were handled by the function
		builder.append(convertedDateCols_R + " <- lapply(c('" + StringUtils.join(cols, "','") + "'), clean_convertToDate_function);");
		builder.append(convertedDateColsList_R + " <- c('" + StringUtils.join(cols,"','") + "')[-grep('NOTHING'," + convertedDateCols_R + ")];");
		//update columns via the outputs of the function
		builder.append("if (length(" + convertedDateColsList_R + ") > 0) "
				+ tableName + "[, (" + convertedDateColsList_R + ") := lapply("
				+ "setdiff(seq(1,length(" + convertedDateCols_R + ")), grep('NOTHING'," + convertedDateCols_R + ")), "
				+ "function(x) unlist(" + convertedDateCols_R + "[[x]] ))];");
		
		//clean up variables/functions
		builder.append("rm(clean_convertToDate_function," + convertedDateCols_R + "); gc()");
		
		//prep return list
		dateExcelR.add(builder.toString());
		dateExcelR.add(convertedDateColsList_R);
		
		return dateExcelR;
	}

	public static String alterColumnTypeToDateTime(String tableName, String format,String colName) {
		if(format == null) {
			format = "%Y-%m-%d %H:%M:%S|NULL";
		}
		String[] parsedFormat = format.split("\\|");
		StringBuilder builder = new StringBuilder();
		builder.append("options(digits.secs=" + parsedFormat[1] + ");");
		builder.append(tableName + "$" + colName + "<- as.POSIXct(fast_strptime(" 
				+ tableName + "$" + colName + ", format = '" + parsedFormat[0] +"'))");
		return builder.toString();
	}
	
	public static String alterColumnTypeToDateTime(String tableName, String format, List<String> cols) {
		if(format == null) {
			format = "%Y-%m-%d %H:%M:%S|NULL";
		}
		//parse out the milliseconds options
		String[] parsedFormat = format.split("\\|");
		StringBuilder builder = new StringBuilder();
		builder.append("options(digits.secs=" + parsedFormat[1] + ");");
		builder.append(tableName + "[,(c('" + StringUtils.join(cols,"','") + "')) := "
				+ "lapply(.SD, function(x) as.POSIXct(fast_strptime(x, format='" + parsedFormat[0] + "'))), "
				+ ".SDcols = c('" + StringUtils.join(cols,"','") + "')]");
		return builder.toString();
	}
	
	public static List<String> alterColumnTypeToDateTime_Excel(String tableName, List<String> cols) {
		List<String> dateTimeExcelR= new ArrayList<String>();
		StringBuilder builder = new StringBuilder();
		String convertedDTCols_R = "convertedDTCols" + Utility.getRandomString(6);
		String convertedDTColsList_R = "convertedDTColsList" + Utility.getRandomString(6);
		
		//TODO need to make this based on an input parameter instead of being hardcoded
		builder.append("options(digits.secs = 3);");
		
		//append the function that will handle datetimes that have been translated into a numerical value during read.xlsx2 upload
		builder.append("clean_convertToPOSIXct_function <- function(x){ cleanCol <- gsub('^\\\\s*$', 'NA', " + tableName + "[[x]]) "
				+ "%>% .[!is.na(.) & . != 'NA' & . != 'null' & . != 'NULL'];"
				+ "if (all(!is.na(as.numeric(cleanCol)))) as.POSIXct(as.numeric(" + tableName + "[[x]])*86400, origin = '1899-12-30', tz='UTC') else 'NOTHING' };");
		//call the function above and sort columns by whether they were handled by the function
		builder.append(convertedDTCols_R + " <- lapply(c('" + StringUtils.join(cols, "','") + "'), clean_convertToPOSIXct_function);");
		builder.append(convertedDTColsList_R + " <- c('" + StringUtils.join(cols,"','") + "')[-grep('NOTHING'," + convertedDTCols_R + ")];");
		//update columns via the outputs of the function
		builder.append("if (length(" + convertedDTColsList_R + ") > 0) "
				+ tableName + "[, (" + convertedDTColsList_R + ") := lapply("
				+ "setdiff(seq(1,length(" + convertedDTCols_R + ")), grep('NOTHING'," + convertedDTCols_R + ")), "
				+ "function(x) unlist(" + convertedDTCols_R + "[[x]] ))];");
		
		//clean up variables/functions
		builder.append("rm(clean_convertToDate_function," + convertedDTCols_R + "); gc()");
		
		//prep return list
		dateTimeExcelR.add(builder.toString());
		dateTimeExcelR.add(convertedDTColsList_R);
		
		return dateTimeExcelR;
	}
	
	/**
	 * Get specific columns from a frame
	 * @param resultingFrame
	 * @param dataframe
	 * @param cols
	 * @return
	 */
	public static String getFrameSubset(String resultingFrame, String dataframe, Object[] cols) {
		StringBuilder rsb = new StringBuilder();
		String rColsVec = RSyntaxHelper.createStringRColVec(cols);
		rsb.append(resultingFrame + "<- subset(" + dataframe + ", select=" + rColsVec + ");");
		return rsb.toString();
	}
	
	/**
	 * Return new frame object
	 * @param oldFrame
	 * @param newFrame
	 * @return
	 */
	public static String asDataTable(String newFrame, String oldFrame) {	
		StringBuilder rsb = new StringBuilder();
		rsb.append(newFrame + "<- as.data.table(" + oldFrame + ");");
		return rsb.toString();
	}
	
	/**
	 * Return new frame object
	 * @param oldFrame
	 * @param newFrame
	 * @return
	 */
	public static String asDataFrame(String newFrame, String oldFrame) {	
		StringBuilder rsb = new StringBuilder();
		rsb.append(newFrame + "<- as.data.frame(" + oldFrame + ");");
		return rsb.toString();
	}
	
	/**
	 * Creates a numeric column by removing numbers from a column
	 * @param dataframe
	 * @param column
	 * @return
	 */
	public static String extractNumbers(String dataframe, String column) {
		StringBuilder rsb = new StringBuilder();
		rsb.append(dataframe + "$" + column + " <- as.numeric(gsub('[^-\\\\.0-9]', '', " + dataframe + "$" + column + "));");
		return rsb.toString();
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
		builder.append(returnTable).append(" <- merge(").append(leftTableName).append(", ")
				.append(rightTableName).append(", by.x = c(");
		getMergeColsSyntax(builder, joinCols);
		builder.append("), by.y = c(");
		getMergeColsSyntax(builder, joinCols);
		
		if (joinType.equals("inner.join")) {
			builder.append("), all = FALSE, allow.cartesian = TRUE)");
		} else if (joinType.equals("left.outer.join")) {
			builder.append("), all.x = TRUE, all.y = FALSE, allow.cartesian = TRUE)");
		} else if (joinType.equals("right.outer.join")) {
			builder.append("), all.x = FALSE, all.y = TRUE, allow.cartesian = TRUE)");
		} else if (joinType.equals("outer.join")) {
			builder.append("), all = TRUE, allow.cartesian = TRUE)");
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
	public static String alterMissingColumns(String tableName,  Map<String, SemossDataType> newColumnsToTypeMap,  List<Join> joins, Map<String, String> newColumnAlias) {
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
		
		StringBuilder rExec = new StringBuilder();
		for(int i = 0; i < newColumnsToAdd.size(); i++) {
			String newCol = newColumnsToAdd.get(i);
			SemossDataType newColType = newColumnsToAddTypes.get(i);
			
			String newColSyntax = tableName + "$" + newCol;
			
			if(newColType == SemossDataType.DOUBLE) {
				rExec.append(newColSyntax).append(" <- as.numeric(").append(newColSyntax).append(");");
			} else if(newColType == SemossDataType.INT) {
				rExec.append(newColSyntax).append(" <- as.integer(").append(newColSyntax).append(");");
			} else {
				rExec.append(newColSyntax).append(" <- as.character(").append(newColSyntax).append(");");
			}
		}
		
		return rExec.toString();
	}

	//////////////////////////////////////////
	// File I/O
	/////////////////////////////////////////
	/**
	 * Generate the syntax to perform a fRead to ingest a file
	 * @param tableName
	 * @param absolutePath
	 * @return
	 */
	public static String getFReadSyntax(String tableName, String absolutePath) {
		return getFReadSyntax(tableName, absolutePath, ",");
	}
	
	/**
	 * Generate the syntax to perform a fRead to ingest a file
	 * @param tableName
	 * @param absolutePath
	 * @param delimiter
	 * @return
	 */
	public static String getFReadSyntax(String tableName, String absolutePath, String delimiter) {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append(" <- fread(\"").append(absolutePath.replace("\\", "/"))
			.append("\", sep=\"").append(delimiter).append("\");");
		return builder.toString();
	}
	
	/**
	 * Write dataframe to csv
	 * @param dataframe
	 * @param absolutePath
	 * @return
	 */
	public static String getFWriteSyntax(String dataframe, String absolutePath) {
		StringBuilder rsb = new StringBuilder();
		rsb.append("fwrite(" + dataframe + ",file=\"" + absolutePath.replace("\\", "/") + "\");");
		return rsb.toString();
	}

	public static String getExcelReadSheetSyntax(String tableName, String absolutePath, int sheetIndex, List<Integer> colIndices, boolean uploadSubset) {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName).append(" <- as.data.table(read.xlsx2(\"").append(absolutePath.replace("\\", "/"))
		.append("\", sheetIndex=").append(sheetIndex).append(", colClasses = c(rep('character',").append(colIndices.size())
		.append(")), stringsAsFactors=FALSE");
		if (uploadSubset) {
			builder.append(", colIndex = c(").append(StringUtils.join(colIndices,",")).append(")");
		}
		builder.append("))");
		return builder.toString();
	}
	
	/**
	 * Load excel sheet to dataframe
	 * @param filePath The file path of the excel file
	 * @param frameName The name of the frame to load excel sheet
	 * @param sheetName The name of the sheet in the excel workbook
	 * @param sheetRange The desired range to load
	 * @return
	 */
	public static String loadExcelSheet(String filePath, String frameName, String sheetName, String sheetRange) {
		StringBuilder rsb = new StringBuilder();
		rsb.append("library(readxl);library(cellranger);");
		filePath = filePath.replace("\\", "/");
		rsb.append(frameName + " <- read_excel(path = \"" + filePath + "\", col_names = TRUE, sheet = \"" + sheetName + "\", range='"
				+ sheetRange + "');");
		rsb.append(frameName + " <- as.data.table(" + frameName + ");");
		return rsb.toString();
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
	
	public static String escapeRegexR(String expr){
		if (Pattern.matches("^(grepl).*?", expr)){
			String [] parsedRegex = (String[]) expr.split("\\\\\"");
			String regex = "";
			for (int k=1; k < parsedRegex.length - 1; k++){
				String escapedParsedRegex = parsedRegex[k].replace("\\", "\\\\");
				regex = regex + escapedParsedRegex;
			}
			expr = parsedRegex[0] + "\\\"" + regex + "\\\"" + parsedRegex[parsedRegex.length - 1];
		}
		return expr;
	}
	
	/////////////////////////////////////////////////////////
	// Date formatting
	/////////////////////////////////////////////////////////
	
	/**
	 * Converts Java datetime format to R datetime format
	 * @param javaFormat
	 * @return rFormat
	 */
	public static String translateJavaRDateTimeFormat(String javaFormat){
		String rFormat = "";
		String substr = "";
		String optionsMiliSeconds = "";
		
		String datetimeRegex = "[yYMDdEuaHhmsSZX]";
		int lastIndxSubstr = 0;
		for (int i = 0; i < javaFormat.length(); i++){
			String ch = (i == javaFormat.length()-1) ? javaFormat.substring(i) : javaFormat.substring(i, i + 1);
			if (!ch.matches(datetimeRegex) && !ch.equals("'")){
				//handle delimiters/whitespaces
				rFormat += ch;
			} else {
				lastIndxSubstr = javaFormat.lastIndexOf(ch);
				substr = javaFormat.substring(i, lastIndxSubstr + 1);
				
				if (ch.equals("'")) {
					//if character is a single quote (SQ), then need to properly parse to the nearest closing single quote
					if (substr.equals("''")) {
						rFormat += substr.replaceAll("''","'");
						i = lastIndxSubstr;
						continue;
					}
					String substr_trimmed = substr.substring(1, substr.length()-1).replaceAll("''","");
					int nextSQIndx = substr_trimmed.indexOf("'");
					int multiSQIndx = substr.substring(1, substr.length()-1).indexOf("''");
					if (nextSQIndx > 0) {
						if (multiSQIndx > 0 && multiSQIndx < nextSQIndx) {
							rFormat += substr.substring(1, nextSQIndx + 3).replaceAll("''","'");
							i = substr.substring(1, nextSQIndx + 3).length() + i + 1;
							continue;
						} else {
							rFormat += substr.substring(1, nextSQIndx + 1);
							i = substr.substring(1, nextSQIndx + 1).length() + i + 1;
							continue;
						}
					} else {
						rFormat += substr.substring(1, substr.length()-1).replaceAll("''","'");
					}
				} else {
					//check if there are any other characters in the substr other than the character (ch)
					for (int j = 1; j < substr.length(); j++) {
						char ch_substr = substr.toCharArray()[j];
						if (!String.valueOf(ch_substr).equals(ch)) {
							substr = substr.substring(0, j);
							lastIndxSubstr = i + j - 1;
							break;
						}
					}
					if (ch.equalsIgnoreCase("y") || ch.equals("M") || ch.equals("E")) {
						// for these ch, it needs to be concatenated with the length of the substr to identify the appropriate R syntax
						int lengthSubstr = substr.length();
						if (ch.equalsIgnoreCase("e") && lengthSubstr != 3) {
							rFormat += javaRDatTimeTranslationMap.get("E");
						} else if (ch.equals("M") && lengthSubstr > 3) {
							rFormat += javaRDatTimeTranslationMap.get("M+");
						} else if (javaRDatTimeTranslationMap.get(ch + lengthSubstr) != null) {
							rFormat += javaRDatTimeTranslationMap.get(ch + lengthSubstr);
						} else {
							throw new RuntimeException("Associated R date/time format is undefined.");
						}
					} else if (ch.equals("S")) {
						// need to retain how many digits the millisecond request is
						optionsMiliSeconds = Integer.toString(substr.length());
						// for second, need to check for millisecond to properly translate to R syntax
						if (rFormat.indexOf("%S") > 1) {
							rFormat = rFormat.replaceAll("%S", "%OS");
							rFormat = rFormat.substring(0, rFormat.indexOf("%OS") + 3);
						} else {
							throw new RuntimeException(
									"R timestamps cannot support milliseconds without the presence of seconds.");
						}
					} else if (javaRDatTimeTranslationMap.get(ch) != null) {
						// for these ch, it can be used to directly search for the appropriate R syntax
						rFormat += javaRDatTimeTranslationMap.get(ch);
					} else {
						throw new RuntimeException("Associated R date/time format is undefined.");
					}
				}
				i = lastIndxSubstr;
			}
		}
		
		//persist the millisecond decimal place option alongside the R date format
		if (optionsMiliSeconds != "") {
			rFormat += "|" + optionsMiliSeconds ;
		} else {
			rFormat += "|NULL" ;
		}		
		
		return rFormat;
	}
	
	public static String getValueJavaRDatTimeTranslationMap(String key){
		String value = javaRDatTimeTranslationMap.get(key);
		return value;
	}
	
	/////////////////////////////////////////////////////////
	// R Environment
	/////////////////////////////////////////////////////////
	/**
	 * Generate R syntax to load r packages
	 * 
	 * @param packages
	 * @return
	 */
	public static String loadPackages(String[] packages) {
		StringBuilder sb = new StringBuilder();
		for (String lib : packages) {
			sb.append(RSyntaxHelper.loadLibrary(lib));
		}
		return sb.toString();
	}

	/**
	 * Generate R synatx to load a single library
	 * 
	 * @param library
	 * @return
	 */
	public static String loadLibrary(String library) {
		StringBuilder rsb = new StringBuilder();
		rsb.append("library(" + library + ");");
		return rsb.toString();
	}
	
	public static void main(String[] args) {
//		// testing inner
//		System.out.println("testing inner...");
//		System.out.println("testing inner...");
//		System.out.println("testing inner...");
//		System.out.println("testing inner...");
//		System.out.println("testing inner...");
//
//		String returnTable = "tableToTest";
//		String leftTableName = "x";
//		String rightTableName = "y";
//		String joinType = "inner.join"; // left.outer.join, right.outer.join, outer.join
//		List<Map<String, String>> joinCols = new Vector<Map<String, String>>();
//		Map<String, String> join1 = new HashMap<String, String>();
//		join1.put("", "");
//		joinCols.add(join1);
//
//		// when you get to multi
//		//		Map<String, String> join2 = new HashMap<String, String>();
//		//		join2.put("", "");
//		//		joinCols.add(join2);
//
//		System.out.println(getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols));
//
//		System.out.println("testing left...");
//		System.out.println("testing left...");
//		System.out.println("testing left...");
//		System.out.println("testing left...");
//		System.out.println("testing left...");
//
//		returnTable = "tableToTest";
//		joinType = "left.outer.join"; // left.outer.join, right.outer.join, outer.join
//
//		System.out.println(getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols));
		
		
		
		
	}
}
