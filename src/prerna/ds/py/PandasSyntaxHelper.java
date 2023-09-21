package prerna.ds.py;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.HeadersException;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.sablecc2.om.Join;

public class PandasSyntaxHelper {

	public static final List<String> OPERATOR_LIST = new ArrayList<String>(6);
	static {
		OPERATOR_LIST.add(">");
		OPERATOR_LIST.add("<");
		OPERATOR_LIST.add(">=");
		OPERATOR_LIST.add("<=");
		OPERATOR_LIST.add("==");
		OPERATOR_LIST.add("!=");
	}
	
	private static final String NAN_VALUES = "[\"\",\"#N/A\",\"#N/A N/A\",\"#NA\",\"-1.#IND\",\"<NA>\",\"N/A\",\"NULL\"" 
			+ "\"n/a\",\"null\",\"-1.#QNAN\",\"-NaN\",\"-nan\",\"1.#IND\",\"1.#Q.#NAN\",\"NA\",\"NaN\",\"nan\"]";
	
	private static final String EQUAL = "==";
	private static final String NOT_EQUAL = "!=";

	private PandasSyntaxHelper() {

	}

	/**
	 * Get the syntax to construct get the wrapper for a pandas frame
	 * 
	 * @param wrapper
	 * @param tableName
	 * @return
	 */
	public static String makeWrapper(String wrapper, String tableName) {
		return wrapper + " = PyFrame.makefm(" + tableName + ")";
	}
	
	/**
	 * Get the name of the wrapper variable to be used
	 * 
	 * @param frameName
	 * @return
	 */
	public static String createFrameWrapperName(String frameName) {
		return frameName + "w";
	}

	/**
	 * Execute a .py file
	 * 
	 * @param fileLocation
	 * @return
	 */
	public static String execFile(String fileLocation) {
		return "execfile('" + fileLocation.replaceAll("\\\\+", "/") + "')";
	}

	/**
	 * Get the syntax to load a csv file Defaults the encoding to utf-8
	 * 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @return
	 */
	public static String getCsvFileRead(String pandasImportVar, String numpyImportVar, String fileLocation, String tableName) {
		return getCsvFileRead(pandasImportVar, numpyImportVar, fileLocation, tableName, null, null, null);
	}

	/**
	 * Get the syntax to load a csv file
	 * 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param sep
	 * @param quoteChar
	 * @param escapeChar
	 * @return
	 */
	public static String getCsvFileRead(String pandasImportVar, String numpyImportVar, String fileLocation, 
			String tableName, String sep, String quoteChar, String escapeChar) {
		if (sep == null || sep.isEmpty()) {
			sep = ",";
		}
		// default to double quote
		if(quoteChar == null) {
			quoteChar = "\"";
		}
		// default to \\ in python
		if(escapeChar == null) {
			escapeChar = "\\\\";
		}
		return getCsvFileRead(pandasImportVar, numpyImportVar, fileLocation, tableName, sep, quoteChar, escapeChar, null);
	}

	/**
	 * Get the syntax to load a CSV file that preserves leading zeros. 
	 * 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param sep
	 * @param quoteChar
	 * @param escapeChar
	 * @param encoding
	 * @return
	 */
	public static String getCsvFileRead(String pandasImportVar, String numpyImportVar, String fileLocation, 
			String tableName, String sep, String quoteChar, String escapeChar, String encoding) {
		if (encoding == null || encoding.isEmpty()) {
			encoding = "utf-8";
		}
		
		StringBuffer readCsv = buildReadCsv(pandasImportVar, numpyImportVar, fileLocation, tableName, sep, quoteChar, escapeChar, encoding);
		return readCsv.toString();
	}
	
	/**
	 * Builds the script to read in a CSV file to a pandas data frame. Code preserves leading zeros of integers. Returned data frame can have mixed 
	 * columns of floats and ints, but if there is a string present, the column type is returned as an object. 
	 * 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param sep
	 * @param quoteChar
	 * @param escapeChar
	 * @param encoding
	 * @return
	 */
	public static StringBuffer buildReadCsv(String pandasImportVar, String numpyImportVar, String fileLocation, 
			String tableName, String sep, String quoteChar, String escapeChar, String encoding) {
		StringBuffer script = new StringBuffer();
		StringBuffer replace = new StringBuffer(".replace(");
		StringBuffer converter = new StringBuffer("converters={i: lambda x: x.strip() if (isinstance(");
		StringBuffer numeric = new StringBuffer("");
		
		numeric.append(pandasImportVar).append(".to_numeric(x,errors='ignore')");
		replace.append(NAN_VALUES).append(",").append(numpyImportVar).append(".nan)");
		converter.append(numeric).append(",").append(numpyImportVar).append(".integer) and x.strip()[0] == '0' and len(x.strip()) > 1) else ")
				 .append(numeric).append(" for i in range(" + numberOfColumns(fileLocation) + ")}");
		
		
		script.append(tableName).append(" =").append(pandasImportVar).append(".read_csv('").append(fileLocation.replaceAll("\\\\+", "/"))
			  .append("',sep='" + sep + "',encoding='" + encoding + "',")
			  .append(converter + ")").append(replace)
			  .append(".apply(lambda x: x.astype(str) if (any(x.map(type) == str)) else x)").append(replace);
		
		return script;
	}

	/**
	 * Returns the number of columns in a csv file.
	 * 
	 * @param filePath
	 * @return
	 */
	public static Number numberOfColumns(String filePath) {
		CSVFileHelper csv = new CSVFileHelper();
		csv.parse(filePath);
		return csv.getHeaders().length;
	}
	
	/**
	 * Reads a CSV using pandas. less greedy / dynamic version. 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param sep
	 * @param encoding
	 * @param dataTypeMaps
	 * @return
	 */
	public static String getCsvFileRead(String pandasImportVar, String numpyImportVar, String fileLocation, String tableName, String sep,
			String quoteChar, String escapeChar, String encoding, Map<String, ?> dataTypeMaps) {
		StringBuffer script = new StringBuffer();
		if (encoding == null || encoding.isEmpty()) {
			encoding = "utf-8";
		}
		if (dataTypeMaps == null || dataTypeMaps.isEmpty()) {
			script.append(tableName).append("=").append(pandasImportVar).append(".read_csv('").append(fileLocation.replaceAll("\\\\+", "/"))
			  .append("',sep='"+sep+"',quotechar='"+quoteChar+"',escapechar='"+escapeChar+"',encoding='"+encoding+"')");
		} else {
			script = buildReadCsv(pandasImportVar, numpyImportVar, fileLocation, tableName, sep, quoteChar, escapeChar, encoding, dataTypeMaps);
		}
		return script.toString();
	}
	
	/**
	 * Builds the pandas CSV read method. Less greedy approach that accounts for date parsing. 
	 * https://pandas.pydata.org/pandas-docs/version/0.15.1/generated/pandas.read_csv.html
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param sep
	 * @param quoteChar		
	 * @param escapeChar
	 * @param encoding
	 * @param dataTypeMaps
	 * @return
	 */
	public static StringBuffer buildReadCsv(String pandasImportVar, String numpyImportVar, String fileLocation, 
			String tableName, String sep, String quoteChar, String escapeChar, String encoding, Map<String, ?> dataTypeMaps) {
		StringBuffer sb = new StringBuffer();
		StringBuffer dataMap = new StringBuffer();
		StringBuffer dateList = new StringBuffer();

		for (String column : dataTypeMaps.keySet()) {
			Object inputType = dataTypeMaps.get(column);
			SemossDataType dataType = null;
			if(inputType instanceof SemossDataType) {
				dataType = (SemossDataType) inputType;
			} else {
				dataType = SemossDataType.convertStringToDataType(inputType + "");
			}
			
			if (dataType == null) {
				continue;
			}
			if (dataType == SemossDataType.DATE || dataType == SemossDataType.TIMESTAMP) {
				if (dateList.length() > 0) {
					dateList.append(",");
				}
				dateList.append("'").append(column).append("'");
			} else {
				if (dataMap.length() > 0) {
					dataMap.append(",");
				}
				dataMap.append("'").append(column).append("':").append(convertSemossDataType(numpyImportVar, dataType));
			}
		}
		dataMap = new StringBuffer("{").append(dataMap).append("}");
		dateList = new StringBuffer("[").append(dateList).append("]");
		
		sb.append(tableName).append("=").append(pandasImportVar).append(".read_csv('").append(fileLocation.replaceAll("\\\\+", "/"))
		  .append("',sep='"+sep+"',quotechar='"+quoteChar+"',escapechar='"+escapeChar+"',encoding='"+encoding+
				  "',dtype="+ dataMap.toString()).append(",parse_dates=").append(dateList)
		  .append(", infer_datetime_format=True)");
		
		return sb;
	}
	
	/**
	 * Reads a JSON using pandas. less greedy / dynamic version. 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param dataTypeMaps
	 * @return
	 */
	public static String getJsonFileRead(String pandasImportVar, String numpyImportVar, String fileLocation, String tableName, Map<String, ?> dataTypeMaps) {
		StringBuffer script = new StringBuffer();
		if (dataTypeMaps == null || dataTypeMaps.isEmpty()) {
			script.append(tableName).append("=").append(pandasImportVar).append(".read_json('").append(fileLocation.replaceAll("\\\\+", "/"));
		} else {
			script = buildReadJson(pandasImportVar, numpyImportVar, fileLocation, tableName, dataTypeMaps);
		}
		return script.toString();
	}
	
	/**
	 * Builds the pandas JSON read method. Less greedy approach that accounts for date parsing. 
	 * https://pandas.pydata.org/pandas-docs/version/0.15.1/generated/pandas.read_csv.html
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param sep
	 * @param quoteChar		
	 * @param escapeChar
	 * @param encoding
	 * @param dataTypeMaps
	 * @return
	 */
	public static StringBuffer buildReadJson(String pandasImportVar, String numpyImportVar, String fileLocation, String tableName, Map<String, ?> dataTypeMaps) {
		StringBuffer sb = new StringBuffer();
		StringBuffer dataMap = new StringBuffer();
		StringBuffer dateList = new StringBuffer();

		for (String column : dataTypeMaps.keySet()) {
			Object inputType = dataTypeMaps.get(column);
			SemossDataType dataType = null;
			if(inputType instanceof SemossDataType) {
				dataType = (SemossDataType) inputType;
			} else {
				dataType = SemossDataType.convertStringToDataType(inputType + "");
			}
			
			if (dataType == null) {
				continue;
			}
			if (dataType == SemossDataType.DATE || dataType == SemossDataType.TIMESTAMP) {
				if (dateList.length() > 0) {
					dateList.append(",");
				}
				dateList.append("'").append(column).append("'");
			} else {
				if (dataMap.length() > 0) {
					dataMap.append(",");
				}
				dataMap.append("'").append(column).append("':").append(convertSemossDataType(numpyImportVar, dataType));
			}
		}
		dataMap = new StringBuffer("{").append(dataMap).append("}");
		dateList = new StringBuffer("[").append(dateList).append("]");
		
		sb.append(tableName).append("=").append(pandasImportVar).append(".read_json('").append(fileLocation.replaceAll("\\\\+", "/"))
		  .append("',dtype="+ dataMap.toString()).append(",convert_dates=").append(dateList).append(")");
		
		return sb;
	}
	
	/**
	 * Maps Semoss data types to Pandas / Numpy data types. 
	 * @param numpyImportVar
	 * @param type
	 * @return
	 */
	public static String convertSemossDataType(String numpyImportVar, Object inputType) {
		// account for string or enum type
		SemossDataType type = null;
		if(inputType instanceof SemossDataType) {
			type = (SemossDataType) inputType;
		} else {
			type = SemossDataType.convertStringToDataType(inputType + "");
		}
		
		if (type == SemossDataType.INT) {
			return numpyImportVar + ".float64";
		} else if (type == SemossDataType.DOUBLE) {
			return numpyImportVar + ".float64";
		} else if(type == SemossDataType.BOOLEAN) {
			return "object";
//			return numpyImportVar + ".bool";
		} if (type == SemossDataType.DATE) {
			return "object";
			//return numpyImportVar + ".datetime64";
		} else if (type == SemossDataType.TIMESTAMP) {
			//return numpyImportVar + ".datetime64";
			return "object";
		} else {
			return "object";
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param fileLocation
	 * @return
	 */
	public static String getWriteCsvFile(String tableName, String fileLocation) {
		return getWriteCsvFile(tableName, fileLocation, null);
	}

	/**
	 * 
	 * @param tableName
	 * @param fileLocation
	 * @param sep
	 * @return
	 */
	public static String getWriteCsvFile(String tableName, String fileLocation, String sep) {
		if (sep == null || sep.isEmpty()) {
			sep = ",";
		}
		return getWriteCsvFile(tableName, fileLocation, sep, null);
	}

	/**
	 * 
	 * @param tableName
	 * @param fileLocation
	 * @param sep
	 * @param encoding
	 * @return
	 */
	public static String getWriteCsvFile(String tableName, String fileLocation, String sep, String encoding) {
		if (encoding == null || encoding.isEmpty()) {
			encoding = "utf-8";
		}
		String readCsv = tableName + ".to_csv('" + fileLocation.replaceAll("\\\\+", "/") + "', sep='" + sep
				+ "', encoding='" + encoding + "', index=False)";
		return readCsv;
	}

	public static String loadExcelSheet(String pandasImportVar, String fileLocation, String tableName, String sheetName,
			String sheetRange) {
		int[] rangeIndicies = ExcelRange.getSheetRangeIndex(sheetRange);
		int startCol = rangeIndicies[0];
		int startRow = rangeIndicies[1];
		int endCol = rangeIndicies[2];
		int endRow = rangeIndicies[3];
		fileLocation = fileLocation.replace("\\", "/");
		StringBuffer sb = new StringBuffer();
		sb.append(tableName + " = " + pandasImportVar + ".read_excel('" + fileLocation + "',");
		// add column range
		sb.append("sheet_name='" + sheetName + "', usecols=range(" + (startCol - 1) + ", " + endCol + "), ");
		// add row range
		int rowNum = endRow - startRow;
		sb.append("skiprows = " + (startRow - 1) + ", nrows=" + rowNum + ")");
		return sb.toString();
	}

	public static String getWritePandasToPickle(String pickleVarName, String tableName, String fileLocation) {
		return pickleVarName + ".dump(" + tableName + ", open(\"" + fileLocation.replaceAll("\\\\+", "/") + "\", \"wb\"))";
	}

	public static String getReadPickleToPandas(String pandasImportVar, String fileLocation, String tableName) {
		return tableName + " = " + pandasImportVar + ".read_pickle(\"" + fileLocation.replaceAll("\\\\+", "/") + "\")";
		//		return tableName + " = " + pickleVarName + ".load(open(\"" + fileLocation.replaceAll("\\\\+", "/") + "\", \"wb\"))";
	}
	
 	/**
	 * Reads a Parquet File using pandas. less greedy / dynamic version. 
	 * @param pandasImportVar
	 * @param numpyImportVar
	 * @param fileLocation
	 * @param tableName
	 * @param dataTypeMaps
	 * @return
	 */
	public static String getParquetFileRead(String pandasImportVar, String numpyImportVar, String fileLocation, String tableName) {
		StringBuffer script = new StringBuffer();
		script.append(tableName).append("=").append(pandasImportVar).append(".read_parquet('").append(fileLocation.replaceAll("\\\\+", "/"))
		  .append("')");
		return script.toString();
	}

	/** Merges two dataframes. if nonEqui is true, implicitly performs a join by taking the cross product. 
	 * 
	 * @param leftTableName
	 * @param rightTableName
	 * @param joinType
	 * @param joinCols
	 * @return
	 */
	public static String getMergeSyntax(String pandasFrameVar, String returnTable, String leftTableName,
			String rightTableName, String joinType, List<Map<String, String>> joinCols,boolean nonEqui) {
		/*
		 * joinCols = [ {leftTable.Title -> rightTable.Movie} , {leftTable.Genre ->,
		 * rightTable.Genre} ]
		 */

		StringBuffer builder = new StringBuffer();
		
		if (!nonEqui) {
			builder.append(returnTable).append(" = ").append(pandasFrameVar).append(".merge(").append(leftTableName)
			.append(", ").append(rightTableName).append(", left_on=[");
			getMergeColsSyntax(builder, joinCols, true);
			builder.append("], right_on=[");
			getMergeColsSyntax(builder, joinCols, false);

			if (joinType.equals("inner.join")) {
				builder.append("], how=\"inner\")");
			} else if (joinType.equals("left.outer.join")) {
				builder.append("], how=\"left\")");
			} else if (joinType.equals("right.outer.join")) {
				builder.append("], how=\"right\")");
			} else if (joinType.equals("outer.join")) {
				builder.append("], how=\"outer\")");
			}
		} else {
			builder.append(returnTable).append("=").append(pandasFrameVar).append(".merge(").append(leftTableName).append(".assign(key=0),")
				   .append(rightTableName).append(".assign(key=0),on='key').drop('key',axis=1)");
		}
		return builder.toString();
	}
	
	public static String getMergeFilterSyntax(String tableName, List<Map<String, String>> joinCols, List<String> joinComparators) {
		StringBuffer builder = new StringBuffer();
		StringBuffer dropSyntax = new StringBuffer();
		
		for (int i = 0; i < joinCols.size(); i++) {
			if (builder.length() > 0) {
				builder.append(" & ");
			}
			if (dropSyntax.length() > 0) {
				dropSyntax.append(", ");
			}
			Map<String, String> joinMap = joinCols.get(i);
			for (String lColumn : joinMap.keySet()) {
				String rColumn = joinMap.get(lColumn);
				builder.append("(").append(tableName).append("['").append(lColumn).append("']")
					   .append(joinComparators.get(i)).append(tableName).append("['").append(rColumn).append("'])");
				if (rColumn.equals(lColumn + "_CTD")) {
					dropSyntax.append("'" + rColumn + "'");
				}
			}
		}
		builder = new StringBuffer(tableName).append("=").append(tableName).append(".loc[(").append(builder).append(")]");
		if (dropSyntax.length() > 0) {
			builder.append(".drop([").append(dropSyntax).append("],axis=1)");
		}
		return builder.toString();
	}

	/**
	 * Get the correct py syntax for the table join columns This uses the join
	 * information keys since it is {leftCol -> rightCol}
	 * 
	 * @param builder
	 * @param colNames
	 */
	public static void getMergeColsSyntax(StringBuffer builder, List<Map<String, String>> colNames, boolean grabKeys) {
		// iterate through the map
		boolean firstLoop = true;
		int numJoins = colNames.size();
		for (int i = 0; i < numJoins; i++) {
			Map<String, String> joinMap = colNames.get(i);
			// just in case an empty map is passed
			// we do not want to modify the firstLoop boolean
			if (joinMap.isEmpty()) {
				continue;
			}
			if (!firstLoop) {
				builder.append(",");
			}
			// this should really be 1
			// since each join between 2 columns is its own map
			Collection<String> tableCols = null;
			if (grabKeys) {
				tableCols = joinMap.keySet();
			} else {
				tableCols = joinMap.values();
			}
			// keep track of where to add a ","
			int counter = 0;
			int numCols = tableCols.size();
			for (String colName : tableCols) {
				builder.append("\"").append(colName).append("\"");
				if (counter + 1 != numCols) {
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
	
	public static String alterColumnNames(String tableName, String[] oldHeaders, String[] newHeaders) {
		// create loop to generate key value pair
		Map<String, String> headerMapping = new HashMap<>();
		for(int i = 0; i < oldHeaders.length; i++) {
			headerMapping.put(oldHeaders[i], newHeaders[i]);
		}
		return alterColumnNames(tableName, headerMapping);
	}
	
	public static String alterColumnNames(String tableName, Map<String, String>  oldNewHeaders) {
		// create loop to generate key value pair
		int numRenames = oldNewHeaders.size();
		StringBuilder keyValues = new StringBuilder();
		int i = 1;
		for (String oldHeader : oldNewHeaders.keySet()) {
			String newHeader = oldNewHeaders.get(oldHeader);
			if (i == numRenames) {
				keyValues.append("'" + oldHeader + "':'" + newHeader + "'");
				break;
			} else {
				keyValues.append("'" + oldHeader + "':'" + newHeader + "',");
			}
			i++;
		}
		return tableName + ".rename(columns={" + keyValues.toString() + "}, inplace=True)";
	}

	/**
	 * Generate an alter statement to add new columns, taking into consideration
	 * joins and new column alias's
	 * 
	 * @param tableName
	 * @param existingColumns
	 * @param newColumns
	 * @param joins
	 * @param newColumnAlias
	 * @return
	 */
	public static String alterMissingColumns(String tableName, String[] curHeaders,
			Map<String, SemossDataType> newColumnsToTypeMap, List<Join> joins, Map<String, String> newColumnAlias) {
		List<String> newColumnsToAdd = new Vector<String>();
		List<SemossDataType> newColumnsToAddTypes = new Vector<SemossDataType>();

		// get all the join columns
		List<String> joinColumns = new Vector<String>();
		for (Join j : joins) {
			String columnName = j.getRColumn();
			if (columnName.contains("__")) {
				columnName = columnName.split("__")[1];
			}
			joinColumns.add(columnName);
		}

		for (String newColumn : newColumnsToTypeMap.keySet()) {
			SemossDataType newColumnType = newColumnsToTypeMap.get(newColumn);
			// modify the header
			if (newColumn.contains("__")) {
				newColumn = newColumn.split("__")[1];
			}
			// if its a join column, ignore it
			if (joinColumns.contains(newColumn)) {
				continue;
			}
			// not a join column
			// check if it has an alias
			// and then add
			if (newColumnAlias.containsKey(newColumn)) {
				newColumnsToAdd.add(newColumnAlias.get(newColumn));
			} else {
				newColumnsToAdd.add(newColumn);
			}
			// and store the type at the same index
			// in its list
			newColumnsToAddTypes.add(newColumnType);
		}

		// TODO: account for column types

		StringBuffer command = new StringBuffer(tableName).append(".reindex(columns=[");
		// add current headers
		for (int i = 0; i < curHeaders.length; i++) {
			command.append("\"").append(curHeaders[i]).append("\",");
		}

		// add the new headers
		int numNew = newColumnsToAdd.size();
		for (int i = 0; i < numNew; i++) {
			String newCol = newColumnsToAdd.get(i);
			SemossDataType newColType = newColumnsToAddTypes.get(i);

			command.append("\"").append(newCol).append("\"");
			if ((i + 1) < numNew) {
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
	public static String getDFLength(String tableName) {
		String dfLength = "len(" + tableName + ".index)";
		return dfLength;
	}

	// gets all the columns as an array
	public static String getColumns(String tableName) {
		String cols = "list(" + tableName + ".columns)";
		return cols;
	}

	// get all types
	// this needs to be used in combination with get columns
	public static String getTypes(String tableName) {
		String types = "list(" + tableName + ".dtypes.astype(str))";
		return types;
	}

	public static String getColumnType(String tableName, String column) {
		String types = tableName + "['" + column + "'].dtype.name";
		return types;
	}

	// gets the record
	public static String getColumnChange(String tableName, String colName, String type) {
		String typeChanger = tableName + "['" + colName + "'] = " + tableName + "['" + colName + "'].astype('" + type
				+ "')";
		return typeChanger;
	}

	/**
	 * Remove duplicate columns in a frame based on column names
	 * 
	 * @param tableName the frame with the duplicate column names
	 * @param newTable  the new frame to set
	 * @return Python syntax to remove duplicate columns
	 */
	public static String removeDuplicateColumns(String tableName, String newTable) {
		String script = newTable + " = " + tableName + ".loc[:,~" + tableName + ".columns.duplicated()]";
		return script;
	}

	/**
	 * Create a pandas vector from a java vector
	 * 
	 * @param row      The object[] to convert
	 * @param dataType The data type for each entry in the object[]
	 * @return String containing the equivalent r column vector
	 */
	public static String createPandasColVec(List<Object> row, SemossDataType dataType) {
		StringBuffer str = new StringBuffer("([");
		int i = 0;
		int size = row.size();
		for (; i < size; i++) {
			if (SemossDataType.STRING == dataType) {
				String escaper = row.get(i) + "";
				escaper = escaper.replace("'", "\\'");
				str.append("'").append(escaper).append("'");
			} else if (SemossDataType.INT == dataType || SemossDataType.DOUBLE == dataType) {
				str.append(row.get(i).toString());
			} else if (SemossDataType.DATE == dataType) {
				str.append("np.datetime64(\"" + row.get(i).toString() + "\")");
			} else if (SemossDataType.TIMESTAMP == dataType) {
				str.append("np.datetime64(\"" + row.get(i).toString() + "\")");
			} else {
				// just in case this is not defined yet...
				// see the type of the value and add it in based on that
				if (dataType == null) {
					if (row.get(i) instanceof String) {
						String escaper = row.get(i) + "";
						escaper = escaper.replace("'", "\\'");
						str.append("'").append(escaper).append("'");
					} else {
						str.append(row.get(i));
					}
				} else {
					str.append(row.get(i));
				}
			}
			// if not the last entry, append a "," to separate entries
			if ((i + 1) != size) {
				str.append(",");
			}
		}
		str.append("])");
		return str.toString();
	}

	public static String formatFilterValue(Object value, SemossDataType dataType) {
		if (SemossDataType.STRING == dataType) {
			return "'" + value + "'";
		} else if (SemossDataType.INT == dataType || SemossDataType.DOUBLE == dataType) {
			return value.toString();
		} else if (SemossDataType.DATE == dataType) {
			return "np.datetime64(\"" + value.toString() + "\", format='%Y-%m-%d')";
		} else if (SemossDataType.TIMESTAMP == dataType) {
			return "np.datetime64(\"" + value.toString() + "\", format='%Y-%m-%d %H:%M:%S')";
		} else {
			// just in case this is not defined yet...
			// see the type of the value and add it in based on that
			if (dataType == null) {
				if (value instanceof String) {
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
		String[] pyColNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		StringBuffer colRen = new StringBuffer("{");
		for (int i = 0; i < colNames.length; i++) {
			colRen.append("'").append(colNames[i]).append("':'").append(pyColNames[i]).append("'");
			if (i < colNames.length - 1) {
				colRen.append(", ");
			}
		}
		colRen.append("}");
		String script = frameName + ".rename(columns=" + colRen + ", inplace=True)";
		return script;
	}

	/**
	 * Set the column names for a frame
	 * 
	 * @param tableName
	 * @param headers   the new column names to use
	 * @return
	 */
	public static String setColumnNames(String tableName, String[] headers) {
		StringBuffer sb = new StringBuffer();
		for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
			if (sb.length() == 0) {
				sb.append("[");
			} else {
				sb.append(",");
			}
			sb.append("'").append(headers[headerIndex]).append("'");
		}
		sb.append("]");
		String headerS = tableName + ".columns=" + sb.toString();
		return headerS;
	}

	/** 
	 * Filters a data frame on specified columns.
	 * @param tableName
	 * @param newTableName
	 * @param columns
	 * @return
	 */
	public static String filterByColumn(String tableName, String newTableName, List<String> columns) {
		StringBuffer sb = new StringBuffer();
		if (columns.isEmpty() || columns == null) {
			return newTableName + "=" + tableName;
		}
		sb.append("[");

		if (columns.size() == 1) {
			sb.append("'" + columns.get(0) + "'");
		} else {
			for (int i = 0; i < columns.size() - 1; i++) {
				sb.append("'" + columns.get(i) + "', ");
			}
			sb.append("'" + columns.get(columns.size() - 1) + "'");
		}
		sb.append("]");

		String script = newTableName + " = " + tableName + "[" + sb.toString() + "]";
		return script;
	}

	/** 
	 * Filters a data frame by taking a slice based on index values. 
	 * @param tableName
	 * @param newTableName
	 * @param rowEnd
	 * @param rowStart
	 * @return
	 */
	public static String filterRowBySlice(String tableName, String newTableName, Object indexEnd, Object indexStart)  {
		boolean nullPassed = false;
		try {
			if (indexStart == null) {
				indexStart = "0";
				nullPassed = true;
			}
			Integer.parseInt((String) indexStart);
			Integer.parseInt((String) indexEnd);

			String script = newTableName + " = " + tableName + ".iloc[" + indexStart + ":" + indexEnd + "]";
			return script;
		} 
		catch (NumberFormatException e) {
			if (nullPassed) {
				throw new IllegalArgumentException("Incorrect input of 'null' when filtering by label.");
			}
			String script = newTableName + " = " + tableName + ".loc['" + indexStart + "':'" + indexEnd + "']";
			return script;
		} 
	}

	/** 
	 * Filters data frame by rows, either numeric or label.
	 * @param tableName
	 * @param newTableName
	 * @param rowIndex
	 * @return
	 */
	public static String filterByRow(String tableName, String newTableName, List<String> rowIndex) {
		StringBuffer sb = new StringBuffer();

		if (rowIndex.isEmpty() || rowIndex == null) {
			String script = newTableName + " = " + tableName;
			return script;
		}

		try {
			sb.append("[");
			if (rowIndex.size() == 1) {
				Integer.parseInt(rowIndex.get(0));
				sb.append(rowIndex.get(0) + "]");
				String script = newTableName + " = " + tableName + ".iloc[" + sb.toString() + "]";
				return script;
			}
			else {
				for (int i = 0; i < rowIndex.size() - 1; i++) {
					Integer.parseInt(rowIndex.get(i));
					sb.append(rowIndex.get(i) + ", ");
				}
				Integer.parseInt(rowIndex.get(rowIndex.size() - 1));
				sb.append(rowIndex.get(rowIndex.size() - 1) + "]");
				String script = newTableName + " = " + tableName + ".iloc[" + sb.toString() + "]";
				return script;
			}
		}
		catch (NumberFormatException e) {
			sb.delete(0, sb.length());
			sb.append("['");
			if (rowIndex.size() == 1) {
				sb.append(rowIndex.get(0) + "']");
				String script = newTableName + " = " + tableName + ".loc[" + sb.toString() + "]";
				return script;
			}
			else {
				for (int i = 0; i < rowIndex.size() - 1; i++) {
					sb.append(rowIndex.get(i) + "', '");
				}
				sb.append(rowIndex.get(rowIndex.size() - 1) + "']");
				String script = newTableName + " = " + tableName + ".loc[" + sb.toString() + "]";
				return script;
			}
		}
	} 

	/** 
	 * Filter pandas data frame by specified value and operator. Handles both numeric and object value types. 
	 * @param tableName
	 * @param newTableName
	 * @param columnName
	 * @param operator
	 * @param value
	 * @return
	 */
	public static String filterBySingleValue(String tableName, String newTableName, String columnName, String operator, Object value) {
		try {
			Double.parseDouble((String) value);

			// check to ensure valid operator
			if (!OPERATOR_LIST.contains(operator.trim())) {
				throw new IllegalArgumentException("Operator " + operator.trim() + " is not a valid operator.");
			} 

			String script = newTableName + " = " + tableName + "[" + tableName + "['" + columnName + "']"
					+ operator.trim() + value + "]";
			return script;

		} 
		catch (NumberFormatException e) {
			if (!(operator.trim().equals(EQUAL) || operator.trim().equals(NOT_EQUAL))) {
				throw new IllegalArgumentException("Operator " + operator.trim() + " is not a valid operator.");
			}
			String script = newTableName + " = " + tableName + "[" + tableName + "['" + columnName + "']"
					+ operator.trim() + "'" +  value + "']";
			return script;
		} 
	}

	/** 
	 * Filters a data frame using boolean expressions on multiple values in a single column. 
	 * @param tableName
	 * @param newTableName
	 * @param columnName
	 * @param values
	 * @param negation -- default to null
	 * @return
	 */
	public static String filterByMultipleValues(String tableName, String newTableName, String columnName, List<String> values,
			Object negation) {
		if (negation == null) {
			negation = "";
		}
		StringBuffer sb = new StringBuffer();

		if (values.isEmpty() || values == null) {
			String script = newTableName + " = " + tableName;
			return script;
		}
		try {
			sb.append("[");
			if (values.size() == 1) {
				Double.parseDouble(values.get(0));
				sb.append(values.get(0) + "]");
				String script = newTableName + " = " + tableName + "[" + negation + tableName + "['" + columnName 
						+ "'].isin(" + sb.toString() + ")]";
				return script;
			}
			else {
				for (int i = 0; i < values.size() - 1; i++) {
					Double.parseDouble(values.get(i));
					sb.append(values.get(i) + ", ");
				}
				Double.parseDouble(values.get(values.size() - 1));
				sb.append(values.get(values.size() - 1) + "]");
				String script = newTableName + " = " + tableName + "[" + negation + tableName + "['" + columnName 
						+ "'].isin(" + sb.toString() + ")]";
				return script;
			}
		}
		catch (NumberFormatException e) {
			sb.delete(0, sb.length());
			sb.append("['");
			if (values.size() == 1) {
				sb.append(values.get(0) + "']");
				String script = newTableName + " = " + tableName + "[" + negation + tableName + "['" + columnName 
						+ "'].isin(" + sb.toString() + ")]";
				return script;
			}
			else {
				for (int i = 0; i < values.size() - 1; i++) {
					sb.append(values.get(i) + "', '");
				}
				sb.append(values.get(values.size() - 1) + "']");
				String script = newTableName + " = " + tableName + "[" + negation + tableName + "['" + columnName
						+ "'].isin(" + sb.toString() + ")]";
				return script;
			}
		}
	}

	/**
	 * Filters a data frame by whether a specified column contains an expression. Requirements are that columnType is object.
	 * @param tableName
	 * @param newTableName
	 * @param columnName
	 * @param expression
	 * @param negation -- default to null
	 * @return
	 */
	public static String filterByExpression(String tableName, String newTableName, String columnName, Object expression, 
			Object negation) {
		if (negation == null) {
			negation = "";
		}
		String script = newTableName + " = " + tableName + "[" + negation + tableName + "['" + columnName
				+ "'].str.contains('" + expression + "')]";
		return script;
	}

	/** 
	 * Filters a data frame by selecting all rows between 2 values. 
	 * @param tableName
	 * @param newTableName
	 * @param columnName
	 * @param floor
	 * @param ceiling
	 * @param negation -- default to null
	 * @return
	 */
	public static String filterBetweenNumbers(String tableName, String newTableName, String columnName, Object floor, 
			Object ceiling, Object negation) {
		if (negation == null) {
			negation = "";
		}
		String script = newTableName + " = " + tableName + "[" + negation + tableName + "['" + columnName 
				+ "'].between(" + floor.toString() + ", " + ceiling.toString() + ")]";
		return script;
	}

}
