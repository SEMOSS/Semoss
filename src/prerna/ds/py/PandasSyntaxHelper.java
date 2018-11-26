package prerna.ds.py;

import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.HeadersException;

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
