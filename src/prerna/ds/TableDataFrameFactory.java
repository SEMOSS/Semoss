package prerna.ds;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.math.NumberUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import prerna.ds.H2.TinkerH2Frame;
import prerna.util.Utility;

public class TableDataFrameFactory {

	/**
	 * Method to create an empty TinkerFrame with a primary key metamodel (flat table)
	 * @param headers
	 * @return
	 */
	public static TinkerFrame createPrimKeyTinkerFrame(String[] headers) {
		return createPrimKeyTinkerFrame(headers, "tinkerFrame");
	}
	
	public static TinkerFrame createPrimKeyTinkerFrame(String[] headers, String dataFrameType) {
		
		TinkerFrame dataFrame;
		if(dataFrameType.equalsIgnoreCase("H2")) {
			dataFrame = new TinkerH2Frame(headers);
		} else {
			dataFrame = new TinkerFrame(headers);
		}
		Map<String, Set<String>> primKeyEdgeHash = dataFrame.createPrimKeyEdgeHash(headers);
		dataFrame.mergeEdgeHash(primKeyEdgeHash);		
		return dataFrame;
	}
	
//	/**
//	 * Method to generate a Tinker Frame from a JSON string
//	 * Assumes all the records are unique so it assigned a unique key to each row
//	 * @param jsonString				The JSON string which contains the data
//	 * @return							The TinkerFrame object
//	 */
//	public static TinkerFrame generateTinkerFrameFromJson(String jsonString) {
//		Gson gson = new Gson();
//		
//		List<Map<String, Object>> data = gson.fromJson(jsonString, new TypeToken<List<Map<String, Object>>>() {}.getType());
//		String[] headers = data.get(0).keySet().toArray(new String[]{});
//		String[] cleanHeaders = new String[headers.length];
//		// keep boolean so dont need to compare strings with each iteration
//		boolean[] cleanIsSame = new boolean[headers.length];
//		// clean headers
//        for(int i = 0; i<headers.length; i++){
//              String orig = headers[i];
//              String cleaned = Utility.cleanVariableString(orig);
//              cleanHeaders[i] = cleaned;
//              if(orig.equals(cleaned)) {
//            	  cleanIsSame[i] = true;
//              }
//        }
//		
//		TinkerFrame dataFrame = createPrimKeyTinkerFrame(cleanHeaders);
//		
//		int i = 0;
//		int numRows = data.size();
//		for(; i < numRows; i++) {
//			Map<String, Object> row = data.get(i);
//            Map<String, Object> cleanRow = new HashMap<String, Object>();
//			for(int j = 0; j < headers.length; j++) {
//				Object value = row.get(headers[j]);
//				Object cleanVal = null;
//				if(value instanceof String) {
//					// if string need to clean string for clean values and need to append type for raw
//					// might as well add with clean headers as well
//					cleanVal = "http://" + cleanHeaders[j] + "/" + value;
//					row.remove(headers[j]);
//					row.put(cleanHeaders[j], cleanVal);
//	                cleanRow.put(cleanHeaders[j], Utility.cleanString(value + "", true, true, false));
//				} else if(!cleanIsSame[j]) {
//					// if not the same, need to use the clean headers on raw
//					// need to add on the value with its type
//					row.remove(headers[j]);
//					row.put(cleanHeaders[j], value);
//	                cleanRow.put(cleanHeaders[j], value);
//				} else {
//					// need to add on the raw the value with its type
//	                cleanRow.put(cleanHeaders[j], value);
//				}
//			}
//            dataFrame.addRow(cleanRow, row);
//		}
//		
//		return dataFrame;
//	}

	
	
	/**
	 * Method to generate a Tinker Frame from a CSV file
	 * @param dataStr
	 * @param delimeter
	 * @return
	 */
	public static TinkerFrame generateDataFrameFromFile(String dataStr, String delimeter, String dataFrameType) {
		long sT = System.currentTimeMillis();

		if(dataFrameType.equalsIgnoreCase("H2")) {
			return generateH2FrameFromFile(dataStr, delimeter);
		} else {
			return generateTinkerFrameFromFile(dataStr, delimeter);
		}
	}
	
	public static TinkerH2Frame generateH2FrameFromFile(String dataStr, String delimeter) {
		String[] rows = dataStr.split("\\n");
		if(rows.length == 0) {
			throw new IllegalArgumentException("No data has been inputted");
		} else if(rows.length == 1) {
			throw new IllegalArgumentException("Only a header row has been inputted.  Need at least one row of values");
		}
		String headerStr = rows[0];
		String[] headers = headerStr.split(delimeter + "{1}");
		int numHeaders = headers.length;
		for(int i = 0; i< numHeaders; i++){
			headers[i] = Utility.cleanVariableString(headers[i]);
		}
		
		TinkerH2Frame dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers, "H2");
		CsvParser parser = getDefaultParser();
		
		for(int i = 1; i < rows.length; i++) {
			String[] cells = parser.parseLine(rows[i]); 
			dataFrame.addRow(cells);
		}
				
		return dataFrame;
	}
	
	private static CsvParser getDefaultParser() {
		CsvParserSettings settings = new CsvParserSettings();
    	settings.setNullValue("");

        settings.setEmptyValue(""); // for CSV only
        settings.setSkipEmptyLines(true);

    	CsvParser parser = new CsvParser(settings);	
    	return parser;
	}
	
	public static TinkerFrame generateTinkerFrameFromFile(String dataStr, String delimeter) {
		long sT = System.currentTimeMillis();
		
		final DateFormat DATE_DF = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSSSSS");
		final DateFormat SIMPLE_DATE_DF = new SimpleDateFormat("yyyy/MM/dd");
		final String DATE_KEY = "DATE";
		final String SIMPLE_DATE_KEY = "SIMPLE_DATE_KEY";
		final String NUMBER_KEY = "NUMBER";
		final String STRING_KEY = "STRING";

		String[] rows = dataStr.split("\\n");
		if(rows.length == 0) {
			throw new IllegalArgumentException("No data has been inputted");
		}
		if(rows.length == 1) {
			throw new IllegalArgumentException("Only a header row has been inputted.  Need at least one row of values");
		}
		String headerStr = rows[0];
		String[] headers = headerStr.split(delimeter + "{1}");
		int numHeaders = headers.length;
		for(int i = 0; i< numHeaders; i++){
			headers[i] = Utility.cleanVariableString(headers[i]);
		}
		
		TinkerFrame dataFrame = new TinkerFrame(headers);
		Map<String, Set<String>> primKeyEdgeHash = dataFrame.createPrimKeyEdgeHash(headers);
		dataFrame.mergeEdgeHash(primKeyEdgeHash); 
		
		int i = 1;
		int numRows = rows.length;
		
		String[] colTypes = new String[headers.length];
		// use top 5 rows to determine the type of the column
		for(; i < 5 && i < numRows; i++) {
			String rowStr = rows[i];
			if(rowStr.isEmpty()) {
				rowStr = TinkerFrame.EMPTY;
			}
			String[] row = rowStr.split(delimeter + "{1}", headers.length);
			if(row.length != numHeaders) {
				throw new IllegalArgumentException("Number of columns in row #" + (i+1) + " does not match the number of columns in the header.");
			}
			
			Map<String, Object> rowMap = new HashMap<String, Object>();
			Map<String, Object> cleanMap = new HashMap<String, Object>();
			ROW_LOOP : for(int j = 0; j < numHeaders; j++) {
				String valStr = row[j].trim();
				if(valStr == null || valStr.isEmpty() ) {
					rowMap.put(headers[j], "");
				}
				Object valObj = null;
				// check if number
				try {
					valObj = Double.parseDouble(valStr);
					rowMap.put(headers[j], valObj);
					cleanMap.put(headers[j], valObj);
					if(!STRING_KEY.equals(colTypes[j])) {
						colTypes[j] = NUMBER_KEY;
					}
					continue ROW_LOOP;
				} catch(NumberFormatException e) {
					//do nothing
				}
				// check if simple date
				try {
					valObj = SIMPLE_DATE_DF.parse(valStr + "");
					rowMap.put(headers[j], valObj);
					cleanMap.put(headers[j], valObj);
					if(!STRING_KEY.equals(colTypes[j])) {
						colTypes[j] = SIMPLE_DATE_KEY;
					}
					continue ROW_LOOP;
				}  catch (ParseException e) {
					//do nothing
				}
				// check if more complex date
				try {
					valObj = DATE_DF.parse(valStr + "");
					rowMap.put(headers[j], valObj);
					cleanMap.put(headers[j], valObj);
					if(!STRING_KEY.equals(colTypes[j])) {
						colTypes[j] = DATE_KEY;
					}
					continue ROW_LOOP;
				}  catch (ParseException e) {
					//do nothing
				}
				// must be string
				colTypes[j] = STRING_KEY;
				String cleanVal = "http://" + headers[j] + "/" + Utility.cleanString(valStr + "", true, true, false);
				rowMap.put(headers[j], cleanVal);
				cleanMap.put(headers[j], Utility.cleanString(valStr + "", true, true, false));
			}
            dataFrame.addRow(cleanMap, rowMap);
		}
		
		// in case the first 5 rows are empty
		for(int k = 0; k < numHeaders; k++) {
			if(colTypes[k] == null) {
				colTypes[k] = STRING_KEY;
			}
		}
		
		for(; i < numRows; i++) {
			String rowStr = rows[i];
			if(rowStr.isEmpty()) {
				rowStr = TinkerFrame.EMPTY;
			}
			String[] row = rowStr.split(delimeter + "{1}", headers.length);
			if(row.length != numHeaders) {
				throw new IllegalArgumentException("Number of columns in row #" + (i+1) + " does not match the number of columns in the header.");
			}
			
			Map<String, Object> rowMap = new HashMap<String, Object>();
			Map<String, Object> cleanMap = new HashMap<String, Object>();
			for(int j = 0; j < numHeaders; j++) {
				String valStr = row[j].trim();
				if(valStr == null || valStr.isEmpty() ) {
					rowMap.put(headers[j], "");
				}
				Object valObj = null;
				if(colTypes[j].equals(STRING_KEY)) {
					String cleanVal = "http://" + headers[j] + "/" + Utility.cleanString(valStr + "", true, true, false);
					rowMap.put(headers[j], cleanVal);
					cleanMap.put(headers[j], Utility.cleanString(valStr + "", true, true, false));
				} else if(colTypes[j].equals(NUMBER_KEY)) {
					try {
						valObj = Double.parseDouble(valStr);
						rowMap.put(headers[j], valObj);
						cleanMap.put(headers[j], valObj);
					} catch(NumberFormatException e) {
						// just add as string
						String cleanVal = "http://" + headers[j] + "/" + valStr;
						rowMap.put(headers[j], cleanVal);
						cleanMap.put(headers[j], Utility.cleanString(valStr + "", true, true, false));
					}
				} else if(colTypes[j].equals(SIMPLE_DATE_KEY)) {
					// check if simple date
					try {
						valObj = SIMPLE_DATE_DF.parse(valStr + "");
						rowMap.put(headers[j], valObj);
						cleanMap.put(headers[j], valObj);
					}  catch (ParseException e) {
						// just add as string
						String cleanVal = "http://" + headers[j] + "/" + valStr;
						rowMap.put(headers[j], cleanVal);
						cleanMap.put(headers[j], Utility.cleanString(valStr + "", true, true, false));
					}
				} else if(colTypes[j].equals(DATE_KEY)) {
					// check if more complex date
					try {
						valObj = DATE_DF.parse(valStr + "");
						rowMap.put(headers[j], valObj);
						cleanMap.put(headers[j], valObj);
					}  catch (ParseException e) {
						// just add as string
						String cleanVal = "http://" + headers[j] + "/" + valStr;
						rowMap.put(headers[j], cleanVal);
						cleanMap.put(headers[j], Utility.cleanString(valStr + "", true, true, false));
					}
				}
			}
            dataFrame.addRow(cleanMap, rowMap);
		}
		
		
		System.out.println("Time in sec = " + (System.currentTimeMillis() - sT)/1000);
		
		return dataFrame;
	}


}
