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
		TinkerFrame dataFrame = new TinkerH2Frame(headers);
		Map<String, Set<String>> primKeyEdgeHash = dataFrame.createPrimKeyEdgeHash(headers);
		dataFrame.mergeEdgeHash(primKeyEdgeHash);		
		return dataFrame;
	}
	
	/**
	 * Method to generate a Tinker Frame from a JSON string
	 * Assumes all the records are unique so it assigned a unique key to each row
	 * @param jsonString				The JSON string which contains the data
	 * @return							The TinkerFrame object
	 */
	public static TinkerFrame generateTinkerFrameFromJson(String jsonString) {
		Gson gson = new Gson();
		
		List<Map<String, Object>> data = gson.fromJson(jsonString, new TypeToken<List<Map<String, Object>>>() {}.getType());
		String[] headers = data.get(0).keySet().toArray(new String[]{});
		String[] cleanHeaders = new String[headers.length];
		// keep boolean so dont need to compare strings with each iteration
		boolean[] cleanIsSame = new boolean[headers.length];
		// clean headers
        for(int i = 0; i<headers.length; i++){
              String orig = headers[i];
              String cleaned = Utility.cleanVariableString(orig);
              cleanHeaders[i] = cleaned;
              if(orig.equals(cleaned)) {
            	  cleanIsSame[i] = true;
              }
        }
		
		TinkerFrame dataFrame = createPrimKeyTinkerFrame(cleanHeaders);
		
		int i = 0;
		int numRows = data.size();
		for(; i < numRows; i++) {
			Map<String, Object> row = data.get(i);
            Map<String, Object> cleanRow = new HashMap<String, Object>();
			for(int j = 0; j < headers.length; j++) {
				Object value = row.get(headers[j]);
				Object cleanVal = null;
				if(value instanceof String) {
					// if string need to clean string for clean values and need to append type for raw
					// might as well add with clean headers as well
					cleanVal = "http://" + cleanHeaders[j] + "/" + value;
					row.remove(headers[j]);
					row.put(cleanHeaders[j], cleanVal);
	                cleanRow.put(cleanHeaders[j], Utility.cleanString(value + "", true, true, false));
				} else if(!cleanIsSame[j]) {
					// if not the same, need to use the clean headers on raw
					// need to add on the value with its type
					row.remove(headers[j]);
					row.put(cleanHeaders[j], value);
	                cleanRow.put(cleanHeaders[j], value);
				} else {
					// need to add on the raw the value with its type
	                cleanRow.put(cleanHeaders[j], value);
				}
			}
            dataFrame.addRow(cleanRow, row);
		}
		
		return dataFrame;
	}

	
	
	/**
	 * Method to generate a Tinker Frame from a CSV file
	 * @param dataStr
	 * @param delimeter
	 * @return
	 */
	public static TinkerFrame generateDataFrameFromFile(String dataStr, String delimeter) {
		long sT = System.currentTimeMillis();

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
		
		TinkerH2Frame dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers);
		CsvParser parser = getDefaultParser();
		
		for(int i = 1; i < rows.length; i++) {
			String[] cells = parser.parseLine(rows[i]); 
			dataFrame.addRow(cells);
		}
				
		return dataFrame;
	}
	
//	/**
//	 * Method to generate a Tinker Frame from a CSV file
//	 * @param dataStr
//	 * @param delimeter
//	 * @return
//	 */
//	public static TinkerFrame generateDataFrameFromFile(String[][] data, String[] headers) {
//		long sT = System.currentTimeMillis();
//
//		if(data.length == 0) {
//			throw new IllegalArgumentException("No data has been inputted");
//		}
//		
//		int numHeaders = headers.length;
//		for(int i = 0; i< numHeaders; i++){
//			headers[i] = Utility.cleanVariableString(headers[i]);
//		}
//		
//		TinkerH2Frame dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers);
//		
//		for(int i = 0; i < data.length; i++) {
//			String[] cells = data[i]; 
//			dataFrame.addRow(cells);
//		}
//				
//		return dataFrame;
//	}
//	
	private static CsvParser getDefaultParser() {
		CsvParserSettings settings = new CsvParserSettings();
    	settings.setNullValue("");

        settings.setEmptyValue(""); // for CSV only
        settings.setSkipEmptyLines(true);

    	CsvParser parser = new CsvParser(settings);	
    	return parser;
	}


}
