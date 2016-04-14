package prerna.ds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.TinkerH2Frame;
import prerna.ds.util.TinkerCastHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.XLFileHelper;

public class TableDataFrameFactory {

	private static final String CSV_FILE_KEY = "CSV";
	
	/**
	 * Method to generate a table data frame from a file
	 * @param dataStr
	 * @param delimeter
	 * @param dataTypeMap 
	 * @return
	 */
	public static ITableDataFrame generateDataFrameFromFile(String fileLoc, String delimeter, String dataFrameType, Map<String, Map<String, String>> dataTypeMap) {
		if(dataFrameType.equalsIgnoreCase("H2")) {
			if(fileLoc.endsWith(".xlsx") || fileLoc.endsWith(".xlsm")) {
				return generateH2FrameFromExcel(fileLoc, dataTypeMap);
			} else {
				return generateH2FrameFromFile(fileLoc, delimeter, dataTypeMap.get(CSV_FILE_KEY));
			}
		} else {
			if(fileLoc.endsWith(".xlsx") || fileLoc.endsWith(".xlsm")) {
				return generateTinkerFrameFromExcel(fileLoc, dataTypeMap);
			} else {
				return generateTinkerFrameFromFile(fileLoc, delimeter, dataTypeMap.get(CSV_FILE_KEY));
			}
		}
	}
	
	//////////////////////// START EXCEL LOADING //////////////////////////////////////
	
	private static TinkerFrame generateTinkerFrameFromExcel(String fileLoc, Map<String, Map<String, String>> dataTypeMap) {
		XLFileHelper helper = new XLFileHelper();
		helper.parse(fileLoc);
		String[] tables = helper.getTables();
		
		TinkerFrame tf = null;
		TinkerCastHelper caster = new TinkerCastHelper();

		int tableCounter = 0;
		for(int i = 0; i < tables.length; i++)
		{
			String primKeyHeader = null;
			String sheetName = tables[i];
			
			String [] headers = null;
			String [] types = null;
			
			if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
				Map<String, String> sheetMap = dataTypeMap.get(sheetName);
				
				if(sheetMap == null || sheetMap.isEmpty()) {
					//not loading anything from this sheet
					continue;
				}
				
				headers = sheetMap.keySet().toArray(new String[]{});
				headers = helper.orderHeadersToGet(sheetName, headers);
				
				types = new String[headers.length];
				for(int j = 0; j < headers.length; j++) {
					types[j] = sheetMap.get(headers[j]);
				}
			} else {
				headers = helper.getHeaders(sheetName);
				types = helper.predictRowTypes(sheetName);			
			}
			
			if(tf == null) {
				tf = (TinkerFrame) createPrimKeyTinkerFrame(headers, "TinkerFrame", types);
			} else {
				Map<String, Set<String>> newEdgeHash = new Hashtable<String, Set<String>>();
				// need to create a new prim_key vertex
				Set<String> values = new HashSet<String>();
				values.addAll(Arrays.asList(headers));
				primKeyHeader = TinkerFrame.PRIM_KEY + "_" + i;
				newEdgeHash.put(primKeyHeader, values);
				tf.mergeEdgeHash(newEdgeHash);
				tf.addMetaDataTypes(headers, types);
			}
			
			Object[] values = null;	
			String[] row = null;
			helper.getNextRow(sheetName); // first row is header
			while( ( row = helper.getNextRow(sheetName, headers) ) != null) {
				values = caster.castToTypes(row, types);
				Map<String, Object> cleanRow = new HashMap<>();
				Map<String, Object> rawRow = new HashMap<>();
				for(int j = 0; j < headers.length; j++) {
					
					String header = headers[j];
					Object value = values[j];
					String rawVal = "http://" + header + "/" + value;

					cleanRow.put(headers[j], values[j]);
					rawRow.put(header, rawVal);
				}
				if(tableCounter == 0) {
					tf.addRow(cleanRow, rawRow);
				} else {
					String primKeyVal = values.hashCode() + "";
					cleanRow.put(primKeyHeader, primKeyVal);
					rawRow.put(primKeyHeader, primKeyVal);
					
					tf.addRelationship(cleanRow, rawRow);
				}
			}
			tableCounter++;
		}
		
		return tf;
	}

	private static TinkerH2Frame generateH2FrameFromExcel(String fileLoc, Map<String, Map<String, String>> dataTypeMap) {
		
		XLFileHelper helper = new XLFileHelper();
		helper.parse(fileLoc);
		
		TinkerH2Frame dataFrame = new TinkerH2Frame();
		String [] cells = null;
		String[] tables = helper.getTables();
		for(int i = 0; i < tables.length; i++)
		{
			String primKeyHeader = null;
			String table = tables[i];
			
			String [] headers = null;
			String [] types = null;
			
			if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
				Map<String, String> sheetMap = dataTypeMap.get(table);
				
				if(sheetMap == null || sheetMap.isEmpty()) {
					//not loading anything from this sheet
					continue;
				}
				
				headers = sheetMap.keySet().toArray(new String[]{});
				headers = helper.orderHeadersToGet(table, headers);
				
				types = new String[headers.length];
				for(int j = 0; j < headers.length; j++) {
					types[j] = sheetMap.get(headers[j]);
				}
			} else {
				headers = helper.getHeaders(table);
				types = helper.predictRowTypes(table);			
			}
			
			// need to make everything a property of the table (i.e. sheet)
			for(int index = 0; index < headers.length; index++) {
				String header = headers[index];
				headers[index] = table+"__"+header;
			}
			
			Map<String, Set<String>> newEdgeHash = new Hashtable<String, Set<String>>();
			// need to create a new prim_key vertex
			Set<String> values = new HashSet<String>();
			values.addAll(Arrays.asList(headers));
			primKeyHeader = TinkerFrame.PRIM_KEY + "_" + table;
			newEdgeHash.put(primKeyHeader, values);
			dataFrame.mergeEdgeHash(newEdgeHash);
			dataFrame.addMetaDataTypes(headers, types);

			helper.getNextRow(table); // first row is header
			dataFrame.setMetaData(table, headers, types);
			while((cells = helper.getNextRow(table, headers)) != null) {
				//add these cells to this table
				dataFrame.addRow(table, cells);
			}
		}
		dataFrame.setRelations(helper.getRelations());
		
		return dataFrame;
	}

	//////////////////////// END EXCEL LOADING //////////////////////////////////////

	
	//////////////////////// START CSV LOADING //////////////////////////////////////
	
	private static TinkerH2Frame generateH2FrameFromFile(String fileLoc, String delimiter, Map<String, String> dataTypeMap) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimiter.charAt(0));
		helper.parse(fileLoc);
		
		String [] headers = null;
		String [] types = null;
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			headers = new String[dataTypeMap.keySet().size()];
			types = new String[headers.length];
			int counter = 0;
			for(String header : dataTypeMap.keySet()) {
				headers[counter] = header;
				types[counter] = dataTypeMap.get(header);
				counter++;
			}
			
			helper.parseColumns(headers);
			helper.getNextRow(); // next row is a header
		} else {
			headers = helper.getHeaders();
			types = helper.predictTypes();			
		}
		
		TinkerH2Frame dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers, "H2", types);
		
		// unique names always match the headers when creating from csv/excel
		String[] values = new String[headers.length];
		for(int i = 0; i < headers.length; i++) {
			values[i] = dataFrame.getValueForUniqueName(headers[i]);
		}
		
		String tableName = null;
		List<String> uniqueNames = Arrays.asList(dataFrame.getColumnHeaders());
		for(String name : uniqueNames) {
			if(name.startsWith(TinkerFrame.PRIM_KEY)) {
				tableName = dataFrame.getValueForUniqueName(name);
				break;
			}
		}
		
		String [] cells = null;
		while((cells = helper.getNextRow()) != null) {
			dataFrame.addRow2(tableName, cells, values, types);
		}
		return dataFrame;
	}
	
	private static TinkerFrame generateTinkerFrameFromFile(String fileLoc, String delimiter, Map<String, String> dataTypeMap) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimiter.charAt(0));
		helper.parse(fileLoc);
		
		String [] headers = null;
		String [] types = null;
		
		if(dataTypeMap != null && !dataTypeMap.isEmpty()) {
			int counter = 0;
			headers = helper.getHeaders();
			types = new String[headers.length];
			for(String header : headers) {
				types[counter] = dataTypeMap.get(header);
				counter++;
			}
			
			helper.parseColumns(headers);
			helper.getNextRow(); // next row is a header
		} else {
			headers = helper.getHeaders();
			types = helper.predictTypes();			
		}

		TinkerFrame dataFrame = (TinkerFrame)createPrimKeyTinkerFrame(headers, "TinkerFrame", types);
		
		TinkerCastHelper caster = new TinkerCastHelper();
		String[] cells = null;
		Object[] values = null;
		while((cells = helper.getNextRow()) != null) {
		
			values = caster.castToTypes(cells, types);
			Map<String, Object> row = new HashMap<>();
			Map<String, Object> rawRow = new HashMap<>();
			for(int i = 0; i < headers.length; i++) {
				
				String header = headers[i];
				Object value = values[i];
				String rawVal = "http://" + header + "/" + value;

				row.put(headers[i], values[i]);
				rawRow.put(header, rawVal);
			}
			dataFrame.addRow(row, rawRow);
		}
				
		return dataFrame;
	}
	
	//////////////////////// END CSV LOADING //////////////////////////////////////

	private static ITableDataFrame createPrimKeyTinkerFrame(String[] headers, String dataFrameType, String[] types) {
		
		ITableDataFrame dataFrame;
		if(dataFrameType.equalsIgnoreCase("H2")) {
			dataFrame = new TinkerH2Frame(headers);
		} else {
			dataFrame = new TinkerFrame(headers);
		}
		Map<String, Set<String>> primKeyEdgeHash = dataFrame.createPrimKeyEdgeHash(headers);
		dataFrame.mergeEdgeHash(primKeyEdgeHash);
		dataFrame.addMetaDataTypes(headers, types);
		return dataFrame;
	}
}
