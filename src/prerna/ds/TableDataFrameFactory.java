package prerna.ds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.ds.H2.TinkerH2Frame;
import prerna.ds.util.TinkerCastHelper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.XLFileHelper;

public class TableDataFrameFactory {

	/**
	 * Method to generate a table data frame from a file
	 * @param dataStr
	 * @param delimeter
	 * @return
	 */
	public static TinkerFrame generateDataFrameFromFile(String fileLoc, String delimeter, String dataFrameType) {
		if(dataFrameType.equalsIgnoreCase("H2")) {
			if(fileLoc.endsWith(".xlsx") || fileLoc.endsWith(".xlsm")) {
				return generateH2FrameFromExcel(fileLoc);
			} else {
				return generateH2FrameFromFile(fileLoc, delimeter);
			}
		} else {
			if(fileLoc.endsWith(".xlsx") || fileLoc.endsWith(".xlsm")) {
				return generateTinkerFrameFromExcel(fileLoc);
			} else {
				return generateTinkerFrameFromFile(fileLoc, delimeter);
			}
		}
	}
	
	private static TinkerFrame generateTinkerFrameFromExcel(String fileLoc) {
		XLFileHelper helper = new XLFileHelper();
		helper.parse(fileLoc);
		String[] tables = helper.getTables();
		
		TinkerFrame tf = null;
		TinkerCastHelper caster = new TinkerCastHelper();

		for(int i = 0; i < tables.length; i++)
		{
			String primKeyHeader = null;
			String sheetName = tables[i];
			String[] headers = helper.getHeaders(sheetName);
			String[] types = helper.predictRowTypes(sheetName);
			
			if(tf == null) {
				tf = createPrimKeyTinkerFrame(headers, "TinkerFrame", types);
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
			while( ( row = helper.getNextRow(sheetName) ) != null) {
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
				if(i == 0) {
					tf.addRow(cleanRow, rawRow);
				} else {
					String primKeyVal = values.hashCode() + "";
					cleanRow.put(primKeyHeader, primKeyVal);
					rawRow.put(primKeyHeader, primKeyVal);
					
					tf.addRelationship(cleanRow, rawRow);
				}
			}
		}
		
		return tf;
	}

	private static TinkerFrame generateH2FrameFromExcel(String fileLoc) {
		
		XLFileHelper helper = new XLFileHelper();
		helper.parse(fileLoc);
		
		TinkerH2Frame dataFrame = new TinkerH2Frame();
		String [] cells = null;
		String[] tables = helper.getTables();
		for(int i = 0; i < tables.length; i++)
		{
			String primKeyHeader = null;
			String table = tables[i];
			String[] headers = helper.getHeaders(table);
			for(int index = 0; index < headers.length; index++) {
				String header = headers[index];
				headers[index] = table+"__"+header;
			}
			String[] types = helper.predictRowTypes(table);
			
//			if(dataFrame == null) {
//				dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers, "H2", types);
//			} else {
			
			Map<String, Set<String>> newEdgeHash = new Hashtable<String, Set<String>>();
			// need to create a new prim_key vertex
			Set<String> values = new HashSet<String>();
			values.addAll(Arrays.asList(headers));
			primKeyHeader = TinkerFrame.PRIM_KEY + "_" + table;
			newEdgeHash.put(primKeyHeader, values);
			dataFrame.mergeEdgeHash(newEdgeHash);
			dataFrame.addMetaDataTypes(headers, types);

			
//			}
			
			Object[] vals = null;	
			String[] row = null;
			helper.getNextRow(table); // first row is header
			dataFrame.setMetaData(table, headers, types);
			while((cells = helper.getNextRow(table)) != null) {
				//add these cells to this table
				dataFrame.addRow(table, cells);
			}
		}
		dataFrame.setRelations(helper.getRelations());
		
		return dataFrame;
	}

	
	private static TinkerH2Frame generateH2FrameFromFile(String fileLoc, String delimeter) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimeter.charAt(0));
		helper.parse(fileLoc);
		
		String [] headers = helper.getHeaders();
		String [] types = helper.predictTypes();
		
		TinkerH2Frame dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers, "H2", types);
		
		// for efficiency, we get all the meta information before the add row
		IMetaData metaData = dataFrame.getMetaData();
		// unique names always match the headers when creating from csv/excel
//		Map<String, String> uniqueNameToValueMap = new HashMap<String, String>();
		String[] values = new String[headers.length];
		for(int i = 0; i < headers.length; i++) {
			values[i] = metaData.getValueForUniqueName(headers[i]);
//			uniqueNameToValueMap.put(headers[i], values[i]);
		}
		
		String tableName = null;
		List<String> uniqueNames = metaData.getUniqueNames();
		for(String name : uniqueNames) {
			if(name.startsWith(TinkerH2Frame.PRIM_KEY)) {
				tableName = metaData.getValueForUniqueName(name);
				break;
			}
		}
		//set the meta data for the frame csv only has one table so just pass in null 
//		dataFrame.setMetaData(null, headers, types);
//		dataFrame.addMetaDataTypes(headers, types);
//		dataFrame.addH2Alias(headers);
		
		String [] cells = null;
		while((cells = helper.getNextRow()) != null) {
			dataFrame.addRow2(tableName, cells, values, types);
		}
//		dataFrame.H2HeaderMap = uniqueNameToValueMap;
		return dataFrame;
	}
	
	private static TinkerFrame generateTinkerFrameFromFile(String fileLoc, String delimeter) {
		// the data string is no longer string but a file name
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimeter.charAt(0));
		helper.parse(fileLoc);
		
		String [] headers = helper.getHeaders();
		String [] types = helper.predictTypes();
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
	
	private static TinkerFrame createPrimKeyTinkerFrame(String[] headers, String dataFrameType, String[] types) {
		
		TinkerFrame dataFrame;
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
