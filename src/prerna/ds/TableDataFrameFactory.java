package prerna.ds;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

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
				newEdgeHash.put(TinkerFrame.PRIM_KEY + "_" + i, values);
				tf.mergeEdgeHash(newEdgeHash);
				tf.addMetaDataTypes(headers, types);
			}
			
			Object[] values = null;	
			String[] row = null;
			while( ( row = helper.getNextRow(sheetName) ) != null) {
				values = caster.castToTypes(row, types);
				Map<String, Object> cleanRow = new HashMap<>();
				Map<String, Object> rawRow = new HashMap<>();
				for(int j = 0; j < headers.length; j++) {
					
					String header = headers[j];
					Object value = values[j];
					String rawVal = "http://" + header + "/" + value;

					cleanRow.put(headers[i], values[i]);
					rawRow.put(header, rawVal);
				}
				tf.addRow(cleanRow, rawRow);
			}
		}
		
		return tf;
	}

	private static TinkerFrame generateH2FrameFromExcel(String fileLoc) {
		
		
		
		return null;
	}

	private static TinkerH2Frame generateH2FrameFromFile(String fileLoc, String delimeter) {
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delimeter.charAt(0));
		helper.parse(fileLoc);
		
		String [] headers = helper.getHeaders();
		String [] types = helper.predictTypes();
		TinkerH2Frame dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers, "H2", types);
		
		String [] cells = null;
		while((cells = helper.getNextRow()) != null) {
			dataFrame.addRow(cells);
		}
				
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
