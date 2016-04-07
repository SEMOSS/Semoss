package prerna.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import prerna.ds.H2.H2Builder;
import prerna.ds.H2.TinkerH2Frame;
import prerna.ds.util.TinkerCastHelper;
import prerna.poi.main.helper.CSVFileHelper;

public class TableDataFrameFactory {

	/**
	 * Method to create an empty TinkerFrame with a primary key metamodel (flat table)
	 * @param headers
	 * @return
	 */
	public static TinkerFrame createPrimKeyTinkerFrame(String[] headers, String[] types) {
		return createPrimKeyTinkerFrame(headers, "tinkerFrame", types);
	}
	
	public static TinkerFrame createPrimKeyTinkerFrame(String[] headers, String dataFrameType, String[] types) {
		
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
	
	/**
	 * Method to generate a Tinker Frame from a CSV file
	 * @param dataStr
	 * @param delimeter
	 * @return
	 */
	public static TinkerFrame generateDataFrameFromFile(String fileLoc, String delimeter, String dataFrameType) {
		if(dataFrameType.equalsIgnoreCase("H2")) {
			return generateH2FrameFromFile(fileLoc, delimeter);
		} else {
			return generateTinkerFrameFromFile(fileLoc, delimeter);
		}
	}
	
	public static TinkerH2Frame generateH2FrameFromFile(String fileLoc, String delimeter) {
		
		// the data string is no longer string but a file name
		CSVFileHelper daHelper = new CSVFileHelper();
		daHelper.setDelimiter(delimeter.charAt(0));
		daHelper.parse(fileLoc);
		String [] headers = daHelper.getHeaders();
		TinkerH2Frame dataFrame = null;
		String [] cells = null;
		String [] types = null;
		H2Builder builder = new H2Builder();
		while((cells = daHelper.getNextRow()) != null) {
			if(types == null) {
				builder.predictRowTypes(cells);
				types = builder.getTypes();
				dataFrame = (TinkerH2Frame)createPrimKeyTinkerFrame(headers, "H2", types);
			}
			
			dataFrame.addRow(cells);
		}
				
		return dataFrame;
	}
	
	public static TinkerFrame generateTinkerFrameFromFile(String fileLoc, String delimeter) {
		// the data string is no longer string but a file name
		CSVFileHelper daHelper = new CSVFileHelper();
		daHelper.setDelimiter(delimeter.charAt(0));
		daHelper.parse(fileLoc);
		String [] headers = daHelper.getHeaders();
		
		TinkerFrame dataFrame = null;
		TinkerCastHelper caster = new TinkerCastHelper();
		
		String[] cells = null;
		String[] types = null;
		Object[] values = null;
		while((cells = daHelper.getNextRow()) != null) {
		
			if(types == null) {
				types = caster.guessTypes(cells);
				dataFrame = (TinkerFrame)createPrimKeyTinkerFrame(headers, "TinkerFrame", types);
			}
			
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
			dataFrame.addRow(row, row);
		}
				
		return dataFrame;
	}
}
