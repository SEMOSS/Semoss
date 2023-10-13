package prerna.reactor.algorithms.xray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCSVSchemaReactor extends AbstractReactor {
	
	public GetCSVSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.DELIMITER.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		//get inputs
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if(filePath == null) {
			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.FILE_PATH.getKey());
		}
		String delimiter = this.keyValue.get(this.keysToGet[1]);
		if(delimiter == null) {
			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DELIMITER.getKey());
		}
		
		CSVFileHelper cv = new CSVFileHelper();
		cv.setDelimiter(delimiter.charAt(0));
		cv.parse(filePath);
		String[] headers = cv.getAllCSVHeaders();
		Object[][] typePredictions = cv.predictTypes();

		Map<String, Object> ret = new HashMap<String, Object>();
		// generate db name
		String[] parts = filePath.split("\\\\");
		String dbName = parts[parts.length - 1].replace(".", "_");
		// C:\\..\\file.csv -> file_csv
		ret.put("databaseName", dbName);

		// construct empty relationship map (assuming flat table)
		Map<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		for(String concept : headers) {
			relationshipMap.put(concept, new ArrayList<String>());
		}
		ret.put("relationships", relationshipMap);

		// add column details
		// since it's a flat table we don't need to worry about concept/property
		// relationships
		Map<String, Map> tableDetails = new HashMap<String, Map>();
		for (int i = 0; i < headers.length; i++) {
			Map<String, String> colDetails = new HashMap<String, String>();
			colDetails.put("name", headers[i]);
			// index 1 is the data type as an enum
			colDetails.put("type", typePredictions[i][1].toString());
			tableDetails.put(headers[i], colDetails);
		}

		ret.put("tables", tableDetails);
		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}





}
