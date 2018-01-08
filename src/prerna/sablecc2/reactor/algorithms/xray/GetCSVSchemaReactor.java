package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetCSVSchemaReactor extends AbstractReactor{
	public GetCSVSchemaReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.toString(), ReactorKeysEnum.DELIMITER.toString()};
	}
	@Override
	public NounMetadata execute() {
		String filePath = getFilePath();
		String delimiter = getDelimiter();
		CSVFileHelper cv = new CSVFileHelper();
		cv.setDelimiter(delimiter.charAt(0));
		cv.parse(filePath);
		String[] headers = cv.getAllCSVHeaders();
		String[] types = cv.predictTypes();

		HashMap<String, Object> ret = new HashMap<String, Object>();
		//generate db name
		String[] parts = filePath.split("\\\\");
		String dbName = parts[parts.length-1].replace(".", "_");
		// C:\\..\\file.csv -> file_csv
		ret.put("databaseName", dbName);

		//construct empty relationship map (assuming flat table)
		HashMap<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		for(String concept : headers) {
			relationshipMap.put(concept, new ArrayList<String>()); //return empty list for FE
		}

		ret.put("relationships", relationshipMap);

		//add column details
		//since it's a flat table we don't need to worry about concept/property relationships
		HashMap<String, HashMap> tableDetails = new HashMap<String, HashMap>();
		for(int i = 0; i < headers.length; i++) {
			HashMap<String, String> colDetails = new HashMap<String, String>();
			colDetails.put("name", headers[i]);
			String dataType = SemossDataType.convertStringToDataType(types[i]).toString();
			colDetails.put("type", dataType);
			tableDetails.put(headers[i], colDetails);
		}

		ret.put("tables", tableDetails);

		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		String schema = null;
		try {
			schema = ow.writeValueAsString(ret);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(schema, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}
	
	private String getDelimiter() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.DELIMITER.toString());
		if (grs != null && !grs.isEmpty()) {
			String file = grs.getNoun(0).getValue() + "";
			if (file.length() > 0) {
				return file;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DELIMITER.toString());
	}

	private String getFilePath() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.FILE_PATH.toString());
		if (grs != null && !grs.isEmpty()) {
			String file = grs.getNoun(0).getValue() + "";
			if (file.length() > 0) {
				return file;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.FILE_PATH.toString());
	}

}
