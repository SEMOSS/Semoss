package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import prerna.poi.main.helper.XLFileHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetXLSchemaReactor extends AbstractReactor {
	public GetXLSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.toString(), ReactorKeysEnum.SHEET_NAME.toString() };
	}

	@Override
	public NounMetadata execute() {
		String filePath = getFilePath();
		String sheetName = getSheetName();
		HashMap<String, Object> ret = new HashMap<String, Object>();
		XLFileHelper helper = new XLFileHelper();
		helper.parse(filePath);
		ret.put("databaseName", FilenameUtils.getName(filePath).replace(".", "_"));

		// store the suggested data types
		Map<String, Map<String, String>> dataTypes = new Hashtable<String, Map<String, String>>();
		Map<String, String> sheetDataMap = new LinkedHashMap<String, String>();
		String[] columnHeaders = helper.getHeaders(sheetName);
		String[] predicatedDataTypes = helper.predictRowTypes(sheetName);

		HashMap<String, List<String>> relationshipMap = new HashMap<String, List<String>>();
		for (String concept : columnHeaders) {
			relationshipMap.put(concept, new ArrayList<String>());
		}

		ret.put("relationships", relationshipMap);

		dataTypes.put(sheetName, sheetDataMap);

		HashMap<String, HashMap> tableDetails = new HashMap<String, HashMap>();
		for (int i = 0; i < columnHeaders.length; i++) {
			HashMap<String, String> colDetails = new HashMap<String, String>();
			colDetails.put("name", columnHeaders[i]);
			String dataType = Utility.getCleanDataType(predicatedDataTypes[i]);
			colDetails.put("type", dataType);
			tableDetails.put(columnHeaders[i], colDetails);
		}

		ret.put("tables", tableDetails);
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
		String schema = null;
		try {
			schema = ow.writeValueAsString(ret);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return new NounMetadata(schema, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

	private String getSheetName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.SHEET_NAME.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.SHEET_NAME.toString());
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
