package prerna.reactor.qs.source;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.om.IStringExportProcessor;
import prerna.om.InsightFile;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.AbstractExportTxtReactor;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

public class ConvertRunPixelTaskDataToQueryStructReactor extends AbstractQueryStructReactor {
	
	private static String DELETE_AFTER_INSIGHT_CLOSE = "deleteAfterInsightClose";
	
	public ConvertRunPixelTaskDataToQueryStructReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.INDEX.getKey(), 
				ReactorKeysEnum.FILE_NAME.getKey(), DELETE_AFTER_INSIGHT_CLOSE};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		
		JsonObject jsonObject = JsonParser.parseString(getStringInput()).getAsJsonObject();
		JsonArray pixelReturn = jsonObject.get("pixelReturn").getAsJsonArray();
		int index = getIndex();
		if(index == -1) {
			index = pixelReturn.size()-1;
		}
		JsonObject specificPixelOutput = pixelReturn.get(index).getAsJsonObject();
		JsonObject outputObj = specificPixelOutput.getAsJsonObject("output");
		JsonArray valuesJson = outputObj.getAsJsonObject("data").getAsJsonArray("values");
		JsonArray headersJson = outputObj.getAsJsonObject("data").getAsJsonArray("headers");
		JsonArray headerInfoJson = outputObj.getAsJsonArray("headerInfo");

		int numColumns = headersJson.size();
		String[] headers = new String[numColumns];
		SemossDataType[] types = new SemossDataType[numColumns];
		Map<String, String> typesMapStr = new HashMap<>();
		Map<String, SemossDataType> typesMap = new HashMap<>();
		for(int i = 0; i < numColumns; i++) {
			headers[i] = headersJson.get(i).getAsString();
		}
		for(int i = 0; i < numColumns; i++) {
			JsonObject headerMapJson = headerInfoJson.get(i).getAsJsonObject();
			String dataTypeStr = headerMapJson.get("dataType").getAsString();
			types[i] = SemossDataType.convertStringToDataType(dataTypeStr);
			// also store in map form
			typesMap.put(headers[i], types[i]);
			// store in str format as well for QS
			typesMapStr.put(headers[i], dataTypeStr);
		}
		
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		
		// get a random file name
		String prefixName =  Utility.normalizePath(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()));
		String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "csv");
		// grab file path to write the file
		String insightFolder = this.insight.getInsightFolder();
		File f = new File(insightFolder);
		if(!f.exists()) {
			f.mkdirs();
		}
		String fileLocation = insightFolder + DIR_SEPARATOR + exportName;
		insightFile.setFilePath(fileLocation);
		// if the file location is not defined generate a random path and set
		// location so that the front end will download
		Boolean deleteOnClose = null;
		if(this.keyValue.containsKey(DELETE_AFTER_INSIGHT_CLOSE)) {
			deleteOnClose = Boolean.parseBoolean(this.keyValue.get(DELETE_AFTER_INSIGHT_CLOSE));
		} else {
			deleteOnClose = !this.insight.isSavedInsight();
		}
		insightFile.setDeleteOnInsightClose(deleteOnClose);

		RunPixelTaskDataIterator it = new RunPixelTaskDataIterator(valuesJson, headers, types);
		
		Utility.writeResultToFile(fileLocation, it, typesMap, ",", new IStringExportProcessor() {
			// we need to replace all inner quotes with ""
			@Override
			public String processString(String input) {
				return input.replace("\"", "\"\"");
			}});

		// store the insight file in the insight even if this is not being downloaded
		this.insight.addExportFile(downloadKey, insightFile);
		
		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setDelimiter(',');
		qs.setFilePath(fileLocation);
		qs.setColumnTypes(typesMapStr);
		
		qs.merge(this.qs);
		this.qs = qs;
		return qs;
	}
	
	/**
	 * 
	 */
	public static class RunPixelTaskDataIterator implements Iterator<IHeadersDataRow> {

		private JsonArray valuesJson = null;
		private String[] headers = null;
		private SemossDataType[] types = null;
		
		private int curRow = 0;
		private int numRows = 0;
		private int numColumns = 0;
		
		public RunPixelTaskDataIterator(JsonArray valuesJson, String[] headers, SemossDataType[] types) {
			this.valuesJson = valuesJson;

			this.numRows = valuesJson.size();
			this.numColumns = headers.length;
			this.headers = headers;
			this.types = types;
		}
		
		@Override
		public boolean hasNext() {
			return curRow < numRows;
		}

		@Override
		public IHeadersDataRow next() {
			JsonArray rowJson = this.valuesJson.get(this.curRow).getAsJsonArray();
			Object[] rowValue = new Object[this.numColumns];
			for(int i = 0; i < numColumns; i++) {
				JsonElement element = rowJson.get(i);
				if(element.isJsonNull()) {
					rowValue[i] = null;
				} else if(types[i] == SemossDataType.INT) {
					rowValue[i] = element.getAsInt();
				} else if(types[i] == SemossDataType.DOUBLE) {
					rowValue[i] = element.getAsDouble();
				} else if(types[i] == SemossDataType.BOOLEAN) {
					rowValue[i] = element.getAsBoolean();
				} else if(types[i] == SemossDataType.DATE) {
					rowValue[i] = new SemossDate(element.getAsString(), "yyyy-MM-dd");
				} else if(types[i] == SemossDataType.TIMESTAMP) {
					rowValue[i] = new SemossDate(element.getAsString(), "yyyy-MM-dd HH:mm:ss");
				} else {
					rowValue[i] = element.getAsString();
				}
			}
			
			this.curRow++;
			IHeadersDataRow row = new HeadersDataRow(this.headers, rowValue);
			return row;
		}
		
	}
	
	/**
	 * 
	 * @return
	 */
	private String getStringInput() {
		GenRowStruct valGrs = this.store.getNoun(this.keysToGet[0]);
		if (valGrs != null && !valGrs.isEmpty()) {
			return valGrs.getAllStrValues().get(0);
		}
		
		valGrs = this.store.getNoun(PixelDataType.CONST_STRING.toString());
		if (valGrs != null && !valGrs.isEmpty()) {
			return valGrs.getAllStrValues().get(0);
		}
		
		if(curRow != null && !curRow.isEmpty()) {
			return curRow.getAllStrValues().get(0);
		}
		
		throw new IllegalArgumentException("No valid string input to serialize");
	}
	
	private int getIndex() {
		GenRowStruct valGrs = this.store.getNoun(this.keysToGet[1]);
		if (valGrs != null && !valGrs.isEmpty()) {
			return (int) ((Number) valGrs.getAllNumericColumns().get(0)).intValue();
		}
		
		if(curRow != null && !curRow.isEmpty()) {
			return (int) ((Number) valGrs.getAllNumericColumns().get(0)).intValue();
		}
		
		return -1;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to flush a string in the format of the JSON response for the runPixel endpoint."
				+ " It will grab a specific index of the pixel return (or assume last index) to be in the task data format and write that to file and return a query struct for importing that response into a frame";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(DELETE_AFTER_INSIGHT_CLOSE)) {
			return "boolean if this file should be deleted when the insight is closed during the user session";
		}
		return super.getDescriptionForKey(key);
	}

}
