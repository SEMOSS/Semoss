package prerna.poi.main.helper;

import java.util.HashMap;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;

public class FileHelperUtil {

	private FileHelperUtil() {
		
	}
	
	/**
	 * Convenience method to get the 2 maps we usually use within CsvQueryStruct
	 * @param headers
	 * @param predictions
	 * @return
	 */
	public static Map[] generateDataTypeMapsFromPrediction(String[] headers, Object[][] predictions) {
		Map[] retArray = new Map[2];
		Map<String, String> dataTypeMap = new HashMap<String, String>();
		Map<String, String> additionalDataTypeMap = new HashMap<String, String>();
		retArray[0] = dataTypeMap;
		retArray[1] = additionalDataTypeMap;
		
		int numHeaders = headers.length;
		for(int i = 0; i < numHeaders; i++) {
			Object[] pred = predictions[i];
			SemossDataType type = (SemossDataType) pred[0];
			dataTypeMap.put(headers[i], type.toString());
			if(pred[1] != null) {
				additionalDataTypeMap.put(headers[i], pred[1] + "");
			}
		}
		return retArray;
	}
	
	/**
	 * Convenience method to just return the data types
	 * @param headers
	 * @param predictions
	 * @return
	 */
	public static String[] generateDataTypeArrayFromPrediction(Object[][] predictions) {
		int numHeaders = predictions.length;
		String[] returnTypes = new String[numHeaders];
		
		for(int i = 0; i < numHeaders; i++) {
			Object[] pred = predictions[i];
			SemossDataType type = (SemossDataType) pred[0];
			returnTypes[i] = type.toString();
		}
		return returnTypes;
	}
	
}
