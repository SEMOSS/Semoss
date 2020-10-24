package prerna.algorithm.api;

import prerna.util.Utility;

public enum SemossDataType {

	BOOLEAN,
	INT,
	DOUBLE,
	STRING, 
	DATE,
	TIMESTAMP,
	FACTOR;
	
	public static boolean isNotString(SemossDataType type) {
		if(type == STRING || type == FACTOR) {
			return false;
		}
		return true;
	}
	
	public static boolean isNotString(String typeStr) {
		SemossDataType type = convertStringToDataType(typeStr);
		if(type == STRING || type == FACTOR) {
			return false;
		}
		return true;
	}
	
	public static SemossDataType convertStringToDataType(String dataType) {
		if(dataType == null) {
			return null;
		}
		if(dataType.startsWith("TYPE:")) {
			dataType = dataType.substring("TYPE:".length());
		}
		
		if(Utility.isBoolean(dataType)) {
			return SemossDataType.BOOLEAN;
		} else if(Utility.isIntegerType(dataType)) {
			return SemossDataType.INT;
		} else if(Utility.isDoubleType(dataType)) {
			return SemossDataType.DOUBLE;
		} else if(Utility.isDateType(dataType)) {
			return SemossDataType.DATE;
		} else if(Utility.isTimeStamp(dataType)) {
			return SemossDataType.TIMESTAMP;
		} else if (Utility.isFactorType(dataType)) {
			return SemossDataType.FACTOR;
		} else {
			return SemossDataType.STRING;
		}
	}
}
