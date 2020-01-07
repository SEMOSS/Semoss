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
	
	// THIS IS JUST WRONG!
	// 1) IF YOU NEED A STRING, JUST + "" THE ENUM
	// 2) DIFFERENT SQL HAVE DIFFERNT TYPES
//	public static String convertDataTypeToString(SemossDataType type) {
//		if(SemossDataType.BOOLEAN == type) { 
//			return "boolean";
//		} else if(SemossDataType.INT == type) { 
//			return "int";
//		} else if(SemossDataType.DOUBLE == type) { 
//			return "double";
//		} else if(SemossDataType.DATE == type) {
//			return "date";
//		} else if(SemossDataType.TIMESTAMP == type) {
//			return "timestamp";
//		} else if (SemossDataType.FACTOR == type) {
//			return "factor";
//		} else {
//			return "varchar(800)";
//		}
//	}
	
}
