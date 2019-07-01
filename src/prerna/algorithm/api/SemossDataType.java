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
	
	public static String convertDataTypeToString(SemossDataType type) {
		if(SemossDataType.BOOLEAN == type) { 
			return "boolean";
		} else if(SemossDataType.INT == type) { 
			return "int";
		} else if(SemossDataType.DOUBLE == type) { 
			return "double";
		} else if(SemossDataType.DATE == type) {
			return "date";
		} else if(SemossDataType.TIMESTAMP == type) {
			return "timestamp";
		} else if (SemossDataType.FACTOR == type) {
			return "factor";
		} else {
			return "varchar(800)";
		}
	}
	
	public static SemossDataType convertStringToDataType(String dataType) {
		if(dataType == null) {
			return null;
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
