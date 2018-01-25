package prerna.algorithm.api;

import prerna.util.Utility;

public enum SemossDataType {

	INT,
	DOUBLE,
	STRING, 
	DATE,
	TIMESTAMP;
	
	public static String convertDataTypeToString(SemossDataType type) {
		if(SemossDataType.INT == type) { 
			return "int";
		} else if(SemossDataType.DOUBLE == type) { 
			return "double";
		} else if(SemossDataType.DATE == type) {
			return "date";
		} else if(SemossDataType.TIMESTAMP == type) {
			return "timestamp";
		} else {
			return "varchar(800)";
		}
	}
	
	public static SemossDataType convertStringToDataType(String dataType) {
		if(dataType == null) {
			return null;
		}
		
		if(Utility.isIntegerType(dataType)) {
			return SemossDataType.INT;
		} else if(Utility.isDoubleType(dataType)) {
			return SemossDataType.DOUBLE;
		} else if(Utility.isDateType(dataType)) {
			return SemossDataType.DATE;
		} else {
			return SemossDataType.STRING;
		}
	}
	
}
