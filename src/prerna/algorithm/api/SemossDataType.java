package prerna.algorithm.api;

import prerna.util.Utility;

public enum SemossDataType {

	NUMBER, 
	STRING, 
	DATE;
	
	public static String convertDataTypeToString(SemossDataType type) {
		if(SemossDataType.NUMBER == type) { 
			return "double";
		} else if(SemossDataType.DATE == type) {
			return "date";
		} else {
			return "varchar(800)";
		}
	}
	
	public static SemossDataType convertStringToDataType(String dataType) {
		if(dataType == null) {
			return null;
		}
		if(Utility.isNumericType(dataType)) {
			return SemossDataType.NUMBER;
		} else if(Utility.isDateType(dataType)) {
			return SemossDataType.DATE;
		} else if(Utility.isStringType(dataType)) {
			return SemossDataType.STRING;
		} else {
			return SemossDataType.STRING;
		}
	}
	
}
