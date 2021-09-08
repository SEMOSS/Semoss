package prerna.algorithm.api;

import prerna.sablecc2.om.PixelDataType;
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
	
	/**
	 * Convert between {@link prerna.sablecc2.om.PixelDataType} and SemossDataType
	 * @param type
	 * @return
	 */
	public static SemossDataType convertFromSemossDataType(PixelDataType type) {
		if(type == PixelDataType.BOOLEAN) {
			return BOOLEAN;
		} else if(type == PixelDataType.CONST_INT) {
			return INT;
		} else if(type == PixelDataType.CONST_DECIMAL) {
			return DOUBLE;
		} else if(type == PixelDataType.CONST_STRING) {
			return STRING;
		} else if(type == PixelDataType.CONST_DATE) {
			return DATE;
		} else if(type == PixelDataType.CONST_TIMESTAMP) {
			return TIMESTAMP;
		}

		return null;
	}
	
	public static PixelDataType convertToPixelDataType(SemossDataType type) {
		if(type == BOOLEAN) {
			return PixelDataType.BOOLEAN;
		} else if(type == INT) {
			return PixelDataType.CONST_INT;
		} else if(type == DOUBLE) {
			return PixelDataType.CONST_DECIMAL;
		} else if(type == STRING) {
			return PixelDataType.CONST_STRING;
		} else if(type == DATE) {
			return PixelDataType.CONST_DATE;
		} else if(type == TIMESTAMP) {
			return PixelDataType.CONST_TIMESTAMP;
		}

		return null;
	}

}
