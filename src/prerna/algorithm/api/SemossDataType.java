package prerna.algorithm.api;

import prerna.sablecc2.om.PixelDataType;
import prerna.util.Utility;

/**
 * Enumeration that defines the core data types supported by the Semoss system.
 * This enum provides a standardized way to represent different data types and includes
 * utility methods for type conversion, validation, and compatibility checking.
 * 
 * <p>
 * The SemossDataType enum supports common data types including primitives, strings,
 * dates, timestamps, and categorical factors. It provides conversion methods between
 * string representations, {@link PixelDataType} values, and includes utility methods
 * for type checking and validation.
 * </p>
 * 
 * @see {@link PixelDataType} for pixel-level data type representations
 * @see {@link prerna.util.Utility} for type validation utilities
 */
public enum SemossDataType {

	/** Boolean data type for true/false values. */
	BOOLEAN,
	
	/** Integer data type for whole numbers. */
	INT,
	
	/** Double precision floating point data type for decimal numbers. */
	DOUBLE,
	
	/** String data type for text values. */
	STRING, 
	
	/** Date data type for calendar dates without time information. */
	DATE,
	
	/** Timestamp data type for date and time values with precision. */
	TIMESTAMP,
	
	/** Factor data type for categorical variables with discrete levels. */
	FACTOR;
	
	/**
	 * Determines if the specified data type is not a string-like type.
	 * String-like types include {@link #STRING} and {@link #FACTOR}.
	 *
	 * @param type The {@link SemossDataType} to check.
	 * @return False if the type is STRING or FACTOR, true otherwise.
	 */
	public static boolean isNotString(SemossDataType type) {
		if(type == STRING || type == FACTOR) {
			return false;
		}
		return true;
	}
	
	/**
	 * Determines if the specified data type string is not a string-like type.
	 * String-like types include STRING and FACTOR representations.
	 *
	 * @param typeStr The string representation of the data type to check.
	 * @return False if the type represents STRING or FACTOR, true otherwise.
	 */
	public static boolean isNotString(String typeStr) {
		SemossDataType type = convertStringToDataType(typeStr);
		if(type == STRING || type == FACTOR) {
			return false;
		}
		return true;
	}
	
	/**
	 * Converts a string representation to the corresponding Semoss data type.
	 * This method handles various string formats and uses utility methods to
	 * determine the appropriate data type based on the input string.
	 *
	 * @param dataType The string representation of the data type to convert.
	 * @return The corresponding {@link SemossDataType}, or null if the input is null.
	 *         Defaults to {@link #STRING} if the type cannot be determined.
	 */
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
	 * Converts from {@link PixelDataType} to the corresponding {@link SemossDataType}.
	 * This method provides a mapping between pixel-level data types and Semoss data types.
	 *
	 * @param type The {@link PixelDataType} to convert.
	 * @return The corresponding {@link SemossDataType}, or null if no mapping exists.
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
	
	/**
	 * Converts from {@link SemossDataType} to the corresponding {@link PixelDataType}.
	 * This method provides a mapping from Semoss data types to pixel-level data types.
	 *
	 * @param type The {@link SemossDataType} to convert.
	 * @return The corresponding {@link PixelDataType}, or null if no mapping exists.
	 */
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
	
	/**
	 * Converts a {@link SemossDataType} to its string representation.
	 * Note that both {@link #STRING} and {@link #FACTOR} types are converted to "STRING".
	 *
	 * @param dataType The {@link SemossDataType} to convert.
	 * @return The string representation of the data type, or null if the input is null or unmapped.
	 */
	public static String convertDataTypeToString(SemossDataType dataType) {
		if(dataType == null) {
			return null;
		}

		if(dataType == SemossDataType.STRING || dataType == SemossDataType.FACTOR) {
			return "STRING";
		} else if(dataType == SemossDataType.INT) {
			return "INT";
		} else if(dataType == SemossDataType.DOUBLE) {
			return "DOUBLE";
		} else if(dataType == SemossDataType.DATE) {
			return "DATE";
		} else if(dataType == SemossDataType.TIMESTAMP) {
			return "TIMESTAMP";
		} else if(dataType == SemossDataType.BOOLEAN) {
			return "BOOLEAN";
		}
		
		return null;
	}
	
	/**
	 * Converts an array of {@link SemossDataType} values to their string representations.
	 * This method applies {@link #convertDataTypeToString(SemossDataType)} to each element
	 * in the input array.
	 *
	 * @param dataTypes Array of {@link SemossDataType} values to convert.
	 * @return Array of string representations corresponding to the input data types, 
	 *         or null if the input array is null.
	 */
	public static String[] convertSemossDataTypeArrToStringArr(SemossDataType[] dataTypes) {
		if(dataTypes == null) {
			return null;
		}
		
		String[] retArr = new String[dataTypes.length];
		for(int i = 0; i < dataTypes.length; i++) {
			retArr[i] = convertDataTypeToString(dataTypes[i]);
		}
		
		return retArr;
	}
}
