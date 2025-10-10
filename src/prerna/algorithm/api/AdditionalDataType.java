package prerna.algorithm.api;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Enumeration that defines additional data types used for more specific data formatting and validation.
 * This enum provides specialized data types that extend beyond basic primitive types to include
 * geographic, financial, communication, and specialized numerical formats.
 * 
 * <p>
 * Each additional data type includes a descriptive explanation of its purpose and typical usage patterns.
 * This enumeration is used throughout the system to provide enhanced data type recognition and formatting
 * capabilities for data processing and display.
 * </p>
 * 
 * @see {@link #convertStringToAdtlDataType(String)} for converting string representations to enum values
 * @see {@link #getHelp()} for retrieving descriptions of all data types
 */
public enum AdditionalDataType {

	/** Currency formats are used for general monetary values. */
	CURRENCY("Currency formats are used for general monetary values."),
	
	/** Latitude formats are used to pinpoint a location in either the north or south hemisphere and are represented in degrees. */
	LATITUDE("Latitude formats are used to pinpoint a location in either the north or south hemisphere and are represented in degrees."), 
	
	/** Longitude formats are used to pinpoint a location east or west of the meridian at Greenwich, England and are represented in degrees. */
	LONGITUDE("Longitude formats are used to pinpoint a location east or west of the meridian at Greenwich, England and are represented in degrees."),
	
	/** Country formats are used to signify that the text is a Country in the world. */
	COUNTRY("Country formats are used to signify that the text is a Country in the world."),
	
	/** City formats are used to signify that the text is a City in a State, Region, Province, or Country. */
	CITY("City formats are used to signify that the text is a City in a State, Region, Province, or Country."),
	
	/** State formats are used to signify that the text is a State in a Country or Region. */
	STATE("State formats are used to signify that the text is a State in a Country or Region. "),
	
	/** Zipcode formats are a series of five digits representing a postal code used by the United States Postal Service to identify a location. */
	ZIPCODE("Zipcode formats are a series of five digits representing a postal code used by the United States Postal Service to idenitfy a location."),
	
	/** Complete zipcode formats are series of five plus four digits representing a postal code used by the United States Postal Service to identify a location. */
	FULL_ZIPCODE("Complete zipcode formats are series of five plus four digits representing a postal code used by the United States Postal Service to idenitfy a location."),
	
	/** Phone number formats are typically 10 digits and are typically a separated set of numbers in format (XXX) XXX-XXXX. */
	PHONE_NUMBER("Phone number formats are typically 10 digits and are typically a separated set of numbers in format (XXX) XXX-XXXX."),
	
	/** Social Security formats are a nine-digit number issued to persons within the U.S., used to uniquely identify people. */
	SOCIAL_SECURITY_NUMBER("Social Security formats are a nine-digit number issued to persons within the U.S., used to uniquely identify people."),
	
	/** Accounting formats line up the currency symbols and decimal points in a column. */
	ACCOUNTING("Accounting formats line up the currency symbols and decimal points in a column "),
	
	/** Scientific notation formats are a way of expressing numbers that are too big or too small to be conveniently written in decimal form. */
	SCIENTIFIC("Scientific notation formats are a way of expressing numbers that are too big or too small to be conveniently written in decimal form."),
	
	/** Percentage formats multiply the value by 100 and displays the result with a percent symbol. */
	PERCENT("Percentage formats multiply the value by 100 and displays the result with a percent symbol."),
	
	/** Fraction formats are a numerical quantity that is not a whole number and are used to describe a segment of a number. */
	FRACTION("Fraction formats are a numerical quantity that is not a whole number and are used to describe a segment of a number.");

	/** Map for converting string representations to enum values. */
	private static final Map<String, AdditionalDataType> stringToEnum = new HashMap<String, AdditionalDataType>();
	
	/** Map containing descriptions for each additional data type. */
	private static final Map<AdditionalDataType, String> mapOfEnumDescriptions = new TreeMap<AdditionalDataType, String>();
	
	/** The descriptive text explaining the purpose and usage of this data type. */
	private String description;

	/**
	 * Constructs an additional data type with the specified description.
	 *
	 * @param description The descriptive text explaining the purpose and usage of this data type.
	 */
	private AdditionalDataType(String description) {
		this.description = description;
	}

	static {
		for(AdditionalDataType adt : values()) {
			stringToEnum.put(adt.toString(), adt);
		}

		for (AdditionalDataType adt : AdditionalDataType.values()) {
			mapOfEnumDescriptions.put(adt, adt.description);
		}
	}

	/**
	 * Converts a string representation to the corresponding additional data type.
	 *
	 * @param type The string representation of the additional data type.
	 * @return The {@link AdditionalDataType} enum value corresponding to the input string, or null if not found.
	 */
	public static AdditionalDataType convertStringToAdtlDataType(String type) {
		return stringToEnum.get(type);
	}

	/**
	 * Returns a map of all additional data types and their descriptions.
	 *
	 * @return A map where keys are {@link AdditionalDataType} values and values are their corresponding descriptions.
	 */
	public static Map<AdditionalDataType, String> getHelp() {
		return mapOfEnumDescriptions;
	}
}
