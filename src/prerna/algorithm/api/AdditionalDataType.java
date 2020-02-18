package prerna.algorithm.api;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public enum AdditionalDataType {

	CURRENCY("Currency formats are used for general monetary values."),
	LATITUDE("Latitude formats are used to pinpoint a location in either the north or south hemisphere and are represented in degrees."), 
	LONGITUDE("Longitude formats are used to pinpoint a location east or west of the meridian at Greenwich, England and are represented in degrees."),
	COUNTRY("Country formats are used to signify that the text is a Country in the world."),
	CITY("City formats are used to signify that the text is a City in a State, Region, Province, or Country."),
	STATE("State formats are used to signify that the text is a State in a Country or Region. "),
	ZIPCODE("Zipcode formats are a series of five digits representing a postal code used by the United States Postal Service to idenitfy a location."),
	FULL_ZIPCODE("Complete zipcode formats are series of five plus four digits representing a postal code used by the United States Postal Service to idenitfy a location."),
	PHONE_NUMBER("Phone number formats are typically 10 digits and are typically a separated set of numbers in format (XXX) XXX-XXXX."),
	SOCIAL_SECURITY_NUMBER("Social Security formats are a nine-digit number issued to persons within the U.S., used to uniquely identify people."),
	ACCOUNTING("Accounting formats line up the currency symbols and decimal points in a column "),
	SCIENTIFIC("Scientific notation formats are a way of expressing numbers that are too big or too small to be conveniently written in decimal form."),
	PERCENT("Percentage formats multiply the value by 100 and displays the result with a percent symbol."),
	FRACTION("Fraction formats are a numerical quantity that is not a whole number and are used to describe a segment of a number.");

	private static final Map<String, AdditionalDataType> stringToEnum = new HashMap<String, AdditionalDataType>();
	private static final Map<AdditionalDataType, String> mapOfEnumDescriptions = new TreeMap<AdditionalDataType, String>();
	private String description;

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

	public static AdditionalDataType convertStringToAdtlDataType(String type) {
		return stringToEnum.get(type);
	}

	public static Map<AdditionalDataType, String> getHelp() {
		return mapOfEnumDescriptions;
	}
}
