/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;
import prerna.algorithm.api.AdditionalDataType;

public class AdditionalDataTypeUnitTests {

	@Test
	void testAdditionalDataType() {
		AdditionalDataType c = AdditionalDataType.convertStringToAdtlDataType("CURRENCY");
		assertEquals(AdditionalDataType.CURRENCY, c);
	}

	@Test
	void testGetHelp() {
		Map<AdditionalDataType, String> map = AdditionalDataType.getHelp();
		assertEquals("Currency formats are used for general monetary values.", map.get(AdditionalDataType.CURRENCY));
		assertEquals(
				"Latitude formats are used to pinpoint a location in either the north or south hemisphere and are represented in degrees.",
				map.get(AdditionalDataType.LATITUDE));
		assertEquals(
				"Longitude formats are used to pinpoint a location east or west of the meridian at Greenwich, England and are represented in degrees.",
				map.get(AdditionalDataType.LONGITUDE));
		assertEquals("Country formats are used to signify that the text is a Country in the world.",
				map.get(AdditionalDataType.COUNTRY));
		assertEquals(
				"City formats are used to signify that the text is a City in a State, Region, Province, or Country.",
				map.get(AdditionalDataType.CITY));
		assertEquals("State formats are used to signify that the text is a State in a Country or Region. ",
				map.get(AdditionalDataType.STATE));
		assertEquals(
				"Zipcode formats are a series of five digits representing a postal code used by the United States Postal Service to idenitfy a location.",
				map.get(AdditionalDataType.ZIPCODE));
		assertEquals(
				"Complete zipcode formats are series of five plus four digits representing a postal code used by the United States Postal Service to idenitfy a location.",
				map.get(AdditionalDataType.FULL_ZIPCODE));
		assertEquals(
				"Phone number formats are typically 10 digits and are typically a separated set of numbers in format (XXX) XXX-XXXX.",
				map.get(AdditionalDataType.PHONE_NUMBER));
		assertEquals(
				"Social Security formats are a nine-digit number issued to persons within the U.S., used to uniquely identify people.",
				map.get(AdditionalDataType.SOCIAL_SECURITY_NUMBER));
		assertEquals("Accounting formats line up the currency symbols and decimal points in a column ",
				map.get(AdditionalDataType.ACCOUNTING));
		assertEquals(
				"Scientific notation formats are a way of expressing numbers that are too big or too small to be conveniently written in decimal form.",
				map.get(AdditionalDataType.SCIENTIFIC));
		assertEquals("Percentage formats multiply the value by 100 and displays the result with a percent symbol.",
				map.get(AdditionalDataType.PERCENT));
		assertEquals(
				"Fraction formats are a numerical quantity that is not a whole number and are used to describe a segment of a number.",
				map.get(AdditionalDataType.FRACTION));
		assertEquals(14, map.size());
	}
}
