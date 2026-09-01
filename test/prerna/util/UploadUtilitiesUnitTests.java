package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class UploadUtilitiesUnitTests {

	@Test
	void smssValuesPreserveControlCharactersBackslashesAndUnicode() {
		String value = "First line\nSecond line with \\${PLACEHOLDER}\tand caf\u00e9";
		String serialized = UploadUtilities.escapeSmssPropertyValue(value);

		assertEquals(value, Utility.loadPropertiesString("PROPERTY\t" + serialized).getProperty("PROPERTY"));
	}

	@Test
	void nullSmssValueIsWrittenAsEmpty() {
		assertEquals("", UploadUtilities.escapeSmssPropertyValue(null));
	}
}
