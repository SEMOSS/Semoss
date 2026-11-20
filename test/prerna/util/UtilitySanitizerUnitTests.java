/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Exercises ESAPI.encoder() through Utility's sanitizer methods. Also serves
 * as a behavioral check that ESAPI.properties loads correctly without
 * Encryptor.MasterKey/MasterSalt set, since ESAPI.encoder() (unlike
 * ESAPI.encryptor(), which SEMOSS never calls) must keep working regardless.
 */
class UtilitySanitizerUnitTests {

	@Test
	void inputSanitizerReturnsNullForNull() {
		assertNull(Utility.inputSanitizer(null));
	}

	@Test
	void inputSanitizerStripsScriptTags() {
		String result = Utility.inputSanitizer("<script>alert(1)</script>hello");
		assertTrue(!result.contains("<script>"), "Expected script tag to be stripped, got: " + result);
		assertTrue(result.contains("hello"));
	}

	@Test
	void inputSanitizerPreservesPlainText() {
		assertEquals("just plain text", Utility.inputSanitizer("just plain text"));
	}

	@Test
	void inputSQLSanitizerReturnsNullForNull() {
		assertNull(Utility.inputSQLSanitizer(null));
	}

	@Test
	void inputSQLSanitizerEscapesSingleQuote() {
		String result = Utility.inputSQLSanitizer("O'Brien");
		assertTrue(result.contains("O") && result.contains("Brien") && !result.equals("O'Brien"),
				"Expected the quote to be escaped, got: " + result);
	}

	@Test
	void inputSQLSanitizerPreservesPlainText() {
		assertEquals("just plain text", Utility.inputSQLSanitizer("just plain text"));
	}
}
