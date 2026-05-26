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
package prerna.sablecc2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class PixelUtilityUnitTests {

	@Test
	void decodePixelStringLiteral_decodesCommonEscapes() {
		String input = "\"line1\\nline2\\tindent\\rreturn\\bback\\fform\\\\slash\\\"dq\\'sq\\/\"";
		String expected = "line1\nline2\tindent\rreturn\bback\fform\\slash\"dq'sq/";
		assertEquals(expected, PixelUtility.decodePixelStringLiteral(input));
	}

	@Test
	void decodePixelStringLiteral_decodesUnicodeEscapes() {
		String input = "\"hello " + "\\" + "u263A\"";
		assertEquals("hello " + (char) 0x263A, PixelUtility.decodePixelStringLiteral(input));
	}

	@Test
	void decodeEscapedString_preservesUnknownEscapes() {
		assertEquals("path\\q\\u12G4", PixelUtility.decodeEscapedString("path\\q\\u12G4"));
	}

	@Test
	void decodeEscapedString_handlesNullAndPlainText() {
		assertNull(PixelUtility.decodeEscapedString(null));
		assertEquals("plain-text", PixelUtility.decodeEscapedString("plain-text"));
	}
}
