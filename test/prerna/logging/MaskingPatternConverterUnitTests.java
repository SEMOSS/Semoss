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
package prerna.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import prerna.util.Constants;

class MaskingPatternConverterUnitTests {

	private static final String MASK = Constants.SENSITIVE_INFO_MASK;

	@Test
	void masksAssignmentsWithASecretKey() {
		assertEquals("CreateEngine(OPEN_AI_KEY=" + MASK + ", MODEL=gpt)",
				MaskingPatternConverter.mask("CreateEngine(OPEN_AI_KEY=\"sk-abc\", MODEL=gpt)"));
		assertEquals("smtp SMTP_PASSWORD = " + MASK, MaskingPatternConverter.mask("smtp SMTP_PASSWORD = hunter2"));
	}

	@Test
	void masksJsonPropertiesWithASecretKey() {
		assertEquals("{\"clientSecret\":\"" + MASK + "\",\"clientId\":\"abc\"}",
				MaskingPatternConverter.mask("{\"clientSecret\":\"s3cr3t\",\"clientId\":\"abc\"}"));
	}

	@Test
	void masksQueryParametersWithASecretKeyAndKeepsTheRest() {
		assertEquals("GET /hook?api_key=" + MASK + "&user=bob",
				MaskingPatternConverter.mask("GET /hook?api_key=abc123&user=bob"));
		assertEquals("jdbc:sqlserver://db;user=a?password=" + MASK + "&ssl=true",
				MaskingPatternConverter.mask("jdbc:sqlserver://db;user=a?password=x&ssl=true"));
	}

	@Test
	void leavesValuesUnderOtherKeys() {
		String message = "Opened engine ENGINE=abc with MAX_TOKENS=1000 and \"model\":\"gpt\"";

		assertEquals(message, MaskingPatternConverter.mask(message));
	}

	@Test
	void leavesEmptyMessages() {
		assertNull(MaskingPatternConverter.mask(null));
		assertEquals("", MaskingPatternConverter.mask(""));
	}
}
