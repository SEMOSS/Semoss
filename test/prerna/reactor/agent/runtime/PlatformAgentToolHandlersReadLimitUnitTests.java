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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.util.Constants;
import prerna.util.Utility;

class PlatformAgentToolHandlersReadLimitUnitTests {

	private static final long MB = 1024L * 1024L;

	private static long limitWith(String setting) {
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			utility.when(() -> Utility.getDIHelperProperty(anyString())).thenReturn(null);
			utility.when(() -> Utility.getDIHelperProperty(Constants.DOCUMENT_READ_MAX_FILE_MB)).thenReturn(setting);
			return PlatformAgentToolHandlers.documentReadMaxFileBytes();
		}
	}

	@Test
	void defaultsTo100Mb() {
		assertEquals(100 * MB, limitWith(null));
		assertEquals(100 * MB, limitWith("  "));
	}

	@Test
	void rdfMapSettingOverridesTheDefault() {
		assertEquals(250 * MB, limitWith(" 250 "));
	}

	@Test
	void anInvalidOrTooSmallSettingFallsBack() {
		assertEquals(100 * MB, limitWith("lots"));
		assertEquals(MB, limitWith("0"));
	}
}
