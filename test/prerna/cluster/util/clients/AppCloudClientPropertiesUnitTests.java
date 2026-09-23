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
package prerna.cluster.util.clients;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mockStatic;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.util.Utility;

class AppCloudClientPropertiesUnitTests {

	private AppCloudClientProperties props;
	private Map<String, String> envMap;

	@BeforeEach
	void setUp() throws Exception {
		props = AppCloudClientProperties.build();
		envMap = new HashMap<>();
		Field envField = AppCloudClientProperties.class.getDeclaredField("env");
		envField.setAccessible(true);
		envField.set(props, envMap);
	}

	@Test
	void testEnvExactKeyMatch() {
		envMap.put("MY_KEY", "myValue");
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			String result = props.get("MY_KEY");
			assertEquals("myValue", result);
		}
	}

	@Test
	void testEnvUpperCaseKeyMatch() {
		envMap.put("MY_KEY", "upperVal");
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			String result = props.get("my_key");
			assertEquals("upperVal", result);
		}
	}

	@Test
	void testEnvLowerCaseKeyMatch() {
		envMap.put("my_key", "lowerVal");
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			String result = props.get("MY_KEY");
			assertEquals("lowerVal", result);
		}
	}

	@Test
	void testFallsThroughToDIHelper() {
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			ms.when(() -> Utility.getDIHelperProperty("MY_KEY")).thenReturn("diValue");
			String result = props.get("MY_KEY");
			assertEquals("diValue", result);
		}
	}

	@Test
	void testDIHelperExactKeyMatch() {
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			ms.when(() -> Utility.getDIHelperProperty("myKey")).thenReturn("exactDI");
			String result = props.get("myKey");
			assertEquals("exactDI", result);
		}
	}

	@Test
	void testDIHelperUpperCaseKeyMatch() {
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			ms.when(() -> Utility.getDIHelperProperty("MY_KEY")).thenReturn("upperDI");
			String result = props.get("my_key");
			assertEquals("upperDI", result);
		}
	}

	@Test
	void testDIHelperLowerCaseKeyMatch() {
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			ms.when(() -> Utility.getDIHelperProperty("my_key")).thenReturn("lowerDI");
			String result = props.get("MY_KEY");
			assertEquals("lowerDI", result);
		}
	}

	@Test
	void testReturnsNullWhenNothingFound() {
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			String result = props.get("NONEXISTENT");
			assertNull(result);
		}
	}

	@Test
	void testEnvValueWithWhitespaceIsTrimmed() {
		envMap.put("TRIM_KEY", "  trimmed  ");
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			String result = props.get("TRIM_KEY");
			assertEquals("trimmed", result);
		}
	}

	@Test
	void testEmptyEnvValueFallsThrough() {
		envMap.put("EMPTY_KEY", "");
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			ms.when(() -> Utility.getDIHelperProperty("EMPTY_KEY")).thenReturn("diValue");
			String result = props.get("EMPTY_KEY");
			assertEquals("diValue", result);
		}
	}

	@Test
	void testWhitespaceOnlyEnvValueFallsThrough() {
		envMap.put("WS_KEY", "   ");
		try (MockedStatic<Utility> ms = mockStatic(Utility.class)) {
			ms.when(() -> Utility.getDIHelperProperty("WS_KEY")).thenReturn("fromDI");
			String result = props.get("WS_KEY");
			assertEquals("fromDI", result);
		}
	}

}
