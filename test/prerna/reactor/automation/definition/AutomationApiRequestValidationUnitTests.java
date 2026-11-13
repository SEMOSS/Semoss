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
package prerna.reactor.automation.definition;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.automation.AutomationConstants;

public class AutomationApiRequestValidationUnitTests {

	private static Map<String, Object> config() {
		Map<String, Object> config = new LinkedHashMap<>();
		config.put(AutomationConstants.CONFIG_API_METHOD, "GET");
		config.put(AutomationConstants.CONFIG_API_URL, "https://api.example.com/items");
		config.put(AutomationConstants.CONFIG_API_TIMEOUT_SECONDS, 30);
		return config;
	}

	private static void validate(Map<String, Object> config) {
		AutomationDefinitionValidator.validateApiRequestConfig("call", config);
	}

	@Test
	public void acceptsAMinimalGet() {
		assertDoesNotThrow(() -> validate(config()));
	}

	@Test
	public void acceptsEveryMethod() {
		for (String method : new String[] { "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "patch" }) {
			Map<String, Object> config = config();
			config.put(AutomationConstants.CONFIG_API_METHOD, method);
			assertDoesNotThrow(() -> validate(config), method);
		}
	}

	@Test
	public void rejectsUnknownMethod() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_METHOD, "TRACE");
		assertThrows(IllegalArgumentException.class, () -> validate(config));
	}

	@Test
	public void rejectsUrlWithoutSchemeWhenNoConnection() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_URL, "/items");
		assertThrows(IllegalArgumentException.class, () -> validate(config));
	}

	@Test
	public void acceptsRelativePathWithConnection() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_URL, "/items/${id}");
		config.put(AutomationConstants.CONFIG_API_CONNECTION_ID, "engine-1");
		assertDoesNotThrow(() -> validate(config));
	}

	@Test
	public void rejectsHostWrittenAsReference() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_URL, "https://${host}/items");
		assertThrows(IllegalArgumentException.class, () -> validate(config));
	}

	@Test
	public void acceptsReferenceInPath() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_URL, "https://api.example.com/items/${item.id}");
		assertDoesNotThrow(() -> validate(config));
	}

	@Test
	public void rejectsTypedCredentialHeader() {
		for (String name : new String[] { "Authorization", "x-api-key", "Cookie" }) {
			Map<String, Object> config = config();
			config.put(AutomationConstants.CONFIG_API_HEADERS, Map.of(name, "abc123"));
			assertThrows(IllegalArgumentException.class, () -> validate(config), name);
		}
	}

	@Test
	public void acceptsReferencedCredentialHeader() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_HEADERS,
				Map.of("Authorization", "Bearer ${token}", "Accept", "application/json"));
		assertDoesNotThrow(() -> validate(config));
	}

	@Test
	public void rejectsBodyOnGetAndHead() {
		for (String method : new String[] { "GET", "HEAD" }) {
			Map<String, Object> config = config();
			config.put(AutomationConstants.CONFIG_API_METHOD, method);
			config.put(AutomationConstants.CONFIG_API_BODY_TYPE, AutomationConstants.API_BODY_JSON);
			assertThrows(IllegalArgumentException.class, () -> validate(config), method);
		}
	}

	@Test
	public void rejectsUnknownBodyType() {
		Map<String, Object> config = config();
		config.put(AutomationConstants.CONFIG_API_METHOD, "POST");
		config.put(AutomationConstants.CONFIG_API_BODY_TYPE, "multipart");
		assertThrows(IllegalArgumentException.class, () -> validate(config));
	}

	@Test
	public void rejectsTimeoutOutsideBounds() {
		for (int seconds : new int[] { 0, AutomationConstants.API_MAX_TIMEOUT_SECONDS + 1 }) {
			Map<String, Object> config = config();
			config.put(AutomationConstants.CONFIG_API_TIMEOUT_SECONDS, seconds);
			assertThrows(IllegalArgumentException.class, () -> validate(config), String.valueOf(seconds));
		}
	}
}
