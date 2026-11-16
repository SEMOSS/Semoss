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
package prerna.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import prerna.usertracking.AuditSanitizer;

class SecretRedactorUnitTests {

	private static final String OPENAI_KEY = "sk-live-9f8e7d6c5b4a3f2e1d0c";
	private static final String REDACTED = AuditSanitizer.REDACTED;

	private final List<String> owners = new ArrayList<>();

	@AfterEach
	void forgetRecordedSecrets() {
		this.owners.forEach(SecretRedactor::forgetSecrets);
	}

	@Test
	void aRecordedValueIsRemovedFromErrorText() {
		record("model-one", "OPEN_AI_KEY", OPENAI_KEY);

		assertEquals("upstream rejected " + REDACTED + " with 401",
				SecretRedactor.redactError("upstream rejected " + OPENAI_KEY + " with 401"));
	}

	@Test
	void aTracebackQuotingTheFilledInitScriptLosesTheKey() {
		record("model-four", "OPEN_AI_KEY", OPENAI_KEY);

		String redacted = SecretRedactor
				.redactError("SyntaxError: client = OpenAI(api_key='" + OPENAI_KEY + "', base_url='https://gw'");

		assertFalse(redacted.contains(OPENAI_KEY), redacted);
		assertTrue(redacted.contains("base_url='https://gw'"), redacted);
	}

	@Test
	void theValueIsFoundInWhateverWordsSurroundIt() {
		record("model-two", "API_KEY", OPENAI_KEY);

		assertFalse(SecretRedactor.redactError("upstream said: no access for " + OPENAI_KEY + " today")
				.contains(OPENAI_KEY));
	}

	@Test
	void jsonEscapedAndUrlEncodedFormsAreRemovedToo() {
		String password = "p@ss\"word/with spaces";
		record("database-one", "PASSWORD", password);

		String jsonEscaped = "{\"detail\":\"login failed for p@ss\\\"word/with spaces\"}";
		String urlEncoded = "GET /login?pw=" + URLEncoder.encode(password, StandardCharsets.UTF_8) + " failed";

		assertFalse(SecretRedactor.redactError(jsonEscaped).contains("word/with spaces"));
		assertFalse(
				SecretRedactor.redactError(urlEncoded).contains(URLEncoder.encode(password, StandardCharsets.UTF_8)));
	}

	@Test
	void theTokenInABearerValueIsRemovedOnItsOwn() {
		record("function-one", "AUTH_TOKEN", "Bearer abcdef0123456789");

		assertEquals("token " + REDACTED + " expired", SecretRedactor.redactError("token abcdef0123456789 expired"));
	}

	@Test
	void secretValuesInsideAJsonSettingAreRemoved() {
		record("rest-one", "HEADERS",
				"{\"Authorization\":\"Bearer zyxwvutsrq9876543210\",\"Content-Type\":\"application/json\"}");

		String redacted = SecretRedactor.redactError("401 for token zyxwvutsrq9876543210 sent as application/json");

		assertFalse(redacted.contains("zyxwvutsrq9876543210"));
		assertTrue(redacted.contains("application/json"), redacted);
	}

	@Test
	void valuesUnderKeysThatAreNotSecretAreNotRecorded() {
		record("database-two", "CONNECTION_URL", "jdbc-free-host.internal:5432/claims");

		assertEquals("could not reach jdbc-free-host.internal:5432/claims",
				SecretRedactor.redactError("could not reach jdbc-free-host.internal:5432/claims"));
	}

	@Test
	void shortValuesAreNotReplacedOnSight() {
		record("database-three", "PASSWORD", "admin");

		assertEquals("user admin was not found", SecretRedactor.redactError("user admin was not found"));
		assertEquals("login failed with password=" + REDACTED,
				SecretRedactor.redactError("login failed with password=admin"));
	}

	@Test
	void secretShapesAreRemovedWithoutBeingRecorded() {
		String redacted = SecretRedactor.redactError(
				"failed jdbc:postgresql://db:5432/app?password=x with Authorization: Bearer abc.def.ghi at "
						+ "https://svc:hunter22@api.example.com/v1");

		assertFalse(redacted.contains("jdbc:postgresql"));
		assertFalse(redacted.contains("abc.def.ghi"));
		assertFalse(redacted.contains("hunter22"));
	}

	@Test
	void textWithoutSecretsComesBackAsTheSameString() {
		String error = "Model engine 1234 does not exist";

		assertSame(error, SecretRedactor.redactError(error));
		assertNull(SecretRedactor.redactError(null));
		assertEquals("", SecretRedactor.redactError(""));
	}

	@Test
	void errorMapsAndListsAreCopiedWithTheirTextRedacted() {
		record("model-three", "OPEN_AI_KEY", OPENAI_KEY);
		Map<String, Object> details = new HashMap<>();
		details.put("errorMessage", "rejected " + OPENAI_KEY);
		details.put("code", 401);
		details.put("token", "anything");
		details.put("steps", List.of("first", "then " + OPENAI_KEY));

		Object redacted = SecretRedactor.redactErrorValue(details);

		Map<?, ?> map = (Map<?, ?>) redacted;
		assertEquals("rejected " + REDACTED, map.get("errorMessage"));
		assertEquals(401, map.get("code"));
		assertEquals(REDACTED, map.get("token"));
		assertEquals(List.of("first", "then " + REDACTED), map.get("steps"));
		assertEquals("rejected " + OPENAI_KEY, details.get("errorMessage"));
	}

	@Test
	void valuesWithoutSecretsComeBackAsTheSameObject() {
		Map<String, Object> details = Map.of("errorMessage", "plain failure", "code", 500);
		List<Object> list = List.of("plain", 1);

		assertSame(details, SecretRedactor.redactErrorValue(details));
		assertSame(list, SecretRedactor.redactErrorValue(list));
		assertEquals(42, SecretRedactor.redactErrorValue(42));
		assertNull(SecretRedactor.redactErrorValue(null));
	}

	@Test
	void forgettingAnOwnerKeepsAValueAnotherOwnerShares() {
		record("model-shared-a", "OPEN_AI_KEY", OPENAI_KEY);
		record("model-shared-b", "OPEN_AI_KEY", OPENAI_KEY);

		SecretRedactor.forgetSecrets("model-shared-a");
		assertFalse(SecretRedactor.redactError("key " + OPENAI_KEY).contains(OPENAI_KEY));

		SecretRedactor.forgetSecrets("model-shared-b");
		assertTrue(SecretRedactor.redactError("key " + OPENAI_KEY).contains(OPENAI_KEY));
	}

	@Test
	void recordingAgainAddsToWhatTheOwnerRecorded() {
		record("multi-connection", "1_PASSWORD", "firstConnectionSecret");
		record("multi-connection", "2_PASSWORD", "secondConnectionSecret");

		String redacted = SecretRedactor.redactError("firstConnectionSecret and secondConnectionSecret");

		assertEquals(REDACTED + " and " + REDACTED, redacted);
	}

	@Test
	void anEscapedSecretKeyInsideAnErrorMasksOnlyItsValue() {
		String error = "tool failed: {\\\"pass\\\":false,\\\"token\\\":\\\"abc123\\\",\\\"detail\\\":\\\"timeout\\\"}";

		String redacted = SecretRedactor.redactError(error);

		assertFalse(redacted.contains("abc123"), redacted);
		assertTrue(redacted.contains("\\\"detail\\\":\\\"timeout\\\""), redacted);
		assertTrue(redacted.startsWith("tool failed: "), redacted);
	}

	@Test
	void valuesUnderHeaderNamesEndingInKeyAreRecorded() {
		record("rest-two", "HEADERS",
				"{\"Ocp-Apim-Subscription-Key\":\"abcd1234efgh\",\"Accept\":\"application/json\"}");

		String redacted = SecretRedactor.redactError("401 for subscription abcd1234efgh as application/json");

		assertFalse(redacted.contains("abcd1234efgh"), redacted);
		assertTrue(redacted.contains("application/json"), redacted);
	}

	@Test
	void aListUnderASecretLookingKeyIsRedactedItemByItem() {
		record("model-five", "OPEN_AI_KEY", OPENAI_KEY);
		Map<String, Object> details = new HashMap<>();
		details.put("headers", List.of("Name", "Amount", OPENAI_KEY));

		Map<?, ?> redacted = (Map<?, ?>) SecretRedactor.redactErrorValue(details);

		assertEquals(List.of("Name", "Amount", REDACTED), redacted.get("headers"));
	}

	@Test
	void arraysAndGsonJsonAreRedacted() {
		record("model-six", "OPEN_AI_KEY", OPENAI_KEY);
		JsonObject json = new JsonObject();
		json.addProperty("message", "rejected " + OPENAI_KEY);
		json.addProperty("token", "anything");
		json.addProperty("code", 401);

		List<?> array = (List<?>) SecretRedactor.redactErrorValue(new Object[] { "failed", "key " + OPENAI_KEY });
		JsonObject redactedJson = (JsonObject) SecretRedactor.redactErrorValue(json);

		assertEquals(List.of("failed", "key " + REDACTED), array);
		assertEquals("rejected " + REDACTED, redactedJson.get("message").getAsString());
		assertEquals(REDACTED, redactedJson.get("token").getAsString());
		assertEquals(401, redactedJson.get("code").getAsInt());
		assertEquals("rejected " + OPENAI_KEY, json.get("message").getAsString());
	}

	private void record(String owner, String key, String value) {
		this.owners.add(owner);
		Properties properties = new Properties();
		properties.setProperty("ENGINE", owner);
		properties.setProperty(key, value);
		SecretRedactor.recordSecrets(owner, properties);
	}
}
