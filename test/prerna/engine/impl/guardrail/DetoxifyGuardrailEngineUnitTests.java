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
package prerna.engine.impl.guardrail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.ds.py.PyTranslator;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.ResponseTimeoutException;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.git.GitRepoUtils;

class DetoxifyGuardrailEngineUnitTests {

	private static final String PROMPT = "You are a wonderful colleague";
	private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(15);
	private static final String DEFAULT_TIMEOUT_MESSAGE = "The guardrail did not respond in time, so the request was blocked.";

	@TempDir
	Path engineDir;

	private PyTranslator python;

	@BeforeEach
	void setUp() {
		this.python = mock(PyTranslator.class);
	}

	@Test
	void reportsTheTypeTheCatalogCreatesItUnder() throws Exception {
		try (MockedConstruction<ClientProcessWrapper> processes = mockConstruction(ClientProcessWrapper.class);
				DetoxifyGuardrailEngine engine = new DetoxifyGuardrailEngine()) {
			assertEquals(GuardrailTypeEnum.EMBEDDED_DETOXIFY, engine.getGuardrailType());
		}
	}

	@Test
	void aCategoryAboveTheThresholdBlocks() throws Exception {
		Map<String, Object> scores = Map.of("toxicity", 0.91, "insult", 0.2);

		GuardrailNounMetadata result = check(smss(), scores, PROMPT, null);

		assertFalse(result.isPass());
		assertEquals(PROMPT, result.getReturnPrompt());
		assertEquals(0.7, details(result).get("threshold"));
		assertEquals(scores, details(result).get("return"));
	}

	@Test
	void everyCategoryAtOrBelowTheThresholdPasses() throws Exception {
		assertTrue(check(smss(), Map.of("toxicity", 0.7, "threat", 0.01), PROMPT, null).isPass());
	}

	@Test
	void scoresSentAsTextAreCompared() throws Exception {
		assertFalse(check(smss(), Map.of("toxicity", "0.95"), PROMPT, null).isPass());
		assertTrue(check(smss(), Map.of("toxicity", "0.05"), PROMPT, null).isPass());
	}

	@Test
	void theSmssDefaultThresholdIsUsed() throws Exception {
		Properties smss = smss();
		smss.setProperty("DEFAULT_THRESHOLD", "0.95");

		GuardrailNounMetadata result = check(smss, Map.of("toxicity", 0.91), PROMPT, null);

		assertTrue(result.isPass());
		assertEquals(0.95, details(result).get("threshold"));
	}

	@Test
	void theThresholdOnTheCallOverridesTheDefault() throws Exception {
		GuardrailNounMetadata result = check(smss(), Map.of("toxicity", 0.4), PROMPT, 0.3);

		assertFalse(result.isPass());
		assertEquals(0.3, details(result).get("threshold"));
	}

	@Test
	void thePromptReachesPythonAsOneQuotedString() throws Exception {
		String prompt = "He said \"stop\")\nprint('owned')";

		check(smss(), Map.of("toxicity", 0.1), prompt, null);

		verify(this.python).runDirectPyNoCancelTrace(isNull(), eq(DEFAULT_TIMEOUT), eq(DEFAULT_TIMEOUT_MESSAGE),
				eq("model.predict(\"He said \\\"stop\\\")\\nprint('owned')\")"));
	}

	@Test
	void theSmssTimeoutAndMessageReachPython() throws Exception {
		Properties smss = smss();
		smss.setProperty(AbstractPythonGuardrailReactorFunctionEngine.TIMEOUT_SECONDS_KEY, "30");
		smss.setProperty(AbstractPythonGuardrailReactorFunctionEngine.TIMEOUT_MESSAGE_KEY,
				"Toxicity screening is busy, please try again.");

		check(smss, Map.of("toxicity", 0.1), PROMPT, null);

		verify(this.python).runDirectPyNoCancelTrace(isNull(), eq(Duration.ofSeconds(30)),
				eq("Toxicity screening is busy, please try again."), anyString());
	}

	@ParameterizedTest
	@ValueSource(strings = { "0", "-5", "soon" })
	void anUnusableSmssTimeoutKeepsTheDefault(String timeout) throws Exception {
		Properties smss = smss();
		smss.setProperty(AbstractPythonGuardrailReactorFunctionEngine.TIMEOUT_SECONDS_KEY, timeout);

		check(smss, Map.of("toxicity", 0.1), PROMPT, null);

		verify(this.python).runDirectPyNoCancelTrace(isNull(), eq(DEFAULT_TIMEOUT), eq(DEFAULT_TIMEOUT_MESSAGE),
				anyString());
	}

	@Test
	void aTimeoutFailsTheCheck() {
		ResponseTimeoutException timeout = new ResponseTimeoutException(DEFAULT_TIMEOUT_MESSAGE);
		when(this.python.runDirectPyNoCancelTrace(nullable(Insight.class), any(Duration.class), anyString(),
				anyString())).thenThrow(timeout);

		ResponseTimeoutException thrown = assertThrows(ResponseTimeoutException.class,
				() -> check(smss(), null, PROMPT, null));
		assertSame(timeout, thrown);
	}

	@Test
	void aMissingPromptFailsTheCheck() {
		assertThrows(IllegalArgumentException.class, () -> check(smss(), Map.of("toxicity", 0.1), null, null));
	}

	private static Properties smss() {
		Properties smss = new Properties();
		smss.setProperty(Constants.ENGINE, "detoxify-id");
		smss.setProperty(Constants.ENGINE_ALIAS, "Detoxify");
		return smss;
	}

	private static Map<?, ?> details(GuardrailNounMetadata result) {
		return (Map<?, ?>) result.getFullDetails();
	}

	/**
	 * Opens the guardrail in a temporary engine folder and checks the prompt, with
	 * python answering {@code scores} unless a test has already stubbed it.
	 */
	private GuardrailNounMetadata check(Properties smss, Map<String, Object> scores, String prompt, Double threshold)
			throws Exception {
		String folder = this.engineDir.toString();
		if (scores != null) {
			when(this.python.runDirectPyNoCancelTrace(nullable(Insight.class), any(Duration.class), anyString(),
					anyString())).thenReturn(scores);
		}
		try (MockedStatic<SecretsFactory> secrets = mockStatic(SecretsFactory.class);
				MockedStatic<EngineUtility> folders = mockStatic(EngineUtility.class);
				MockedStatic<GitRepoUtils> git = mockStatic(GitRepoUtils.class);
				MockedConstruction<ClientProcessWrapper> processes = mockConstruction(ClientProcessWrapper.class);
				DetoxifyGuardrailEngine engine = new DetoxifyGuardrailEngine()) {
			folders.when(() -> EngineUtility.getSpecificEngineBaseFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAppRootFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineVersionFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(), any(), any())).thenReturn(folder);

			engine.open(smss);
			engine.pyTranslator = this.python;
			NounStore ns = new NounStore("all");
			if (prompt != null) {
				ns.makeGenRowStruct("prompt").add(new NounMetadata(prompt, PixelDataType.CONST_STRING));
			}
			if (threshold != null) {
				ns.makeGenRowStruct("threshold").add(new NounMetadata(threshold, PixelDataType.CONST_DECIMAL));
			}
			return engine.execute(ns, new GenRowStruct());
		}
	}
}
