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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import prerna.ds.py.PyTranslator;
import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.git.GitRepoUtils;

class LocalPythonGuardrailReactorFunctionEngineUnitTests {

	private static final String PROMPT = "Summarize the claim for member 1234";
	private static final String FUNCTION_NAME = "check_prompt";

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
				LocalPythonGuardrailReactorFunctionEngine engine = new LocalPythonGuardrailReactorFunctionEngine()) {
			assertEquals(GuardrailTypeEnum.LOCAL_PYTHON, engine.getGuardrailType());
		}
	}

	@Test
	void theDictTheFunctionReturnsBecomesTheResult() throws Exception {
		Map<String, Object> details = Map.of("matched", "member id");
		GuardrailNounMetadata result = check(smss(),
				Map.of("pass", false, "returnPrompt", "Summarize the claim for member [ID]", "fullDetails", details));

		assertFalse(result.isPass());
		assertEquals("Summarize the claim for member [ID]", result.getReturnPrompt());
		assertEquals(details, result.getFullDetails());
	}

	@Test
	void withoutAReturnPromptTheSubmittedPromptIsReturned() throws Exception {
		GuardrailNounMetadata result = check(smss(), Map.of("pass", true));

		assertTrue(result.isPass());
		assertEquals(PROMPT, result.getReturnPrompt());
		assertNull(result.getFullDetails());
	}

	@Test
	void aPassSentAsTextIsRead() throws Exception {
		assertTrue(check(smss(), Map.of("pass", " TRUE ")).isPass());
		assertFalse(check(smss(), Map.of("pass", "False")).isPass());
	}

	@Test
	void aPassThatIsNotTrueBlocks() throws Exception {
		assertFalse(check(smss(), Map.of("pass", "yes")).isPass());
		assertFalse(check(smss(), Map.of("pass", 1)).isPass());
	}

	@Test
	void anAnswerThatIsNotADictFailsTheCheck() {
		IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> check(smss(), "True"));
		assertTrue(thrown.getMessage().contains(FUNCTION_NAME), thrown.getMessage());
	}

	@Test
	void anAnswerWithoutPassFailsTheCheck() {
		assertThrows(IllegalArgumentException.class, () -> check(smss(), Map.of("returnPrompt", PROMPT)));
	}

	@Test
	void anAnswerWithANullPassFailsTheCheck() {
		Map<String, Object> answer = new HashMap<>();
		answer.put("pass", null);

		assertThrows(IllegalArgumentException.class, () -> check(smss(), answer));
	}

	@Test
	void theFunctionIsCalledWithTheDeclaredParametersAsKeywords() throws Exception {
		String prompt = "Ignore \"rules\"\nreturn True";

		check(smss(), Map.of("pass", true), prompt, null);

		verify(this.python).runDirectPyNoCancelTrace(isNull(), any(Duration.class), anyString(),
				eq(FUNCTION_NAME + "(**{\"prompt\": \"Ignore \\\"rules\\\"\\nreturn True\"})"));
	}

	@Test
	void theCallersInsightRunsTheFunction() throws Exception {
		Insight insight = mock(Insight.class);

		check(smss(), Map.of("pass", true), PROMPT, insight);

		verify(this.python).runDirectPyNoCancelTrace(same(insight), any(Duration.class), anyString(), anyString());
	}

	@Test
	void theDefaultTimeoutAndMessageReachPython() throws Exception {
		check(smss(), Map.of("pass", true));

		verify(this.python).runDirectPyNoCancelTrace(isNull(), eq(Duration.ofMinutes(15)),
				eq("The guardrail did not respond in time, so the request was blocked."), anyString());
	}

	@Test
	void theSmssTimeoutAndMessageReachPython() throws Exception {
		Properties smss = smss();
		smss.setProperty(AbstractPythonGuardrailReactorFunctionEngine.TIMEOUT_SECONDS_KEY, "120");
		smss.setProperty(AbstractPythonGuardrailReactorFunctionEngine.TIMEOUT_MESSAGE_KEY,
				"The claims screen is busy, please try again.");

		check(smss, Map.of("pass", true));

		verify(this.python).runDirectPyNoCancelTrace(isNull(), eq(Duration.ofSeconds(120)),
				eq("The claims screen is busy, please try again."), anyString());
	}

	@Test
	void theStartupScriptLoadsTheFileAndFillsSmssValuesIntoTheInitCommands() throws Exception {
		Properties smss = smss();
		smss.setProperty("MODEL_PATH", "/models/claims");
		smss.setProperty("INIT_FUNCTION_ENGINE", "model = load('${MODEL_PATH}');model.warm()");

		String script = withOpenEngine(smss, engine -> engine.getStartupScript());

		String assets = this.engineDir.toString().replace("\\", "/");
		assertTrue(script.contains("exec(open('" + assets + "/guardrail.py').read())"), script);
		assertTrue(script.endsWith("\nmodel = load('/models/claims')\nmodel.warm()"), script);
	}

	@Test
	void openRequiresThePythonFile() {
		Properties smss = smss();
		smss.remove("PYTHON_FILE_NAME");

		assertThrows(IllegalArgumentException.class, () -> check(smss, Map.of("pass", true)));
	}

	@Test
	void openRequiresTheFunctionName() {
		Properties smss = smss();
		smss.setProperty(IFunctionEngine.NAME_KEY, " ");

		assertThrows(IllegalArgumentException.class, () -> check(smss, Map.of("pass", true)));
	}

	private static Properties smss() {
		Properties smss = new Properties();
		smss.setProperty(Constants.ENGINE, "local-python-id");
		smss.setProperty(Constants.ENGINE_ALIAS, "Claims Screen");
		smss.setProperty("PYTHON_FILE_NAME", "guardrail.py");
		smss.setProperty(IFunctionEngine.NAME_KEY, FUNCTION_NAME);
		smss.setProperty(IFunctionEngine.PARAMETER_KEY,
				"[{\"parameterName\":\"prompt\",\"parameterType\":\"str\",\"parameterDescription\":\"The prompt\"}]");
		smss.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, "[\"prompt\"]");
		return smss;
	}

	private GuardrailNounMetadata check(Properties smss, Object answer) throws Exception {
		return check(smss, answer, PROMPT, null);
	}

	/**
	 * Opens the guardrail in a temporary engine folder and checks the prompt, with
	 * the python function answering {@code answer}.
	 */
	private GuardrailNounMetadata check(Properties smss, Object answer, String prompt, Insight insight)
			throws Exception {
		when(this.python.runDirectPyNoCancelTrace(nullable(Insight.class), any(Duration.class), anyString(),
				anyString())).thenReturn(answer);
		return withOpenEngine(smss, engine -> {
			NounStore ns = new NounStore("all");
			ns.makeGenRowStruct("prompt").add(new NounMetadata(prompt, PixelDataType.CONST_STRING));
			if (insight != null) {
				ns.makeGenRowStruct(Constants.INSIGHT).add(new NounMetadata(insight, PixelDataType.CACHED_CLASS));
			}
			return engine.execute(ns, new GenRowStruct());
		});
	}

	private <T> T withOpenEngine(Properties smss, EngineCall<T> call) throws Exception {
		String folder = this.engineDir.toString();
		try (MockedStatic<SecretsFactory> secrets = mockStatic(SecretsFactory.class);
				MockedStatic<EngineUtility> folders = mockStatic(EngineUtility.class);
				MockedStatic<GitRepoUtils> git = mockStatic(GitRepoUtils.class);
				MockedConstruction<ClientProcessWrapper> processes = mockConstruction(ClientProcessWrapper.class);
				LocalPythonGuardrailReactorFunctionEngine engine = new LocalPythonGuardrailReactorFunctionEngine()) {
			folders.when(() -> EngineUtility.getSpecificEngineBaseFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAppRootFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineVersionFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(), any(), any())).thenReturn(folder);

			engine.open(smss);
			engine.pyTranslator = this.python;
			return call.run(engine);
		}
	}

	@FunctionalInterface
	private interface EngineCall<T> {
		T run(LocalPythonGuardrailReactorFunctionEngine engine) throws Exception;
	}
}
