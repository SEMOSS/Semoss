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
package prerna.reactor.model.serving;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.engine.impl.model.serving.ServingCatalog;

class ServedModelEnginesUnitTests {

	private static final String OLMO = "allenai/OLMo-2-0425-1B-Instruct";
	private static final String ENDPOINT = "http://10.43.0.90/v1";

	private static ServedModelService service(ServingCatalog.Entry... entries) {
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		ServingCatalog catalog = mock(ServingCatalog.class);
		when(catalog.getEntries()).thenReturn(List.of(entries));
		when(service.catalog()).thenReturn(catalog);
		return service;
	}

	private static ServingCatalog.Entry olmo(Integer contextWindow, Integer maxTokens) {
		return new ServingCatalog.Entry(OLMO, "OLMo 2 1B Instruct", "hf://" + OLMO, null, "nvidia-vllm", "vllm",
				List.of("--gpu-memory-utilization=0.2"), "2", "4Gi", 2979627008L, contextWindow, maxTokens, "apache-2.0",
				"Ai2 (US)", "dgx-admin", "2026-10-26");
	}

	private static ServedModelStatus status(String name, String endpoint, String displayName, String catalogEntry,
			Integer maxModelLen) {
		ServedModelStatus status = mock(ServedModelStatus.class);
		when(status.getServedModel()).thenReturn(name);
		when(status.getEndpoint()).thenReturn(endpoint);
		when(status.getDisplayName()).thenReturn(displayName);
		when(status.getCatalogEntry()).thenReturn(catalogEntry);
		when(status.getMaxModelLen()).thenReturn(maxModelLen);
		return status;
	}

	/** Exactly what the UI's Self Hosted form sends, plus where the model is served */
	@Test
	void detailsAreTheSelfHostedForm() {
		Map<String, Object> expected = new LinkedHashMap<>();
		expected.put("MODEL_TYPE", "TEXT_GENERATION");
		expected.put("MODEL_BRAND", "OPEN_AI");
		expected.put("MODEL", "olmo-2-1b");
		expected.put("VAR_NAME", "myModel");
		expected.put("ENDPOINT", ENDPOINT);
		expected.put("OPEN_AI_KEY", "kserve");
		expected.put("MAX_TOKENS", 1024);
		expected.put("CONTEXT_WINDOW", 4096);
		expected.put("KEEP_INPUT_OUTPUT", "true");
		expected.put("KEEP_CONVERSATION_HISTORY", "true");
		expected.put("INIT_MODEL_ENGINE", "import genai_client;${VAR_NAME} = genai_client.OpenAiClient("
				+ "endpoint = '${ENDPOINT}', model_name = '${MODEL}', api_key = '${OPEN_AI_KEY}')");
		expected.put("KSERVE_NAMESPACE", "semoss");
		expected.put("KSERVE_SERVED_MODEL", "olmo-2-1b");

		Map<String, Object> details = ServedModelEngines.modelDetails("olmo-2-1b", "semoss", ENDPOINT, 4096, 1024);

		assertEquals(List.copyOf(expected.entrySet()), List.copyOf(details.entrySet()));
		Map<String, Object> unknown = ServedModelEngines.modelDetails("olmo-2-1b", "semoss", ENDPOINT, null, null);
		assertTrue(!unknown.containsKey("MAX_TOKENS") && !unknown.containsKey("CONTEXT_WINDOW"), unknown.toString());
	}

	@Test
	void aDefinedModelGetsItsCatalogNameAndLimits() {
		ServedModelService service = service(olmo(4096, 1024));
		try (var security = mockStatic(AbstractSecurityUtils.class)) {
			ServedModelEngines.Request request = ServedModelEngines.request(service,
					status("olmo-2-1b", ENDPOINT, "OLMo 2 1B Instruct", OLMO, 2048), null);

			assertEquals("OLMo 2 1B Instruct", request.engineName());
			assertEquals(4096, request.details().get("CONTEXT_WINDOW"), "the catalog's, over vLLM's");
			assertEquals(1024, request.details().get("MAX_TOKENS"));
			assertEquals(ENDPOINT, request.details().get("ENDPOINT"));
			assertEquals("olmo-2-1b", request.details().get("MODEL"), "the name clients send is the InferenceService's");
			security.verify(() -> AbstractSecurityUtils.containsEngineName("OLMo 2 1B Instruct"));
		}
	}

	@Test
	void anAppliedModelGetsItsOwnNameAndVllmsContextWindow() {
		ServedModelService service = service(olmo(4096, 1024));
		try (var security = mockStatic(AbstractSecurityUtils.class)) {
			ServedModelEngines.Request request = ServedModelEngines.request(service,
					status("olmo-2-1b-instruct", ENDPOINT, null, null, 4096), null);

			assertEquals("olmo-2-1b-instruct", request.engineName());
			assertEquals(4096, request.details().get("CONTEXT_WINDOW"));
			assertTrue(!request.details().containsKey("MAX_TOKENS"));
			verify(service, never()).catalog();
			security.verify(() -> AbstractSecurityUtils.containsEngineName("olmo-2-1b-instruct"));

			// a display name SEMOSS does not accept as an engine name falls back too
			assertEquals("llama-8b", ServedModelEngines
					.request(service, status("llama-8b", ENDPOINT, "Llama 3.1 8B", null, null), null).engineName());
			// and a catalog entry removed since costs only the limits
			ServedModelEngines.Request orphan = ServedModelEngines.request(service,
					status("old", ENDPOINT, "Old", "org/retired", 8192), " My model ");
			assertEquals("My model", orphan.engineName());
			assertEquals(8192, orphan.details().get("CONTEXT_WINDOW"));
		}
	}

	@Test
	void refusesWithoutAnEndpointOrAFreeValidName() {
		ServedModelService service = service(olmo(4096, 1024));
		try (var security = mockStatic(AbstractSecurityUtils.class)) {
			ServedModelStatus noEndpoint = status("olmo-2-1b", null, "OLMo 2 1B Instruct", OLMO, null);
			when(noEndpoint.getEndpointNote()).thenReturn("SEMOSS may not list Services");
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> ServedModelEngines.request(service, noEndpoint, null));
			assertEquals("SEMOSS has no endpoint for olmo-2-1b, so it cannot create its engine: "
					+ "SEMOSS may not list Services", error.getMessage());
			when(noEndpoint.getEndpointNote()).thenReturn(null);
			assertEquals("SEMOSS has no endpoint for olmo-2-1b, so it cannot create its engine",
					assertThrows(IllegalArgumentException.class,
							() -> ServedModelEngines.request(service, noEndpoint, null)).getMessage());

			ServedModelStatus ready = status("olmo-2-1b", ENDPOINT, "OLMo 2 1B Instruct", OLMO, 4096);
			assertTrue(assertThrows(IllegalArgumentException.class,
					() -> ServedModelEngines.request(service, ready, "2 models")).getMessage()
					.startsWith("Invalid engine name 2 models"));

			security.when(() -> AbstractSecurityUtils.containsEngineName("OLMo 2 1B Instruct")).thenReturn(true);
			assertEquals("An engine named OLMo 2 1B Instruct already exists; give another engineName",
					assertThrows(IllegalArgumentException.class, () -> ServedModelEngines.request(service, ready, null))
							.getMessage());
		}
	}
}
