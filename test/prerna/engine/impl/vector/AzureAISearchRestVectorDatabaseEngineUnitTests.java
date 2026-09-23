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
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hc.core5.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Utility;

public class AzureAISearchRestVectorDatabaseEngineUnitTests {

	private static final String INDEX_DEFINITION = "{\"fields\":["
			+ "{\"name\":\"content\",\"type\":\"Edm.String\",\"searchable\":true,\"retrievable\":true},"
			+ "{\"name\":\"tokens\",\"type\":\"Edm.Int64\",\"searchable\":false,\"retrievable\":true},"
			+ "{\"name\":\"secret\",\"type\":\"Edm.String\",\"searchable\":false,\"retrievable\":false}]}";

	private AzureAISearchRestVectorDatabaseEngine engine;

	@BeforeEach
	void setUp() throws Exception {
		engine = new AzureAISearchRestVectorDatabaseEngine();
		setField(engine, "clusterUrl", "https://example.search.windows.net");
		setField(engine, "apiKey", "test-key");
		setField(engine, "apiVersion", "2024-07-01");
		setField(engine, "indexName", "test-index");
	}

	@Test
	void keywordSearchAppliesSearchModeAndSelect() {
		AtomicReference<String> requestBody = new AtomicReference<>();
		String searchResponse = "{\"value\":[{\"@search.score\":1.5,\"content\":\"hello\",\"tokens\":7}]}";

		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			mockIndexDefinition(http);
			http.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenAnswer(invocation -> {
						requestBody.set(invocation.getArgument(2));
						return searchResponse;
					});

			Map<String, Object> parameters = new HashMap<>();
			parameters.put(AbstractVectorDatabaseEngine.SEARCH_MODE_KEY, "all");
			parameters.put(AbstractVectorDatabaseEngine.SELECT_KEY, Arrays.asList("content", "tokens"));
			List<Map<String, Object>> results = engine.keywordSearch(null, "hello world",
					List.of("content"), 3, parameters);

			JsonObject request = JsonParser.parseString(requestBody.get()).getAsJsonObject();
			assertEquals("all", request.get("searchMode").getAsString());
			assertEquals("content,tokens", request.get("select").getAsString());
			assertEquals("hello", results.get(0).get("content"));
			assertEquals(7L, ((Number) results.get(0).get("tokens")).longValue());
			assertEquals(1.5, results.get(0).get("Score"));
			assertFalse(results.get(0).containsKey("secret"));
		}
	}

	@Test
	void keywordSearchRejectsInvalidSearchMode() {
		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			mockIndexDefinition(http);
			Map<String, Object> parameters = Map.of(AbstractVectorDatabaseEngine.SEARCH_MODE_KEY, "some");

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
					() -> engine.keywordSearch(null, "hello", List.of("content"), 3, parameters));
			assertEquals("Search mode must be either 'any' or 'all'", exception.getMessage());
		}
	}

	@Test
	void keywordSearchRejectsUnknownSelectField() {
		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			mockIndexDefinition(http);
			Map<String, Object> parameters = Map.of(AbstractVectorDatabaseEngine.SELECT_KEY, List.of("missing"));

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
					() -> engine.keywordSearch(null, "hello", List.of("content"), 3, parameters));
			assertTrue(exception.getMessage().contains("does not exist"));
		}
	}

	@Test
	void keywordSearchRejectsNonSelectableField() {
		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			mockIndexDefinition(http);
			Map<String, Object> parameters = Map.of(AbstractVectorDatabaseEngine.SELECT_KEY, List.of("secret"));

			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
					() -> engine.keywordSearch(null, "hello", List.of("content"), 3, parameters));
			assertEquals("Select field 'secret' is not selectable", exception.getMessage());
		}
	}

	@Test
	void hybridSearchAppliesSearchModeAndSelect() throws Exception {
		setField(engine, "modelPropsLoaded", true);
		setField(engine, "embedderEngineId", "test-embedder");
		IModelEngine embedder = mock(IModelEngine.class);
		Insight insight = mock(Insight.class);
		EmbeddingsModelEngineResponse embeddingsResponse = mock(EmbeddingsModelEngineResponse.class);
		when(embedder.embeddings(Collections.singletonList("hello"), insight, null)).thenReturn(embeddingsResponse);
		when(embeddingsResponse.getResponse()).thenReturn(List.of(List.of(0.1, 0.2)));
		AtomicReference<String> requestBody = new AtomicReference<>();

		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class);
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			mockIndexDefinition(http);
			utility.when(() -> Utility.getModel("test-embedder")).thenReturn(embedder);
			http.when(() -> HttpHelperUtility.postRequestStringBody(any(String.class), any(Map.class),
					any(String.class), any(ContentType.class), nullable(String.class), nullable(String.class),
					nullable(String.class))).thenAnswer(invocation -> {
						requestBody.set(invocation.getArgument(2));
						return "{\"value\":[{\"@search.score\":1.0,\"content\":\"hello\"}]}";
					});

			Map<String, Object> parameters = new HashMap<>();
			parameters.put(AbstractVectorDatabaseEngine.SEARCH_MODE_KEY, "all");
			parameters.put(AbstractVectorDatabaseEngine.SELECT_KEY, List.of("content"));
			List<Map<String, Object>> results = engine.hybridSearch(insight, "hello", List.of("content"), 3,
					parameters);

			JsonObject request = JsonParser.parseString(requestBody.get()).getAsJsonObject();
			assertEquals("all", request.get("searchMode").getAsString());
			assertEquals("content", request.get("select").getAsString());
			assertTrue(request.has("vectorQueries"));
			assertEquals("hello", results.get(0).get("content"));
		}
	}

	private static void mockIndexDefinition(MockedStatic<HttpHelperUtility> http) {
		http.when(() -> HttpHelperUtility.getRequest(any(String.class), any(Map.class), nullable(String.class),
				nullable(String.class), nullable(String.class))).thenReturn(INDEX_DEFINITION);
	}

	private static void setField(Object target, String fieldName, Object value) throws Exception {
		Field field = null;
		Class<?> currentClass = target.getClass();
		while (currentClass != null && field == null) {
			try {
				field = currentClass.getDeclaredField(fieldName);
			} catch (NoSuchFieldException e) {
				currentClass = currentClass.getSuperclass();
			}
		}
		if (field == null) {
			throw new NoSuchFieldException(fieldName);
		}
		field.setAccessible(true);
		field.set(target, value);
	}
}
