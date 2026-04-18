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
package prerna.rag.generation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.rag.generation.HallucinationDetector.VerificationResult;

public class HallucinationDetectorUnitTests {

	private HallucinationDetector detector;
	private IModelEngine mockModelEngine;
	private Insight mockInsight;

	@BeforeEach
	public void setUp() {
		mockModelEngine = mock(IModelEngine.class);
		mockInsight = mock(Insight.class);
		detector = new HallucinationDetector(mockModelEngine);
	}

	@Test
	public void testGroundedAnswer() throws Exception {
		String llmJson = "{\"grounded\": true, \"confidence\": 0.95, \"unsupported_claims\": []}";
		AskModelEngineResponse mockResponse = mock(AskModelEngineResponse.class);
		when(mockResponse.getResponse()).thenReturn(llmJson);
		when(mockModelEngine.ask(anyString(), isNull(), any(Insight.class), any(Map.class)))
				.thenReturn(mockResponse);

		List<Map<String, Object>> sources = new ArrayList<>();
		sources.add(makeSource("The capital of France is Paris."));

		VerificationResult result = detector.verify("Paris is the capital of France.", sources, mockInsight);

		assertTrue(result.isGrounded());
		assertTrue(result.getConfidence() >= 0.9);
		assertTrue(result.getUnsupportedClaims().isEmpty());
	}

	@Test
	public void testHallucinatedAnswer() throws Exception {
		String llmJson = "{\"grounded\": false, \"confidence\": 0.2, \"unsupported_claims\": [\"stock market crashed\"]}";
		AskModelEngineResponse mockResponse = mock(AskModelEngineResponse.class);
		when(mockResponse.getResponse()).thenReturn(llmJson);
		when(mockModelEngine.ask(anyString(), isNull(), any(Insight.class), any(Map.class)))
				.thenReturn(mockResponse);

		List<Map<String, Object>> sources = new ArrayList<>();
		sources.add(makeSource("The weather is sunny today."));

		VerificationResult result = detector.verify("The stock market crashed yesterday.", sources, mockInsight);

		assertFalse(result.isGrounded());
		assertEquals(1, result.getUnsupportedClaims().size());
	}

	@Test
	public void testMalformedLlmResponse_defaultsToUnverified() throws Exception {
		AskModelEngineResponse mockResponse = mock(AskModelEngineResponse.class);
		when(mockResponse.getResponse()).thenReturn("This is not JSON at all");
		when(mockModelEngine.ask(anyString(), isNull(), any(Insight.class), any(Map.class)))
				.thenReturn(mockResponse);

		List<Map<String, Object>> sources = new ArrayList<>();
		sources.add(makeSource("Some content."));

		VerificationResult result = detector.verify("Some answer.", sources, mockInsight);

		assertNotNull(result);
		assertFalse(result.isGrounded());
	}

	@Test
	public void testEmptyAnswer() {
		VerificationResult result = detector.verify("", new ArrayList<>(), mockInsight);
		assertTrue(result.isGrounded());
		assertEquals(1.0, result.getConfidence());
	}

	@Test
	public void testEmptySources() {
		VerificationResult result = detector.verify("Some answer.", new ArrayList<>(), mockInsight);
		assertFalse(result.isGrounded());
		assertEquals(0.0, result.getConfidence());
	}

	private Map<String, Object> makeSource(String content) {
		Map<String, Object> source = new HashMap<>();
		source.put("Content", content);
		source.put("Source", "test-doc.pdf");
		source.put("Score", 0.9);
		return source;
	}
}
