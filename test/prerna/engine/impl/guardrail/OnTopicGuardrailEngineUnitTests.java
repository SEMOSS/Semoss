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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.engine.api.GuardrailTypeEnum;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.GuardrailNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

class OnTopicGuardrailEngineUnitTests {

	private static final String VECTOR_ID = "on-topic-examples";
	private static final String PROMPT = "How do I reset my benefits portal password?";

	@TempDir
	Path engineDir;

	private IVectorDatabaseEngine vectorDb;
	private Insight insight;

	@BeforeEach
	void setUp() {
		this.vectorDb = mock(IVectorDatabaseEngine.class);
		this.insight = mock(Insight.class);
	}

	@Test
	void reportsTheTypeTheCatalogCreatesItUnder() throws Exception {
		try (OnTopicGuardrailEngine engine = new OnTopicGuardrailEngine()) {
			assertEquals(GuardrailTypeEnum.EMBEDDED_ON_TOPIC, engine.getGuardrailType());
		}
	}

	@Test
	void aCloseDistancePasses() throws Exception {
		GuardrailNounMetadata result = check(smss("true", "0.5"), topMatch(0.4), null);

		assertTrue(result.isPass());
		assertEquals(PROMPT, result.getReturnPrompt());
		assertEquals(0.4, details(result).get("bestScore"));
		assertEquals(0.5, details(result).get("threshold"));
	}

	@Test
	void aFarDistanceBlocks() throws Exception {
		GuardrailNounMetadata result = check(smss("true", "0.5"), topMatch(0.9), null);

		assertFalse(result.isPass());
		assertEquals(PROMPT, result.getReturnPrompt());
	}

	@Test
	void aHighSimilarityPasses() throws Exception {
		assertTrue(check(smss("false", "0.82"), topMatch(0.91), null).isPass());
	}

	@Test
	void aLowSimilarityBlocks() throws Exception {
		assertFalse(check(smss("false", "0.82"), topMatch(0.3), null).isPass());
	}

	@Test
	void aScoreOnTheThresholdPassesForEitherMetric() throws Exception {
		assertTrue(check(smss("true", "0.5"), topMatch(0.5), null).isPass());
		assertTrue(check(smss("false", "0.5"), topMatch(0.5), null).isPass());
	}

	@Test
	void theThresholdOnTheCallOverridesTheSmssDefault() throws Exception {
		GuardrailNounMetadata result = check(smss("false", "0.95"), topMatch(0.85), 0.8);

		assertTrue(result.isPass());
		assertEquals(0.8, details(result).get("threshold"));
	}

	@Test
	void noMatchesBlocksForEitherMetric() throws Exception {
		assertFalse(check(smss("true", "0.5"), List.of(), null).isPass());
		assertFalse(check(smss("false", "0.5"), List.of(), null).isPass());
	}

	@Test
	void aTopMatchWithoutANumericScoreBlocksForEitherMetric() throws Exception {
		List<Map<String, Object>> unscored = List.of(Map.of("Content", "Reset a password", "Score", "close"));

		assertFalse(check(smss("true", "0.5"), unscored, null).isPass());
		assertFalse(check(smss("false", "0.5"), unscored, null).isPass());
		assertFalse(check(smss("false", "0.5"), List.of(Map.of("Content", "Reset a password")), null).isPass());
	}

	@Test
	void onlyTheTopMatchDecides() throws Exception {
		List<Map<String, Object>> results = List.of(Map.of("Score", 0.9), Map.of("Score", 0.1));

		assertFalse(check(smss("true", "0.5"), results, null).isPass());
	}

	@Test
	void searchesTheConfiguredVectorDatabaseWithTheCallersInsightAndLimit() throws Exception {
		Properties smss = smss("true", "0.5");
		smss.setProperty(OnTopicGuardrailEngine.LIMIT_KEY, "3");

		GuardrailNounMetadata result = check(smss, topMatch(0.2), null);

		verify(this.vectorDb).nearestNeighbor(same(this.insight), eq(PROMPT), eq(3), any());
		assertEquals(3, details(result).get("limit"));
	}

	@Test
	void aMissingVectorDatabaseFailsTheCheck() {
		Properties smss = smss("true", "0.5");
		this.vectorDb = null;

		assertThrows(IllegalStateException.class, () -> check(smss, topMatch(0.2), null));
	}

	@Test
	void openRequiresTheVectorDatabase() {
		Properties smss = smss("true", "0.5");
		smss.remove(OnTopicGuardrailEngine.VECTOR_ENGINE_ID_KEY);

		assertThrows(IllegalArgumentException.class, () -> check(smss, topMatch(0.2), null));
	}

	@Test
	void openRequiresTheScoreMetric() {
		Properties missing = smss("true", "0.5");
		missing.remove(OnTopicGuardrailEngine.SCORE_IS_DISTANCE_KEY);
		assertThrows(IllegalArgumentException.class, () -> check(missing, topMatch(0.2), null));

		assertThrows(IllegalArgumentException.class, () -> check(smss("maybe", "0.5"), topMatch(0.2), null));
	}

	private static List<Map<String, Object>> topMatch(double score) {
		return List.of(Map.of("Content", "Reset a password", "Score", score));
	}

	private static Properties smss(String scoreIsDistance, String threshold) {
		Properties smss = new Properties();
		smss.setProperty(Constants.ENGINE, "on-topic-id");
		smss.setProperty(Constants.ENGINE_ALIAS, "On Topic");
		smss.setProperty(OnTopicGuardrailEngine.VECTOR_ENGINE_ID_KEY, VECTOR_ID);
		smss.setProperty(OnTopicGuardrailEngine.SCORE_IS_DISTANCE_KEY, scoreIsDistance);
		smss.setProperty(OnTopicGuardrailEngine.DEFAULT_THRESHOLD_KEY, threshold);
		return smss;
	}

	private static Map<?, ?> details(GuardrailNounMetadata result) {
		return (Map<?, ?>) result.getFullDetails();
	}

	/**
	 * Opens the guardrail in a temporary engine folder and checks {@link #PROMPT}
	 * against the given vector search results, passing a threshold on the call
	 * when one is given.
	 */
	private GuardrailNounMetadata check(Properties smss, List<Map<String, Object>> results, Double threshold)
			throws Exception {
		String folder = this.engineDir.toString();
		try (MockedStatic<SecretsFactory> secrets = mockStatic(SecretsFactory.class);
				MockedStatic<EngineUtility> folders = mockStatic(EngineUtility.class);
				MockedStatic<GitRepoUtils> git = mockStatic(GitRepoUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				OnTopicGuardrailEngine engine = new OnTopicGuardrailEngine()) {
			folders.when(() -> EngineUtility.getSpecificEngineBaseFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAppRootFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineVersionFolder(any(), any())).thenReturn(folder);
			folders.when(() -> EngineUtility.getSpecificEngineAssetsFolder(any(), any())).thenReturn(folder);
			utility.when(() -> Utility.getVectorDatabase(VECTOR_ID)).thenReturn(this.vectorDb);
			if (this.vectorDb != null) {
				when(this.vectorDb.nearestNeighbor(any(), anyString(), any(), any())).thenReturn(results);
			}

			engine.open(smss);
			NounStore ns = new NounStore("all");
			ns.makeGenRowStruct("prompt").add(new NounMetadata(PROMPT, PixelDataType.CONST_STRING));
			if (threshold != null) {
				ns.makeGenRowStruct("threshold").add(new NounMetadata(threshold, PixelDataType.CONST_DECIMAL));
			}
			ns.makeGenRowStruct(Constants.INSIGHT).add(new NounMetadata(this.insight, PixelDataType.CACHED_CLASS));
			return engine.execute(ns, new GenRowStruct());
		}
	}
}
