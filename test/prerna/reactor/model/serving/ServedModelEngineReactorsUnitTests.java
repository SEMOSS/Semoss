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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.om.Insight;
import prerna.reactor.model.serving.ServedModelEngineLinks.ModelEngine;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;

class ServedModelEngineReactorsUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";
	private static final Path SMSS = Path.of("unused.smss");

	/** The engine CreateServedModelEngine made, linked to the sample */
	private static final ModelEngine LINKED = new ModelEngine("linked-1", "KServe test sample",
			"http://localhost:8000/v1", SAMPLE, SAMPLE, "semoss", SMSS);
	/** An engine made by hand that calls the sample's endpoint */
	private static final ModelEngine HAND_MADE = new ModelEngine("hand-1", "OLMo by hand", "http://localhost:8000/v1/",
			"served-under-another-name", null, null, SMSS);
	/** An engine of a model elsewhere */
	private static final ModelEngine ELSEWHERE = new ModelEngine("cloud-1", "Cloud model", "https://api.example.com/v1",
			"gpt-4o", null, null, SMSS);

	private final User user = mock(User.class);
	private final ServedModelService service = mock(ServedModelService.class);
	private MockedStatic<SecurityAdminUtils> security;
	private MockedStatic<ServedModelService> services;
	private MockedStatic<ServedModelEngineStore> store;
	private MockedStatic<UserAuditTrailUtils> audit;

	@BeforeEach
	void mockSemoss() {
		security = mockStatic(SecurityAdminUtils.class);
		services = mockStatic(ServedModelService.class);
		store = mockStatic(ServedModelEngineStore.class);
		audit = mockStatic(UserAuditTrailUtils.class);
		security.when(() -> SecurityAdminUtils.userIsAdmin(this.user)).thenReturn(true);
		services.when(ServedModelService::getInstance).thenReturn(this.service);
		when(this.service.getNamespace()).thenReturn("semoss");
		ServedModelStatus sample = mock(ServedModelStatus.class);
		when(sample.getServedModel()).thenReturn(SAMPLE);
		when(sample.getEndpoint()).thenReturn("http://localhost:8000/v1");
		when(this.service.list()).thenReturn(List.of(sample));
		when(this.service.status(SAMPLE)).thenReturn(sample);
		store.when(ServedModelEngineStore::modelEngines).thenReturn(List.of(LINKED, HAND_MADE, ELSEWHERE));
		for (ModelEngine engine : List.of(LINKED, HAND_MADE, ELSEWHERE)) {
			store.when(() -> ServedModelEngineStore.modelEngine(engine.engineId())).thenReturn(engine);
		}
	}

	@AfterEach
	void closeMocks() {
		this.audit.close();
		this.store.close();
		this.services.close();
		this.security.close();
	}

	private <T extends AbstractServedModelReactor> Map<?, ?> run(T reactor, Map<String, Object> keys) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(this.user);
		reactor.setInsight(insight);
		NounStore nouns = new NounStore("served-model-engine-test");
		keys.forEach((key, value) -> nouns.makeGenRowStruct(key).add(new NounMetadata(value, PixelDataType.CONST_STRING)));
		reactor.setNounStore(nouns);
		return (Map<?, ?>) reactor.execute().getValue();
	}

	@Test
	void theListHasTheLinkedEnginesAndTheOnesThatCallAServedModel() {
		List<?> engines = (List<?>) run(new ListServedModelEnginesReactor(), Map.of()).get("engines");

		assertEquals(2, engines.size(), "the cloud model is left out");
		Map<?, ?> linked = (Map<?, ?>) engines.get(0);
		Map<?, ?> byHand = (Map<?, ?>) engines.get(1);
		assertEquals(SAMPLE, linked.get("servedModel"));
		assertEquals(List.of(SAMPLE), linked.get("calls"));
		assertEquals("hand-1", byHand.get("engineId"));
		assertEquals(null, byHand.get("servedModel"));
		assertEquals(List.of(SAMPLE), byHand.get("calls"), "by its endpoint");
	}

	@Test
	void anEngineThatCallsTheModelIsLinkedAndAudited() {
		store.when(() -> ServedModelEngineStore.link(HAND_MADE, "semoss", SAMPLE)).thenReturn(true);

		Map<?, ?> result = run(new LinkServedModelEngineReactor(), Map.of("servedModel", SAMPLE, "engine", "hand-1"));

		assertEquals(SAMPLE, result.get("servedModel"));
		assertEquals("semoss", result.get("namespace"));
		assertEquals(true, result.get("changed"));
		store.verify(() -> ServedModelEngineStore.link(HAND_MADE, "semoss", SAMPLE));
		audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));
	}

	@Test
	void anEngineThatCallsSomethingElseIsRefusedUntouched() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> run(new LinkServedModelEngineReactor(), Map.of("servedModel", SAMPLE, "engine", "cloud-1")));

		assertTrue(error.getMessage().startsWith("Cloud model calls the model gpt-4o"), error.getMessage());
		store.verify(() -> ServedModelEngineStore.link(any(), any(), any()), org.mockito.Mockito.never());
		audit.verifyNoInteractions();
	}

	@Test
	void linkingAnEngineAgainChangesNothing() {
		Map<?, ?> result = run(new LinkServedModelEngineReactor(), Map.of("servedModel", SAMPLE, "engine", "linked-1"));

		assertEquals(false, result.get("changed"));
		store.verify(() -> ServedModelEngineStore.link(any(), any(), any()), org.mockito.Mockito.never());
		audit.verifyNoInteractions();
	}

	@Test
	void unlinkingRemovesTheLinkOnlyWhenThereIsOne() {
		store.when(() -> ServedModelEngineStore.link(LINKED, null, null)).thenReturn(true);

		Map<?, ?> unlinked = run(new UnlinkServedModelEngineReactor(), Map.of("engine", "linked-1"));
		assertEquals(true, unlinked.get("changed"));
		assertEquals(SAMPLE, unlinked.get("unlinkedFrom"));
		audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));

		audit.clearInvocations();
		Map<?, ?> untouched = run(new UnlinkServedModelEngineReactor(), Map.of("engine", "hand-1"));
		assertEquals(false, untouched.get("changed"));
		audit.verifyNoInteractions();
	}

	@Test
	void theEngineMustBeNamed() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> run(new UnlinkServedModelEngineReactor(), Map.of()));
		assertTrue(error.getMessage().contains("engine"), error.getMessage());
	}
}
