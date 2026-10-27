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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.model.serving.ServedModelChange;
import prerna.engine.impl.model.serving.ServedModelLogs;
import prerna.engine.impl.model.serving.ServedModelRemoval;
import prerna.engine.impl.model.serving.ServedModelService;
import prerna.engine.impl.model.serving.ServedModelState;
import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.engine.impl.model.serving.ServingCapacity;
import prerna.engine.impl.model.serving.ServingCapacityException;
import prerna.om.Insight;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.k8s.KubeApiException;

class ServedModelReactorsUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";

	private static <T extends AbstractServedModelReactor> T reactor(T reactor, User user, Map<String, Object> keys) {
		Insight insight = mock(Insight.class);
		when(insight.getUser()).thenReturn(user);
		reactor.setInsight(insight);
		NounStore store = new NounStore("served-model-test");
		keys.forEach((key, value) -> store.makeGenRowStruct(key).add(new NounMetadata(value, PixelDataType.CONST_STRING)));
		reactor.setNounStore(store);
		return reactor;
	}

	private static ServedModelStatus status(ServedModelState state) {
		ServedModelStatus status = mock(ServedModelStatus.class);
		when(status.getState()).thenReturn(state);
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("state", state.name());
		when(status.toDetailMap()).thenReturn(map);
		when(status.toSummaryMap()).thenReturn(map);
		return status;
	}

	/** ClassGraph skips non-public classes, and with them their subclasses' link to IReactor. */
	@Test
	void reactorsArePublicSoThePixelScanFindsThem() {
		assertTrue(Modifier.isPublic(AbstractServedModelReactor.class.getModifiers()));
		for (Class<?> type : List.of(ListServedModelsReactor.class, GetServedModelStatusReactor.class,
				StartServedModelReactor.class, StopServedModelReactor.class, GetServedModelLogsReactor.class,
				ListServingCatalogReactor.class, DefineServedModelReactor.class, RemoveServedModelReactor.class,
				CreateServedModelEngineReactor.class, GetServingCapacityReactor.class)) {
			assertTrue(Modifier.isPublic(type.getModifiers()), type.getName());
			assertFalse(Modifier.isAbstract(type.getModifiers()), type.getName());
			assertTrue(IReactor.class.isAssignableFrom(type), type.getName());
		}
	}

	@Test
	void signedOutUsersMustLogIn() {
		assertThrows(SemossPixelException.class,
				() -> reactor(new ListServedModelsReactor(), null, Map.of()).execute());
	}

	@Test
	void nonAdminsAreRefusedBeforeAnythingHappens() {
		User user = mock(User.class);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> reactor(new StopServedModelReactor(), user, Map.of("servedModel", SAMPLE)).execute());
			assertTrue(error.getMessage().contains("not an admin"), error.getMessage());
			services.verifyNoInteractions();
		}
	}

	@Test
	void unconfiguredServingIsAClearError() {
		User user = mock(User.class);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance)
					.thenThrow(new IllegalStateException("Model serving is not configured: set KSERVE_ENABLED=true"));
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> reactor(new ListServedModelsReactor(), user, Map.of()).execute());
			assertTrue(error.getMessage().contains("KSERVE_ENABLED"), error.getMessage());
		}
	}

	@Test
	void startAuditsOnlyAChange() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		ServedModelStatus starting = status(ServedModelState.STARTING);
		when(service.start(SAMPLE, user, false)).thenReturn(new ServedModelChange(starting, true));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			NounMetadata result = reactor(new StartServedModelReactor(), user, Map.of("servedModel", SAMPLE)).execute();

			Map<?, ?> map = (Map<?, ?>) result.getValue();
			assertEquals("STARTING", map.get("state"));
			assertEquals(true, map.get("changed"));
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));

			ServedModelStatus ready = status(ServedModelState.READY);
			when(service.start(SAMPLE, user, false)).thenReturn(new ServedModelChange(ready, false));
			audit.clearInvocations();
			reactor(new StartServedModelReactor(), user, Map.of("servedModel", SAMPLE)).execute();
			audit.verifyNoInteractions();
		}
	}

	@Test
	void replaceAuditsTheModelsItStopped() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		ServedModelStatus starting = status(ServedModelState.STARTING);
		when(service.start("olmo-2-1b", user, true))
				.thenReturn(new ServedModelChange(starting, true, List.of(SAMPLE), false));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			NounMetadata result = reactor(new StartServedModelReactor(), user,
					Map.of("servedModel", "olmo-2-1b", "replace", "true")).execute();

			assertEquals(List.of(SAMPLE), ((Map<?, ?>) result.getValue()).get("replaced"));
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(2));
		}
	}

	@Test
	void aRefusedStartStillAuditsWhatItStopped() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		ServingCapacityException refusal = mock(ServingCapacityException.class);
		when(refusal.getReplaced()).thenReturn(List.of(SAMPLE));
		when(service.start("olmo-2-1b", user, true)).thenThrow(refusal);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			assertThrows(ServingCapacityException.class, () -> reactor(new StartServedModelReactor(), user,
					Map.of("servedModel", "olmo-2-1b", "replace", "true")).execute());
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
		}
	}

	@Test
	void capacityIsTheServicesCheck() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		ServingCapacity capacity = mock(ServingCapacity.class);
		Map<String, Object> map = Map.of("gpus", Map.of("offered", 1, "free", 0));
		when(capacity.toMap()).thenReturn(map);
		when(service.capacity()).thenReturn(capacity);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			assertEquals(map, reactor(new GetServingCapacityReactor(), user, Map.of()).execute().getValue());
		}
	}

	@Test
	void kubernetesRefusalsBecomeReadableMessages() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.stop(SAMPLE, user)).thenThrow(new KubeApiException(403, "Forbidden",
				"inferenceservices.serving.kserve.io \"olmo-2-1b-instruct\" is forbidden"));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> reactor(new StopServedModelReactor(), user, Map.of("servedModel", SAMPLE)).execute());
			assertTrue(error.getMessage().startsWith("SEMOSS is not allowed to do this in the cluster (HTTP 403)"),
					error.getMessage());
		}
	}

	@Test
	void listFiltersByState() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		List<ServedModelStatus> statuses = List.of(status(ServedModelState.READY), status(ServedModelState.STOPPED),
				status(ServedModelState.STARTING));
		when(service.list()).thenReturn(statuses);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			assertEquals(3, models(reactor(new ListServedModelsReactor(), user, Map.of()).execute()).size());
			assertEquals(2, models(reactor(new ListServedModelsReactor(), user, Map.of("state", "active")).execute())
					.size());
			assertEquals(1, models(reactor(new ListServedModelsReactor(), user, Map.of("state", "READY")).execute())
					.size());
			assertThrows(IllegalArgumentException.class,
					() -> reactor(new ListServedModelsReactor(), user, Map.of("state", "gone")).execute());
		}
	}

	private static List<?> models(NounMetadata result) {
		return (List<?>) ((Map<?, ?>) result.getValue()).get("servedModels");
	}

	@Test
	void logsPassTheirArguments() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.logs(SAMPLE, "storage-initializer", 50, true, false))
				.thenReturn(new ServedModelLogs(SAMPLE, "pod", "storage-initializer", true, List.of("line")));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			// pixel numbers arrive as doubles
			NounMetadata result = reactor(new GetServedModelLogsReactor(), user, Map.of("servedModel", SAMPLE,
					"container", "storage-initializer", "tailLines", "50.0", "previous", "true")).execute();

			assertEquals(List.of("line"), ((Map<?, ?>) result.getValue()).get("lines"));
			verify(service).logs(SAMPLE, "storage-initializer", 50, true, false);
		}
	}

	@Test
	void defineReadsItsMapAndAudits() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		ServedModelStatus stopped = status(ServedModelState.STOPPED);
		when(service.getNamespace()).thenReturn("semoss");
		when(service.define("olmo-2-1b", "allenai/OLMo-2-0425-1B-Instruct", Map.of("maxModelLen", 2048.0), true, false,
				null, user)).thenReturn(stopped);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);
			DefineServedModelReactor reactor = reactor(new DefineServedModelReactor(), user,
					Map.of("servedModel", "olmo-2-1b", "catalogEntry", "allenai/OLMo-2-0425-1B-Instruct", "start", "true"));
			reactor.getNounStore().makeGenRowStruct("overrides")
					.add(new NounMetadata(Map.of("maxModelLen", 2048.0), PixelDataType.MAP));

			NounMetadata result = reactor.execute();

			assertEquals("STOPPED", ((Map<?, ?>) result.getValue()).get("state"));
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));
		}
	}

	@Test
	void defineCanAlsoCreateTheEngineAndKeepsTheModelWhenThatFails() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		ServedModelStatus first = status(ServedModelState.STOPPED);
		ServedModelStatus second = status(ServedModelState.STOPPED);
		when(service.define("olmo-2-1b", "allenai/OLMo-2-0425-1B-Instruct", null, false, false, null, user))
				.thenReturn(first, second);
		Map<String, Object> keys = Map.of("servedModel", "olmo-2-1b", "catalogEntry", "allenai/OLMo-2-0425-1B-Instruct",
				"createEngine", "true", "engineName", "OLMo on the DGX");
		Map<String, Object> created = Map.of("servedModel", "olmo-2-1b", "engine", Map.of("engine_id", "id-1"));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class); var engines = mockStatic(ServedModelEngines.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);
			engines.when(() -> ServedModelEngines.create(any(), eq(service), eq(first), eq("OLMo on the DGX"), eq(false)))
					.thenReturn(created);

			NounMetadata result = reactor(new DefineServedModelReactor(), user, keys).execute();

			assertEquals(Map.of("engine_id", "id-1"), ((Map<?, ?>) result.getValue()).get("engine"));
			assertTrue(result.getAdditionalReturn() == null || result.getAdditionalReturn().isEmpty());

			IllegalArgumentException taken = new IllegalArgumentException(
					"An engine named OLMo on the DGX already exists; give another engineName");
			engines.when(() -> ServedModelEngines.create(any(), any(), any(), any(), anyBoolean())).thenThrow(taken);

			NounMetadata kept = reactor(new DefineServedModelReactor(), user, keys).execute();

			Map<?, ?> map = (Map<?, ?>) kept.getValue();
			assertEquals("STOPPED", map.get("state"));
			assertFalse(map.containsKey("engine"));
			String warning = (String) kept.getAdditionalReturn().get(0).getValue();
			assertEquals("olmo-2-1b is defined, but its engine was not created: An engine named OLMo on the DGX "
					+ "already exists; give another engineName. Retry with CreateServedModelEngine", warning);
			audit.verify(() -> UserAuditTrailUtils.recordPixelFailure(any(), isNull(), eq("CreateServedModelEngine"),
					eq(taken), isNull()));
		}
	}

	@Test
	void createServedModelEnginePassesItsKeys() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		ServedModelStatus ready = status(ServedModelState.READY);
		when(service.status(SAMPLE)).thenReturn(ready);
		Map<String, Object> created = Map.of("servedModel", SAMPLE, "engine", Map.of("engine_id", "id-1"));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var engines = mockStatic(ServedModelEngines.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);
			engines.when(() -> ServedModelEngines.create(any(), eq(service), eq(ready), eq("OLMo on the DGX"), eq(true)))
					.thenReturn(created);

			NounMetadata result = reactor(new CreateServedModelEngineReactor(), user,
					Map.of("servedModel", SAMPLE, "engineName", "OLMo on the DGX", "global", "true")).execute();

			assertEquals(created, result.getValue());
		}
	}

	@Test
	void defineCanPublish() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		ServedModelStatus stopped = status(ServedModelState.STOPPED);
		when(service.define("olmo-2-1b", "allenai/OLMo-2-0425-1B-Instruct", null, false, true, 8001, user))
				.thenReturn(stopped);
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			// pixel numbers arrive as doubles
			NounMetadata result = reactor(new DefineServedModelReactor(), user, Map.of("servedModel", "olmo-2-1b",
					"catalogEntry", "allenai/OLMo-2-0425-1B-Instruct", "publish", "true", "publishPort", "8001.0"))
					.execute();

			assertEquals("STOPPED", ((Map<?, ?>) result.getValue()).get("state"));
			assertThrows(IllegalArgumentException.class, () -> reactor(new DefineServedModelReactor(), user,
					Map.of("servedModel", "olmo-2-1b", "catalogEntry", "allenai/OLMo-2-0425-1B-Instruct",
							"publishPort", "80.5")).execute());
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
		}
	}

	@Test
	void removeAuditsAndReportsTheOutcome() {
		User user = mock(User.class);
		ServedModelService service = mock(ServedModelService.class);
		when(service.getNamespace()).thenReturn("semoss");
		when(service.remove("olmo-2-1b", user)).thenReturn(new ServedModelRemoval("olmo-2-1b", true, null));
		try (var security = mockStatic(SecurityAdminUtils.class); var services = mockStatic(ServedModelService.class);
				var audit = mockStatic(UserAuditTrailUtils.class)) {
			security.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			services.when(ServedModelService::getInstance).thenReturn(service);

			NounMetadata result = reactor(new RemoveServedModelReactor(), user, Map.of("servedModel", "olmo-2-1b"))
					.execute();

			assertEquals("REMOVED", ((Map<?, ?>) result.getValue()).get("state"));
			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()));
		}
	}
}
