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
package prerna.engine.impl.pipeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IVenvEngine;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SandboxProbe;
import prerna.util.Utility;

/**
 * The access check the engine proxy runs on every call, for calls the
 * platform's own code makes. Each test is one rule of
 * docs/platform_services/engine_access.md. The calls a project's custom code
 * makes are in prerna.util.SandboxEngineAccessUnitTests.
 */
class EngineAccessGuardUnitTests {

	private static final String ENGINE = "engine-1";
	private static final String OTHER_ENGINE = "engine-2";
	private static final String PROJECT = "project-1";
	private static final String AGENT = "agent-1";

	private final User user = new User();
	private MockedStatic<SecurityEngineAccessUtils> access;

	@BeforeEach
	void aServerWithOneSignedInUserWhoHoldsNothing() {
		this.access = Mockito.mockStatic(SecurityEngineAccessUtils.class);
		this.access.when(SecurityEngineAccessUtils::isSecurityDatabaseLoaded).thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.isListed(anyString(), anyBoolean())).thenReturn(true);
		ThreadStore.setUser(this.user);
	}

	@AfterEach
	void clear() {
		this.access.close();
		ThreadStore.remove();
	}

	@ParameterizedTest
	@CsvSource({ "READ_ONLY,true,false,false", "EDIT,true,true,false", "OWNER,true,true,true" })
	void aUserCallsTheMethodsTheirLevelCovers(AccessPermissionEnum held, boolean reads, boolean writes,
			boolean deletes) {
		userHolds(ENGINE, false, held);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertEquals(reads, allowed(engine::getEngineId));
		assertEquals(writes, allowed(() -> engine.insertData("insert into t values (1)")));
		assertEquals(deletes, allowed(engine::delete));
	}

	@Test
	void aUserWithNoPermissionIsRefusedEveryMethod() {
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, engine::getEngineId);

		assertEquals("Engine " + ENGINE + " does not exist or user does not have READ_ONLY permission on it",
				refusal.getMessage());
		assertThrows(SemossPixelException.class, engine::getConnection);
		assertThrows(SemossPixelException.class, engine::getSmssProp);
		assertThrows(SemossPixelException.class, engine::delete);
	}

	@Test
	void aRefusalNamesTheLevelTheMethodNeeds() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, engine::commit);

		assertEquals("Engine " + ENGINE + " does not exist or user does not have EDIT permission on it",
				refusal.getMessage());
	}

	@Test
	void thePlatformGetsAConnectionAndOneSettingAtATimeForAUserWhoCanView() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertDoesNotThrow(engine::getConnection);
		assertDoesNotThrow(() -> engine.getPreparedStatement("select 1"));
		assertDoesNotThrow(engine::getQueryUtil);
		assertDoesNotThrow(() -> engine.getCatalogSubType());
		assertDoesNotThrow(engine::isBigData);
		assertDoesNotThrow(engine::getPipelineFile);
		assertDoesNotThrow(engine::close);
	}

	@ParameterizedTest
	@CsvSource({ "READ_ONLY", "EDIT" })
	void onlyAnOwnerReadsTheSettingsOrPicksTheFileTheEngineOpensFrom(AccessPermissionEnum held) {
		userHolds(ENGINE, false, held);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertThrows(SemossPixelException.class, engine::getSmssProp);
		assertThrows(SemossPixelException.class, engine::getOrigSmssProp);
		assertThrows(SemossPixelException.class, engine::getSmssFilePath);
		assertThrows(SemossPixelException.class, () -> engine.setSmssProp(new Properties()));
		assertThrows(SemossPixelException.class, () -> engine.open("other.smss"));
		assertThrows(SemossPixelException.class, () -> engine.open(new Properties()));
		assertThrows(SemossPixelException.class, () -> engine.getProperty("PASSWORD"));
		assertThrows(SemossPixelException.class, engine::getConnectionUrl);
		assertThrows(SemossPixelException.class, engine::getDataSource);
	}

	@Test
	void anOwnerReadsTheSettingsAndPicksTheFileTheEngineOpensFrom() {
		userHolds(ENGINE, false, AccessPermissionEnum.OWNER);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertNotNull(engine.getSmssProp());
		assertDoesNotThrow(engine::getSmssFilePath);
		assertDoesNotThrow(() -> engine.open("other.smss"));
		assertDoesNotThrow(engine::getConnectionUrl);
	}

	@Test
	void anEditorReloadsTheEngineAndChangesOnlyTheSettingsEditorsChange() {
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		assertThrows(SemossPixelException.class, engine::reload);
		assertThrows(SemossPixelException.class, () -> engine.updateSetting(IEngine.PIPELINE, "pipeline.json"));

		userHolds(ENGINE, false, AccessPermissionEnum.EDIT);
		assertDoesNotThrow(engine::reload);
		assertDoesNotThrow(() -> engine.updateSetting(IEngine.PIPELINE, "pipeline.json"));
	}

	@Test
	void permissionOnOneEngineGivesNothingOnAnother() {
		userHolds(ENGINE, false, AccessPermissionEnum.OWNER);

		assertThrows(SemossPixelException.class, StubEngines.database(OTHER_ENGINE)::getEngineId);
	}

	@Test
	void anAdminCallsAnyMethodThroughAnAdminClass() {
		this.access.when(() -> SecurityEngineAccessUtils.isAdmin(this.user)).thenReturn(true);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertDoesNotThrow(() -> AdminEngineCalls.run(engine::getEngineId));
		assertDoesNotThrow(() -> AdminEngineCalls.run(engine::delete));
	}

	@Test
	void anAdminHasOnlyTheirOwnPermissionsOutsideAnAdminClass() {
		this.access.when(() -> SecurityEngineAccessUtils.isAdmin(this.user)).thenReturn(true);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, engine::getEngineId);

		assertEquals("Engine " + ENGINE + " does not exist or user does not have READ_ONLY permission on it",
				refusal.getMessage());
		assertThrows(SemossPixelException.class, engine::delete);

		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		assertDoesNotThrow(engine::getEngineId);
		assertThrows(SemossPixelException.class, engine::delete);
	}

	@Test
	void aUserWhoIsNotAnAdminGainsNothingFromAnAdminClass() {
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertThrows(SemossPixelException.class, () -> AdminEngineCalls.run(engine::getEngineId));
	}

	@Test
	void anAdminClassWithNoSignedInUserIsRefused() {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertThrows(SemossPixelException.class, () -> AdminEngineCalls.run(engine::getEngineId));
	}

	@Test
	void aCallWithNoSignedInUserIsRefused() {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, engine::getEngineId);

		assertEquals("Engine " + ENGINE + " does not exist or user does not have READ_ONLY permission on it",
				refusal.getMessage());
		assertThrows(SemossPixelException.class, engine::delete);
	}

	@Test
	void aCallWithNoSignedInUserIsRefusedOnAnEngineTheSecurityDatabaseDoesNotList() {
		ThreadStore.remove();
		this.access.when(() -> SecurityEngineAccessUtils.isListed(ENGINE, false)).thenReturn(false);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertThrows(SemossPixelException.class, engine::getEngineId);
	}

	@Test
	void aPlatformOperationGoesThroughWithNoSignedInUser() {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
			assertEquals("cluster sync", operation.getName());
			assertDoesNotThrow(engine::getEngineId);
			assertDoesNotThrow(engine::delete);
		}
	}

	@Test
	void aPlatformOperationGoesThroughForAUserWithNoPermission() {
		IRDBMSEngine engine = StubEngines.database(ENGINE);
		assertThrows(SemossPixelException.class, engine::getEngineId);

		try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
			assertDoesNotThrow(engine::getEngineId);
		}
	}

	@Test
	void aPlatformOperationEndsWhenItIsClosed() {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);
		PlatformOperation operation = PlatformOperation.begin("cluster sync");
		assertDoesNotThrow(engine::getEngineId);

		operation.close();

		assertThrows(SemossPixelException.class, engine::getEngineId);
		assertDoesNotThrow(operation::close);
	}

	@Test
	void aPlatformOperationBegunInsideAnotherLeavesTheOuterOneRunning() {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		try (PlatformOperation outer = PlatformOperation.begin("cluster sync")) {
			try (PlatformOperation inner = PlatformOperation.begin("git sync")) {
				assertDoesNotThrow(engine::getEngineId);
			}
			assertDoesNotThrow(engine::getEngineId);
		}
		assertThrows(SemossPixelException.class, engine::getEngineId);
	}

	@Test
	void anOuterPlatformOperationClosedFirstIsNotPutBack() {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);
		PlatformOperation outer = PlatformOperation.begin("cluster sync");
		PlatformOperation inner = PlatformOperation.begin("git sync");

		outer.close();
		assertDoesNotThrow(engine::getEngineId);
		inner.close();

		assertThrows(SemossPixelException.class, engine::getEngineId);
	}

	@Test
	void aPlatformOperationIsNotCarriedToAThreadTheWorkStarts() throws Exception {
		AtomicReference<String> onAnotherThread = new AtomicReference<>("not read");

		try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
			assertEquals("cluster sync", PlatformOperation.covering());
			Thread worker = new Thread(() -> onAnotherThread.set(PlatformOperation.covering()));
			worker.start();
			worker.join();
		}

		assertNull(onAnotherThread.get());
		assertNull(PlatformOperation.covering());
	}

	@Test
	void aPlatformOperationClosedOnAnotherThreadKeepsRunning() throws Exception {
		ThreadStore.remove();
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		try (PlatformOperation operation = PlatformOperation.begin("cluster sync")) {
			Thread worker = new Thread(operation::close);
			worker.start();
			worker.join();

			assertDoesNotThrow(engine::getEngineId);
		}
	}

	@Test
	void aPlatformOperationNeedsAName() {
		assertThrows(IllegalArgumentException.class, () -> PlatformOperation.begin(" "));
		assertThrows(IllegalArgumentException.class, () -> PlatformOperation.begin(null));
	}

	@Test
	void aUserOffTheThreadIsReadFromTheInsightOfTheCall() {
		ThreadStore.remove();
		Insight insight = new Insight();
		insight.setUser(this.user);
		IModelEngine model = StubEngines.model(ENGINE);
		Map<String, Object> parameters = new HashMap<>();

		assertThrows(SemossPixelException.class, () -> model.ask("question", null, insight, parameters));

		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		assertDoesNotThrow(() -> model.ask("question", null, insight, parameters));
	}

	@Test
	void thePlatformReadsADiscoverableEngineForAUserWithNoPermission() {
		this.access.when(() -> SecurityEngineAccessUtils.isReadableThroughPlatform(this.user, ENGINE, false))
				.thenReturn(true);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertDoesNotThrow(engine::getEngineId);
		assertDoesNotThrow(() -> engine.getCatalogSubType());
		assertThrows(SemossPixelException.class, engine::getSmssProp);
		assertThrows(SemossPixelException.class, () -> engine.insertData("insert into t values (1)"));
		assertThrows(SemossPixelException.class, engine::delete);
	}

	@Test
	void thePlatformReadsAProjectForAUserWhoCanOpenOneOfItsInsights() {
		this.access.when(() -> SecurityEngineAccessUtils.isReadableThroughPlatform(this.user, PROJECT, true))
				.thenReturn(true);
		IProject project = StubEngines.project(PROJECT);

		assertDoesNotThrow(project::getProjectId);
		assertDoesNotThrow(project::getInsightDatabase);
		assertThrows(SemossPixelException.class, project::writeNotebooks);
	}

	@Test
	void aProjectIsCheckedAgainstTheUsersPermissionOnTheProject() {
		IProject project = StubEngines.project(PROJECT);

		SemossPixelException refusal = assertThrows(SemossPixelException.class, project::getProjectId);
		assertEquals("Project " + PROJECT + " does not exist or user does not have READ_ONLY permission on it",
				refusal.getMessage());

		userHolds(PROJECT, true, AccessPermissionEnum.READ_ONLY);
		assertDoesNotThrow(project::getProjectId);
	}

	@Test
	void anEngineTheSecurityDatabaseDoesNotListGoesThrough() {
		this.access.when(() -> SecurityEngineAccessUtils.isListed(ENGINE, false)).thenReturn(false);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertDoesNotThrow(engine::delete);
	}

	@Test
	void anEngineCallingAnEngineThatGaveItNothingIsRefused() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE);
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", configured::getEngineName));

		SemossPixelException refusal = assertThrows(SemossPixelException.class, engine::getSchema);

		assertTrue(refusal.getMessage().contains("engine " + ENGINE), refusal.getMessage());
	}

	@Test
	void anEngineCallsAnEngineThatGaveItAPermissionThatCoversTheMethod() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		engineHolds(OTHER_ENGINE, new EngineGrantee(ENGINE, "DATABASE"), AccessPermissionEnum.READ_ONLY);
		AtomicReference<EngineGrantee> seenByTheConfiguredEngine = new AtomicReference<>();
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE, Map.of("getSchema", () -> {
			seenByTheConfiguredEngine.set(EngineCallPermission.granteeMakingTheCall());
			return null;
		}));
		IRDBMSEngine reads = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			configured.getSchema();
			return null;
		}));
		IRDBMSEngine deletes = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			configured.delete();
			return null;
		}));

		assertDoesNotThrow(reads::getSchema);
		assertEquals(new EngineGrantee(ENGINE, "DATABASE"), seenByTheConfiguredEngine.get());
		assertThrows(SemossPixelException.class, deletes::getSchema);
		assertThrows(SemossPixelException.class, configured::getSchema);
		assertNull(EngineCallPermission.granteeMakingTheCall());
	}

	@Test
	void anEngineThatGaveAnotherEditorStillKeepsItsSettings() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		engineHolds(OTHER_ENGINE, new EngineGrantee(ENGINE, "DATABASE"), AccessPermissionEnum.EDIT);
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE);
		IRDBMSEngine readsOneSetting = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			configured.getCatalogSubType();
			return null;
		}));
		IRDBMSEngine readsTheSettings = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			configured.getSmssProp();
			return null;
		}));

		assertDoesNotThrow(readsOneSetting::getSchema);
		assertThrows(SemossPixelException.class, readsTheSettings::getSchema);
	}

	@Test
	void aPermissionGivenToAnAppDoesNotCoverAnEngineWithTheSameId() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		engineHolds(OTHER_ENGINE, EngineGrantee.ofApp(ENGINE), AccessPermissionEnum.EDIT);
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE);
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", configured::getEngineName));

		assertThrows(SemossPixelException.class, engine::getSchema);
	}

	@Test
	void anEngineCallingAnEngineTheUserCanUseGoesThrough() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		userHolds(OTHER_ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE);
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", configured::getEngineName));

		assertEquals("Stub " + OTHER_ENGINE, engine.getSchema());
	}

	@Test
	void anEngineCallingItselfGoesThrough() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IRDBMSEngine itself = StubEngines.database(ENGINE);
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			itself.delete();
			return null;
		}));

		assertDoesNotThrow(engine::getSchema);
	}

	@Test
	void anEngineReadsAPublicEngineWithNoPermission() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		this.access.when(() -> SecurityEngineAccessUtils.isPublic(OTHER_ENGINE)).thenReturn(true);
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE);
		IRDBMSEngine reads = StubEngines.database(ENGINE, Map.of("getSchema", configured::getEngineName));
		IRDBMSEngine deletes = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			configured.delete();
			return null;
		}));

		assertEquals("Stub " + OTHER_ENGINE, reads.getSchema());
		assertThrows(SemossPixelException.class, deletes::getSchema);
	}

	@Test
	void anEngineUsesAVenvEngineWithNoPermission() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IVenvEngine venv = StubEngines.venv(OTHER_ENGINE);
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", venv::pathToExecutable));

		assertDoesNotThrow(engine::getSchema);
		assertThrows(SemossPixelException.class, venv::pathToExecutable);
	}

	@Test
	void anEngineOpeningCallsAnotherEngineAsItself() {
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE);
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			utility.when(() -> Utility.getDatabase(eq(ENGINE))).thenAnswer(loading -> {
				try (EngineCallPermission.Opening opening = EngineCallPermission.opening(ENGINE, "DATABASE")) {
					SandboxProbe.heldObject = configured;
					SandboxProbe.readHeldEngineSettingsAsAPlatformEngine();
					return configured;
				} finally {
					SandboxProbe.heldObject = null;
				}
			});

			assertThrows(SemossPixelException.class, () -> Utility.getDatabase(ENGINE));
			engineHolds(OTHER_ENGINE, new EngineGrantee(ENGINE, "DATABASE"), AccessPermissionEnum.OWNER);
			assertDoesNotThrow(() -> Utility.getDatabase(ENGINE));
		}
	}

	@Test
	void aReactorAUserRunsInsideAnEngineCallIsCheckedAsTheUser() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		IRDBMSEngine other = StubEngines.database(OTHER_ENGINE);
		IRDBMSEngine engine = StubEngines.database(ENGINE,
				Map.of("getSchema", () -> new ReadsAnEngine(other).execute().getValue()));

		assertThrows(SemossPixelException.class, engine::getSchema);
	}

	@Test
	void thePlatformLoadingAnEngineGoesThrough() {
		IRDBMSEngine engine = StubEngines.database(ENGINE);
		try (MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
			utility.when(() -> Utility.getDatabase(eq(ENGINE))).thenAnswer(loading -> {
				engine.getSmssProp();
				return engine;
			});

			assertDoesNotThrow(() -> Utility.getDatabase(ENGINE));
		}
		assertThrows(SemossPixelException.class, engine::getSmssProp);
	}

	@Test
	void aProxyHandedOutForThePlatformsOwnUseServesAnyUser() {
		IModelEngine platformModel = StubEngines.platformModel(ENGINE);
		IModelEngine model = StubEngines.model(ENGINE);

		assertDoesNotThrow(platformModel::getModelType);
		assertThrows(SemossPixelException.class, model::getModelType);
	}

	@Test
	void methodsOfObjectAreNotChecked() {
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertDoesNotThrow(engine::toString);
		assertDoesNotThrow(engine::hashCode);
		assertDoesNotThrow(() -> engine.equals(engine));
	}

	@Test
	void aJvmWithoutTheSecurityDatabaseChecksNothing() {
		this.access.when(SecurityEngineAccessUtils::isSecurityDatabaseLoaded).thenReturn(false);
		IRDBMSEngine engine = StubEngines.database(ENGINE);

		assertDoesNotThrow(engine::delete);
		this.access.verify(() -> SecurityEngineAccessUtils.getUserPermission(any(), anyString(), anyBoolean()),
				Mockito.never());
	}

	@Test
	void anAgentRunCallsAnEngineThatGaveTheAgentAPermission() {
		AtomicReference<EngineGrantee> seenByTheEngine = new AtomicReference<>();
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			seenByTheEngine.set(EngineCallPermission.granteeMakingTheCall());
			return null;
		}));
		engineHolds(ENGINE, EngineGrantee.ofApp(AGENT), AccessPermissionEnum.READ_ONLY);

		assertThrows(SemossPixelException.class, engine::getSchema);
		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			assertDoesNotThrow(engine::getSchema);
			assertEquals(EngineGrantee.ofApp(AGENT), seenByTheEngine.get());
			SemossPixelException refusal = assertThrows(SemossPixelException.class, engine::delete);
			assertTrue(refusal.getMessage().contains("agent " + AGENT), refusal.getMessage());
		}
		assertThrows(SemossPixelException.class, engine::getSchema);
	}

	@Test
	void anAgentRunReadsAProjectThatGaveTheAgentAPermission() {
		IProject project = StubEngines.project(PROJECT);
		projectHolds(PROJECT, EngineGrantee.ofApp(AGENT), AccessPermissionEnum.READ_ONLY);

		assertThrows(SemossPixelException.class, project::getProjectName);
		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			assertDoesNotThrow(project::getProjectName);
		}
	}

	@Test
	void aPermissionGivenToAnAgentOnAnEngineDoesNotCoverAProjectWithTheSameId() {
		IProject project = StubEngines.project(PROJECT);
		engineHolds(PROJECT, EngineGrantee.ofApp(AGENT), AccessPermissionEnum.READ_ONLY);

		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			assertThrows(SemossPixelException.class, project::getProjectName);
		}
	}

	@Test
	void anEngineCallInsideAnAgentRunCountsTheAgentAsTheAppBehindIt() {
		userHolds(ENGINE, false, AccessPermissionEnum.READ_ONLY);
		AtomicReference<EngineGrantee> seenByTheConfiguredEngine = new AtomicReference<>();
		IRDBMSEngine configured = StubEngines.database(OTHER_ENGINE, Map.of("getSchema", () -> {
			seenByTheConfiguredEngine.set(EngineCallPermission.granteeMakingTheCall());
			return null;
		}));
		IRDBMSEngine engine = StubEngines.database(ENGINE, Map.of("getSchema", () -> {
			configured.getSchema();
			return null;
		}));
		engineHolds(OTHER_ENGINE, EngineGrantee.ofApp(AGENT), AccessPermissionEnum.READ_ONLY);

		assertThrows(SemossPixelException.class, engine::getSchema);
		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			assertDoesNotThrow(engine::getSchema);
		}
		assertEquals(EngineGrantee.ofApp(AGENT), seenByTheConfiguredEngine.get());
	}

	@Test
	void anAgentRunEndsWhenItIsClosedAndAnInnerRunPutsBackTheOuterOne() {
		try (AgentRunOperation outer = AgentRunOperation.begin(AGENT)) {
			try (AgentRunOperation inner = AgentRunOperation.begin("agent-2")) {
				assertEquals("agent-2", AgentRunOperation.runningAgent());
			}
			assertEquals(AGENT, AgentRunOperation.runningAgent());
		}
		assertNull(AgentRunOperation.runningAgent());
		assertThrows(IllegalArgumentException.class, () -> AgentRunOperation.begin(" "));
	}

	@Test
	void anAgentRunIsNotCarriedToAThreadItStarts() throws Exception {
		AtomicReference<String> seenOnTheThread = new AtomicReference<>("not read");
		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			Thread thread = new Thread(() -> seenOnTheThread.set(AgentRunOperation.runningAgent()));
			thread.start();
			thread.join();
		}
		assertNull(seenOnTheThread.get());
	}

	private void userHolds(String id, boolean project, AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.getUserPermission(this.user, id, project)).thenReturn(level);
	}

	private void engineHolds(String engineId, EngineGrantee grantee, AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.givesAGranteePermission(engineId, false)).thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.getGranteePermission(grantee, engineId, false))
				.thenReturn(level);
	}

	private void projectHolds(String projectId, EngineGrantee grantee, AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.givesAGranteePermission(projectId, true)).thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.getGranteePermission(grantee, projectId, true))
				.thenReturn(level);
	}

	private static boolean allowed(Executable call) {
		try {
			call.execute();
			return true;
		} catch (SemossPixelException refused) {
			return false;
		} catch (Throwable other) {
			throw new AssertionError(other);
		}
	}

	/**
	 * A reactor of the platform that reads one engine, as a tool a user runs does.
	 */
	private static final class ReadsAnEngine extends AbstractReactor {

		private final IRDBMSEngine engine;

		private ReadsAnEngine(IRDBMSEngine engine) {
			this.engine = engine;
		}

		@Override
		public NounMetadata execute() {
			return new NounMetadata(this.engine.getEngineName(), PixelDataType.CONST_STRING);
		}

	}

}
