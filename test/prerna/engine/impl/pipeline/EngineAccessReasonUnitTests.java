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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.auth.access.UserAccess;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.logging.SemossLogUtils;
import prerna.om.ThreadStore;
import prerna.usertracking.UserAuditTrailUtils;

/**
 * Why the access check let a call through, as the Activity Log records it: the
 * reason and its details for each rule, the reason on the row a call logs, and
 * a refused call's row, counted per session with only the first on the audit
 * trail. Each test is one rule of docs/platform_services/engine_access.md.
 */
class EngineAccessReasonUnitTests {

	private static final String ENGINE = "reason-engine";
	private static final String PROJECT = "reason-project";
	private static final String AGENT = "reason-agent";
	private static final AtomicInteger NEXT_ENGINE = new AtomicInteger();

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

	@Test
	void theUsersOwnGrantSaysDirectWithItsLevel() throws Exception {
		userHas(ENGINE, false, UserAccess.direct(AccessPermissionEnum.EDIT));

		EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, insertData());

		assertEquals(EngineAccessReason.DIRECT, decision.reason());
		assertEquals(Map.of("level", "EDIT"), decision.details());
	}

	@Test
	void aTeamsGrantSaysTeamWithTheTeam() throws Exception {
		userHas(ENGINE, false, UserAccess.team(AccessPermissionEnum.EDIT, "ops", "CUSTOM"));

		EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, insertData());

		assertEquals(EngineAccessReason.TEAM, decision.reason());
		assertEquals(Map.of("level", "EDIT", "teamId", "ops", "teamType", "CUSTOM"), decision.details());
	}

	@Test
	void aPublicEngineSaysPublic() throws Exception {
		userHas(ENGINE, false, UserAccess.publicRead());

		EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, getEngineId());

		assertEquals(EngineAccessReason.PUBLIC, decision.reason());
	}

	@Test
	void anAgentsRunSaysDelegatedWithTheAgent() throws Exception {
		engineGives(ENGINE, EngineGrantee.ofApp(AGENT), AccessPermissionEnum.READ_ONLY);

		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, getEngineId());

			assertEquals(EngineAccessReason.DELEGATED, decision.reason());
			assertEquals(EngineGrantee.ofApp(AGENT), decision.grantee());
			assertEquals(Map.of("level", "READ_ONLY", "granteeId", AGENT, "granteeType", "PROJECT", "via",
					EngineAccessDecision.VIA_AGENT_RUN), decision.details());
		}
	}

	@Test
	void thePlatformsOwnWorkSaysPlatformWithWhatItWas() throws Exception {
		ThreadStore.remove();
		try (PlatformOperation operation = PlatformOperation.begin("sync files")) {
			EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, insertData());

			assertEquals(EngineAccessReason.PLATFORM, decision.reason());
			assertEquals(Map.of("rule", "PLATFORM_OPERATION", "operation", "sync files"), decision.details());
		}
		EngineAccessDecision forItsOwnUse = EngineAccessGuard.check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, true,
				insertData(), null);
		assertEquals(Map.of("rule", "PLATFORM_USE"), forItsOwnUse.details());
	}

	@Test
	void aReadThroughThePlatformSaysDiscoverableOrInsight() throws Exception {
		this.access.when(() -> SecurityEngineAccessUtils.isReadableThroughPlatform(this.user, ENGINE, false))
				.thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.isReadableThroughPlatform(this.user, PROJECT, true))
				.thenReturn(true);

		assertEquals(EngineAccessReason.DISCOVERABLE,
				check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, getEngineId()).reason());
		assertEquals(EngineAccessReason.INSIGHT, check(PROJECT, IEngine.CATALOG_TYPE.PROJECT, getEngineId()).reason());

		this.access.when(() -> SecurityEngineAccessUtils.isDiscoverable(PROJECT, true)).thenReturn(true);
		assertEquals(EngineAccessReason.DISCOVERABLE,
				check(PROJECT, IEngine.CATALOG_TYPE.PROJECT, getEngineId()).reason());
	}

	@Test
	void anEngineBeingCreatedOrDeletedSaysPlatform() throws Exception {
		this.access.when(() -> SecurityEngineAccessUtils.isListed(ENGINE, false)).thenReturn(false);

		EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, insertData());

		assertEquals(Map.of("rule", "BEING_CREATED_OR_DELETED"), decision.details());
	}

	@Test
	void anAdminInAnAdminClassSaysAdmin() throws Throwable {
		this.access.when(() -> SecurityEngineAccessUtils.isAdmin(this.user)).thenReturn(true);
		Method delete = IEngine.class.getMethod("delete");

		AdminEngineCalls.run(() -> assertEquals(EngineAccessReason.ADMIN,
				check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, delete).reason()));
	}

	@Test
	void aRefusalCarriesTheLevelNeededAndWhoMadeTheCall() throws Exception {
		Method insertData = insertData();
		EngineAccessRefusedException refusal = assertThrows(EngineAccessRefusedException.class,
				() -> check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, insertData));
		assertEquals(Map.of("levelNeeded", "EDIT", "via", EngineAccessDecision.VIA_PLATFORM), refusal.getDetails());

		try (AgentRunOperation run = AgentRunOperation.begin(AGENT)) {
			EngineAccessRefusedException inARun = assertThrows(EngineAccessRefusedException.class,
					() -> check(ENGINE, IEngine.CATALOG_TYPE.DATABASE, insertData));
			assertEquals(Map.of("levelNeeded", "EDIT", "via", EngineAccessDecision.VIA_AGENT_RUN, "agentId", AGENT),
					inARun.getDetails());
			assertEquals(EngineAccessReason.REFUSED, inARun.decision(2).reason());
			assertEquals(2, inARun.decision(2).details().get("attempt"));
		}
	}

	@Test
	void aMethodOfObjectIsNotChecked() throws Exception {
		EngineAccessDecision decision = check(ENGINE, IEngine.CATALOG_TYPE.DATABASE,
				Object.class.getMethod("toString"));

		assertNull(decision.reason());
	}

	@Test
	void aLoggedCallsRowCarriesItsReasonAndDetails() throws Exception {
		String engineId = nextEngineId();
		userHas(engineId, false, UserAccess.team(AccessPermissionEnum.EDIT, "ops", "CUSTOM"));
		Logger engineLogger = mock(Logger.class);
		IRDBMSEngine engine = StubEngines.database(engineId, Map.of("getEngineLogger", () -> engineLogger));

		engine.insertData("insert into t values (1)");

		Map<?, ?> row = loggedRow(engineLogger);
		assertEquals("TEAM", row.get(SemossLogUtils.ACCESS_REASON));
		assertEquals("{\"level\":\"EDIT\",\"teamId\":\"ops\",\"teamType\":\"CUSTOM\"}",
				row.get(SemossLogUtils.ACCESS_DETAILS));
	}

	@Test
	void anEnginesOwnCodeUsingItsPermissionSaysDelegatedWithTheCallingEngine() throws Exception {
		String calledId = nextEngineId();
		String callingId = nextEngineId();
		userHas(callingId, false, UserAccess.direct(AccessPermissionEnum.READ_ONLY));
		engineGives(calledId, new EngineGrantee(callingId, "DATABASE"), AccessPermissionEnum.EDIT);
		Logger calledLogger = mock(Logger.class);
		IRDBMSEngine called = StubEngines.database(calledId, Map.of("getEngineLogger", () -> calledLogger));
		IRDBMSEngine calling = StubEngines.database(callingId, Map.of("getSchema", () -> {
			called.insertData("insert into t values (1)");
			return null;
		}));

		calling.getSchema();

		Map<?, ?> row = loggedRow(calledLogger);
		assertEquals("DELEGATED", row.get(SemossLogUtils.ACCESS_REASON));
		assertEquals("{\"level\":\"EDIT\",\"granteeId\":\"" + callingId
				+ "\",\"granteeType\":\"DATABASE\",\"via\":\"ENGINE_CODE\",\"callingEngineId\":\"" + callingId + "\"}",
				row.get(SemossLogUtils.ACCESS_DETAILS));
	}

	@Test
	void aRefusedCallLogsAFailedRowWithNothingSentAndOnlyTheFirstInASessionIsAudited() {
		String engineId = nextEngineId();
		Logger engineLogger = mock(Logger.class);
		IRDBMSEngine engine = StubEngines.database(engineId, Map.of("getEngineLogger", () -> engineLogger));

		try (MockedStatic<UserAuditTrailUtils> audit = Mockito.mockStatic(UserAuditTrailUtils.class)) {
			assertThrows(EngineAccessRefusedException.class, () -> engine.insertData("insert into t values (1)"));
			assertThrows(EngineAccessRefusedException.class, () -> engine.insertData("insert into t values (2)"));

			audit.verify(() -> UserAuditTrailUtils.recordEvent(any()), times(1));
		}
		List<Map<?, ?>> rows = loggedRows(engineLogger, 2);
		Map<?, ?> first = rows.get(0);
		assertEquals(Boolean.FALSE, first.get(SemossLogUtils.IS_SUCCESS));
		assertEquals("REFUSED", first.get(SemossLogUtils.ACCESS_REASON));
		assertEquals("REQUEST NOT TRACKED", first.get(SemossLogUtils.REQUEST));
		String details = String.valueOf(first.get(SemossLogUtils.ACCESS_DETAILS));
		assertTrue(details.contains("\"levelNeeded\":\"EDIT\"") && details.contains("\"attempt\":1"), details);
		assertTrue(String.valueOf(rows.get(1).get(SemossLogUtils.ACCESS_DETAILS)).contains("\"attempt\":2"));
	}

	private EngineAccessDecision check(String id, IEngine.CATALOG_TYPE type, Method method) {
		return EngineAccessGuard.check(id, type, false, method, null);
	}

	private void userHas(String id, boolean project, UserAccess userAccess) {
		this.access.when(() -> SecurityEngineAccessUtils.getUserAccess(this.user, id, project)).thenReturn(userAccess);
	}

	private void engineGives(String engineId, EngineGrantee grantee, AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.givesAGranteePermission(engineId, false)).thenReturn(true);
		this.access.when(() -> SecurityEngineAccessUtils.getGranteePermission(grantee, engineId, false))
				.thenReturn(level);
	}

	private static Method insertData() throws NoSuchMethodException {
		return IRDBMSEngine.class.getMethod("insertData", String.class);
	}

	private static Method getEngineId() throws NoSuchMethodException {
		return IEngine.class.getMethod("getEngineId");
	}

	/** An engine id no other test uses, since refusals are counted across tests */
	private static String nextEngineId() {
		return ENGINE + "-" + NEXT_ENGINE.incrementAndGet();
	}

	private static Map<?, ?> loggedRow(Logger engineLogger) {
		return loggedRows(engineLogger, 1).get(0);
	}

	/**
	 * The rows the engine's logger was given, in order.
	 *
	 * @param engineLogger the logger, a mock
	 * @param count        how many rows there should be
	 * @return the rows
	 */
	private static List<Map<?, ?>> loggedRows(Logger engineLogger, int count) {
		ArgumentCaptor<Object> rows = ArgumentCaptor.forClass(Object.class);
		verify(engineLogger, times(count)).info(rows.capture());
		return rows.getAllValues().stream().<Map<?, ?>>map(row -> (Map<?, ?>) row).toList();
	}

}
