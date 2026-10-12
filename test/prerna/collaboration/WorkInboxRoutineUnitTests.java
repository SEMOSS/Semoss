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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.MockedConstruction;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.model.RoomUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunHandle;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry.Subscription;
import prerna.om.Insight;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class WorkInboxRoutineUnitTests {
	private Connection connection;
	private MockedStatic<SystemEngineRegistry> registry;
	private MockedStatic<Utility> utility;
	private MockedStatic<BrainTopicModel> models;
	private MockedStatic<MicrosoftGraphSubscriptionRegistry> subscriptions;
	private MockedStatic<MicrosoftGraphCredentialStore> credentials;
	private Map<String, AccessToken> savedTokens;
	private User user;
	private IRDBMSEngine engine;

	@BeforeEach
	void setUp() throws Exception {
		connection = DriverManager.getConnection("jdbc:h2:mem:inbox_routine_" + UUID.randomUUID());
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		engine = mock(IRDBMSEngine.class);
		when(engine.getConnection()).thenReturn(connection);
		when(engine.getQueryUtil()).thenReturn(queryUtil);
		when(engine.getPreparedStatement(anyString())).thenAnswer(call -> connection.prepareStatement(call.getArgument(0)));
		registry = mockStatic(SystemEngineRegistry.class);
		registry.when(SystemEngineRegistry::getCollaborationDb).thenReturn(engine);
		try (Statement statement = connection.createStatement()) {
			for (Pair<String, List<Pair<String, String>>> table : new CollaborationOwlCreator(queryUtil).getDBSchema()) {
				List<String> columns = new ArrayList<>();
				for (Pair<String, String> column : table.getValue1()) {
					columns.add(column.getValue0() + " " + column.getValue1());
				}
				statement.execute("CREATE TABLE " + table.getValue0() + " (" + String.join(", ", columns) + ")");
			}
		}
		user = owner("owner-one");
		utility = mockStatic(Utility.class);
		utility.when(Utility::getCurrentSqlTimestampUTC).thenAnswer(call -> java.sql.Timestamp.valueOf(
				java.time.LocalDateTime.now(java.time.ZoneOffset.UTC)));
		models = mockStatic(BrainTopicModel.class);
		subscriptions = mockStatic(MicrosoftGraphSubscriptionRegistry.class);
		credentials = mockStatic(MicrosoftGraphCredentialStore.class);
		savedTokens = new HashMap<>();
		credentials.when(() -> MicrosoftGraphCredentialStore.isRefreshable(any())).thenCallRealMethod();
		credentials.when(() -> MicrosoftGraphCredentialStore.get(any())).thenAnswer(call -> savedTokens.get(call.getArgument(0)));
		credentials.when(() -> MicrosoftGraphCredentialStore.save(any())).thenAnswer(call -> {
			AccessToken token = call.getArgument(0);
			savedTokens.put(token.getId(), AccessToken.copyToken(token));
			return null;
		});
		credentials.when(() -> MicrosoftGraphCredentialStore.accessToken(anyString())).thenAnswer(call ->
				savedTokens.get(call.getArgument(0)).getAccess_token());
	}

	@AfterEach
	void tearDown() throws Exception {
		credentials.close();
		subscriptions.close();
		models.close();
		utility.close();
		registry.close();
		connection.close();
	}

	@Test
	void newRoutineDefaultsToOptedOutAndNeverStartsWork() {
		Map<String, Object> result = WorkInboxRoutine.get(user);
		assertEquals(false, result.get("enabled"));
		assertEquals(false, result.get("configured"));
		assertEquals(25, result.get("emailCount"));
		assertNull(result.get("nextRunAt"));
		assertNull(result.get("lastRun"));
		assertEquals(0, CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_INBOX_ROUTINE"));
	}

	@Test
	void loadingRoutineChecksSchedulerAvailabilityWithoutRequestingDatabaseAccess() {
		registry.when(SystemEngineRegistry::getSchedulerDb).thenThrow(new SecurityException(
				"Unauthorized access to SchedulerDb from: prerna.collaboration.WorkInboxRoutine"));
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(true);
		utility.when(() -> Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT)).thenReturn("https://semoss.test/Monolith");
		assertEquals(true, WorkInboxRoutine.get(user).get("schedulerAvailable"));
		registry.verify(SystemEngineRegistry::getSchedulerDb, never());
	}

	@Test
	void ownerCanSaveDisabledPreferencesWithoutMailboxModelOrScheduler() {
		Map<String, Object> result = WorkInboxRoutine.save(user, config(false));
		assertEquals(true, result.get("configured"));
		assertEquals(false, result.get("enabled"));
		assertEquals(false, result.get("mailboxAvailable"));
		assertEquals(false, result.get("assistantAvailable"));
		assertEquals(false, result.get("schedulerAvailable"));
		assertEquals(false, WorkInboxRoutine.get(owner("someone-else")).get("configured"));
	}

	@Test
	void invalidSettingsCannotCreateOrChangeAStoredRoutine() {
		List<Map<String, Object>> invalid = new ArrayList<>();
		for (Object days : List.of(List.of(), List.of("MON", "MON"), List.of("MON", "BOGUS"))) {
			Map<String, Object> input = config(false); input.put("days", days); invalid.add(input);
		}
		for (Object count : List.of(0, 101, 1.5, "25")) {
			Map<String, Object> input = config(false); input.put("emailCount", count); invalid.add(input);
		}
		for (String time : List.of("24:00", "08:60", "8:00", "12:00:30")) {
			Map<String, Object> input = config(false); input.put("time", time); invalid.add(input);
		}
		Map<String, Object> timezone = config(false); timezone.put("timezone", "Missing/Zone"); invalid.add(timezone);
		Map<String, Object> enabled = config(false); enabled.put("enabled", "true"); invalid.add(enabled);
		Map<String, Object> owner = config(false); owner.put("ownerId", "another-user"); invalid.add(owner);
		invalid.forEach(input -> assertThrows(IllegalArgumentException.class, () -> WorkInboxRoutine.save(user, input)));
		assertEquals(0, CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_INBOX_ROUTINE"));
	}

	@Test
	void scheduleUsesSelectedDaysAndLocalTimeAcrossDaylightSaving() throws Exception {
		Map<String, Object> input = config(true);
		input.put("days", List.of("FRI", "MON"));
		Map<String, Object> valid = WorkInboxRoutine.validate(input);
		assertEquals("0 0 8 ? * MON,FRI", WorkInboxRoutine.cron(valid));
		CronExpression expression = new CronExpression(WorkInboxRoutine.cron(valid));
		expression.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		assertEquals(Instant.parse("2026-10-30T12:00:00Z"), expression.getNextValidTimeAfter(
				Date.from(Instant.parse("2026-10-30T11:59:59Z"))).toInstant());
		assertEquals(Instant.parse("2026-11-02T13:00:00Z"), expression.getNextValidTimeAfter(
				Date.from(Instant.parse("2026-10-30T12:00:01Z"))).toInstant());
	}

	@Test
	void enablingRequiresDurableMailboxAccessNotOnlyALoggedInBrowser() {
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(true);
		utility.when(() -> Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT)).thenReturn("https://semoss.test/Monolith");
		AccessToken microsoft = new AccessToken(); microsoft.setId("ms-owner"); microsoft.setProvider(AuthProvider.MICROSOFT);
		microsoft.setAccess_token("session-token"); user.setAccessToken(microsoft);
		CollaborationDbUtils.update("INSERT INTO COLLAB_OWNER (OWNER_ID, OWNER_TYPE, MS_USER_ID) VALUES (?, ?, ?)",
				"owner-one", AuthProvider.NATIVE.getLabel(), "ms-owner");
		assertThrows(IllegalArgumentException.class, () -> WorkInboxRoutine.save(user, config(true)));
		assertTrue(((String) WorkInboxRoutine.get(user).get("availabilityMessage")).contains("renewable Microsoft sign-in"));
		assertEquals(0, CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_INBOX_ROUTINE"));
	}

	@Test
	void matchingBrowserConnectionIsSavedOnlyWhenOptingInWithoutAnyWebhook() throws Exception {
		backgroundAvailable();
		subscriptions.when(() -> MicrosoftGraphSubscriptionRegistry.forUser("ms-owner")).thenReturn(List.of());
		AccessToken token = microsoftToken("ms-owner");
		user.setAccessToken(token);
		assertEquals(true, WorkInboxRoutine.get(user).get("mailboxAvailable"));
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
		WorkInboxRoutine.save(user, config(false));
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(true);
		credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), never());
		Scheduler scheduler = mock(Scheduler.class);
		SchedulerFactorySingleton factory = mock(SchedulerFactorySingleton.class);
		when(factory.getScheduler()).thenReturn(scheduler);
		try (MockedStatic<SchedulerFactorySingleton> factories = mockStatic(SchedulerFactorySingleton.class);
				MockedStatic<SchedulerDatabaseUtility> database = mockStatic(SchedulerDatabaseUtility.class)) {
			factories.when(SchedulerFactorySingleton::getInstance).thenReturn(factory);
			database.when(() -> SchedulerDatabaseUtility.insertIntoJobRecipesTable(anyString(), anyString(), anyString(), anyString(),
					anyString(), any(), anyString(), anyString(), anyString(), anyBoolean(), isNull(), anyList())).thenReturn(true);
			doAnswer(call -> {
				assertTrue(savedTokens.containsKey("ms-owner"));
				return null;
			}).when(scheduler).addJob(any(JobDetail.class), eq(true));
			Map<String, Object> result = WorkInboxRoutine.save(user, config(true));
			assertEquals(true, result.get("enabled"));
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(user.getAccessToken(AuthProvider.MICROSOFT)));
			assertFalse(CollaborationDbUtils.toJson(result).contains("secret"));
			user.dropAccessToken(AuthProvider.MICROSOFT);
			assertEquals(true, WorkInboxRoutine.get(user).get("mailboxAvailable"));
			assertEquals(false, WorkInboxRoutine.get(owner("someone-else")).get("mailboxAvailable"));
		}
	}

	@Test
	void anotherMicrosoftSessionCannotAuthorizeTheLinkedMailbox() {
		backgroundAvailable();
		subscriptions.when(() -> MicrosoftGraphSubscriptionRegistry.forUser("ms-owner")).thenReturn(List.of());
		user.setAccessToken(microsoftToken("another-ms-account"));
		assertEquals(false, WorkInboxRoutine.get(user).get("mailboxAvailable"));
		assertThrows(IllegalArgumentException.class, () -> WorkInboxRoutine.save(user, config(true)));
		assertTrue(savedTokens.isEmpty());
		assertEquals(0, CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_INBOX_ROUTINE"));
	}

	@Test
	void settingsMailAndCalendarCredentialsWorkWithoutBrainFolderSubscriptions() {
		backgroundAvailable();
		for (String resource : List.of("me/messages", "me/events")) {
			Subscription subscription = mock(Subscription.class);
			when(subscription.getResource()).thenReturn(resource);
			User credentialUser = owner("unused");
			credentialUser.setAccessToken(microsoftToken("ms-owner"));
			when(subscription.getUser()).thenReturn(credentialUser);
			subscriptions.when(() -> MicrosoftGraphSubscriptionRegistry.forUser("ms-owner")).thenReturn(List.of(subscription));
			assertEquals(true, WorkInboxRoutine.get(user).get("mailboxAvailable"), resource);
		}
		assertTrue(savedTokens.isEmpty());
	}

	@Test
	void credentialSaveFailureCannotEnableOrScheduleTheRoutine() throws Exception {
		backgroundAvailable();
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
		WorkInboxRoutine.save(user, config(false));
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(true);
		credentials.when(() -> MicrosoftGraphCredentialStore.save(any())).thenThrow(new IllegalStateException("Database unavailable"));
		Scheduler scheduler = mock(Scheduler.class);
		SchedulerFactorySingleton factory = mock(SchedulerFactorySingleton.class);
		when(factory.getScheduler()).thenReturn(scheduler);
		try (MockedStatic<SchedulerFactorySingleton> factories = mockStatic(SchedulerFactorySingleton.class);
				MockedStatic<SchedulerDatabaseUtility> database = mockStatic(SchedulerDatabaseUtility.class)) {
			factories.when(SchedulerFactorySingleton::getInstance).thenReturn(factory);
			assertThrows(IllegalStateException.class, () -> WorkInboxRoutine.save(user, config(true)));
			assertEquals(false, WorkInboxRoutine.get(user).get("enabled"));
			verify(scheduler, never()).addJob(any(), anyBoolean());
			database.verifyNoInteractions();
		}
	}

	@Test
	void repeatedSavesUseOneQuartzJobAndPausingRevokesConsentFirst() throws Exception {
		backgroundAvailable();
		Scheduler scheduler = mock(Scheduler.class);
		SchedulerFactorySingleton factory = mock(SchedulerFactorySingleton.class);
		when(factory.getScheduler()).thenReturn(scheduler);
		try (MockedStatic<SchedulerFactorySingleton> factories = mockStatic(SchedulerFactorySingleton.class);
				MockedStatic<SchedulerDatabaseUtility> database = mockStatic(SchedulerDatabaseUtility.class)) {
			factories.when(SchedulerFactorySingleton::getInstance).thenReturn(factory);
			database.when(() -> SchedulerDatabaseUtility.insertIntoJobRecipesTable(anyString(), anyString(), anyString(), anyString(),
					anyString(), any(), anyString(), anyString(), anyString(), anyBoolean(), isNull(), anyList())).thenReturn(true);
			WorkInboxRoutine.save(user, config(true));
			Map<String, Object> changed = config(true); changed.put("time", "09:45");
			WorkInboxRoutine.save(user, changed);
			ArgumentCaptor<JobDetail> jobs = ArgumentCaptor.forClass(JobDetail.class);
			verify(scheduler, times(2)).addJob(jobs.capture(), eq(true));
			assertEquals(jobs.getAllValues().get(0).getKey(), jobs.getAllValues().get(1).getKey());
			JobDetail job = jobs.getValue();
			assertEquals(RunPixelJobFromDB.class, job.getJobClass());
			assertEquals("WorkRunInboxRoutine();", job.getJobDataMap().getString(JobConfigKeys.PIXEL));
			assertFalse(job.getJobDataMap().toString().contains("secret-token"));
			ArgumentCaptor<Trigger> triggers = ArgumentCaptor.forClass(Trigger.class);
			verify(scheduler, times(2)).scheduleJob(triggers.capture());
			assertEquals("0 45 9 ? * MON,TUE,WED,THU,FRI", ((CronTrigger) triggers.getValue()).getCronExpression());
			assertEquals(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING, triggers.getValue().getMisfireInstruction());
			doAnswer(call -> {
				assertEquals(false, WorkInboxRoutine.get(user).get("enabled"));
				return null;
			}).when(scheduler).pauseJob(any(JobKey.class));
			WorkInboxRoutine.save(user, config(false));
			assertEquals(1, CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_INBOX_ROUTINE"));
		}
	}

	@Test
	void schedulerFailureLeavesExistingRoutinePausedAndPreferencesIntact() throws Exception {
		WorkInboxRoutine.save(user, config(false));
		backgroundAvailable();
		Scheduler scheduler = mock(Scheduler.class);
		SchedulerFactorySingleton factory = mock(SchedulerFactorySingleton.class);
		when(factory.getScheduler()).thenReturn(scheduler);
		try (MockedStatic<SchedulerFactorySingleton> factories = mockStatic(SchedulerFactorySingleton.class);
				MockedStatic<SchedulerDatabaseUtility> database = mockStatic(SchedulerDatabaseUtility.class)) {
			factories.when(SchedulerFactorySingleton::getInstance).thenReturn(factory);
			// A failed recipe insertion must not leave an opted-in routine behind.
			assertThrows(IllegalStateException.class, () -> WorkInboxRoutine.save(user, config(true)));
			assertEquals(false, WorkInboxRoutine.get(user).get("enabled"));
			verify(scheduler, atLeastOnce()).pauseJob(any(JobKey.class));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void completedAgentBriefIsStoredPrivatelyAndDuplicateDeliveryDoesNotScanAgain(boolean alreadySaved) throws Exception {
		backgroundAvailable();
		if (alreadySaved) {
			savedTokens.put("ms-owner", microsoftToken("ms-owner"));
			subscriptions.when(() -> MicrosoftGraphSubscriptionRegistry.forUser("ms-owner")).thenReturn(List.of());
		}
		Map<String, Object> everyDay = config(false);
		everyDay.put("days", List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"));
		// Persist a previously enabled schedule without touching a real Quartz scheduler.
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
		WorkInboxRoutine.save(user, everyDay);
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?", true);
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		AgentRunService service = mock(AgentRunService.class);
		when(service.run(any(AgentRunRequest.class))).thenAnswer(call -> new AgentRunHandle("run-1",
				((AgentRunRequest) call.getArgument(0)).getRoomId(), AgentRunStatus.SUBMITTED));
		when(service.waitForRun("run-1", insight, 600_000L)).thenReturn(Map.of("status", "COMPLETED", "finalText", "Your private daily brief."));
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class);
				MockedStatic<AgentRunService> services = mockStatic(AgentRunService.class);
				MockedConstruction<MicrosoftOutlookMailHelper> mail = mockConstruction(MicrosoftOutlookMailHelper.class,
						(helper, context) -> when(helper.listMessages(any(), isNull(), any())).thenReturn(List.of(Map.of("subject", "Project update"))))) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			services.when(AgentRunService::get).thenReturn(service);
			assertEquals("completed", WorkInboxRoutine.run(insight).get("status"));
			assertTrue(savedTokens.containsKey("ms-owner"));
			credentials.verify(() -> MicrosoftGraphCredentialStore.accessToken("ms-owner"));
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), times(alreadySaved ? 0 : 1));
			Map<?, ?> brief = (Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun");
			assertEquals("completed", brief.get("status"));
			assertEquals("Your private daily brief.", brief.get("summary"));
			assertNotNull(brief.get("roomId"));
			assertNotNull(brief.get("finishedAt"));
			assertNull(WorkInboxRoutine.get(owner("another-owner")).get("lastRun"));
			assertEquals("skipped", WorkInboxRoutine.run(insight).get("status"));
			assertEquals(1, mail.constructed().size());
			verify(mail.constructed().getFirst()).listMessages(eq("secret-token"), isNull(), any());
			ArgumentCaptor<AgentRunRequest> requests = ArgumentCaptor.forClass(AgentRunRequest.class);
			verify(service).run(requests.capture());
			assertEquals(InboxBriefHarness.NAME, requests.getValue().getHarnessType());
			assertTrue(requests.getValue().getInput().contains("Project update"));
		}
	}

	@Test
	void microsoftMailDenialFailsTheBriefBeforeStartingAnAgent() {
		backgroundAvailable();
		savedTokens.put("ms-owner", microsoftToken("ms-owner"));
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
		Map<String, Object> everyDay = config(false);
		everyDay.put("days", List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"));
		WorkInboxRoutine.save(user, everyDay);
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?", true);
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class);
				MockedStatic<AgentRunService> services = mockStatic(AgentRunService.class);
				MockedConstruction<MicrosoftOutlookMailHelper> mail = mockConstruction(MicrosoftOutlookMailHelper.class,
						(helper, context) -> when(helper.listMessages(any(), isNull(), any()))
								.thenThrow(new IllegalArgumentException("Microsoft denied mail access")))) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			assertThrows(IllegalStateException.class, () -> WorkInboxRoutine.run(insight));
			assertEquals("failed", ((Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun")).get("status"));
			services.verifyNoInteractions();
		}
	}

	@Test
	void disabledOrAlreadyCompletedDayCannotReadMail() {
		WorkInboxRoutine.save(user, config(false));
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		assertEquals("skipped", WorkInboxRoutine.run(insight).get("status"));
		String today = java.time.LocalDate.now(java.time.ZoneId.of("America/New_York")).toString();
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?, LAST_DATE = ?", true, today);
		assertEquals("skipped", WorkInboxRoutine.run(insight).get("status"));
		subscriptions.verifyNoInteractions();
	}

	@Test
	void interruptedRunWithDurableIdBecomesRetryableInsteadOfStayingRunningForever() {
		WorkInboxRoutine.save(user, config(false));
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_STATUS = ?, RUN_ID = ?, STARTED_AT = ?",
				"running", "old-run", CollaborationDbUtils.toTimestamp("2020-01-01T00:00:00Z", "startedAt"));
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		AgentRunService service = mock(AgentRunService.class);
		when(service.getRun("old-run", insight)).thenReturn(Map.of("status", "RUNNING"));
		try (MockedStatic<AgentRunService> services = mockStatic(AgentRunService.class)) {
			services.when(AgentRunService::get).thenReturn(service);
			Map<?, ?> last = (Map<?, ?>) WorkInboxRoutine.get(insight).get("lastRun");
			assertEquals("failed", last.get("status"));
			assertNotNull(last.get("finishedAt"));
			verify(service).cancelRun(eq("old-run"), anyString());
		}
	}

	@Test
	void pixelSerializerOmitsNullableFieldsSoFrontendMustAcceptMissingValues() {
		String json = prerna.util.gson.GsonUtility.getDefaultGson().toJson(WorkInboxRoutine.get(user));
		assertFalse(json.contains("lastRun"));
		assertFalse(json.contains("nextRunAt"));
		assertTrue(json.contains("availabilityMessage"));
	}

	@Test
	void promptBoundsEmailCountAndBodySizeAndTreatsEmailAsUntrusted() {
		List<Map<String, Object>> mail = List.of(Map.of("subject", "Kept subject", "body", Map.of("content", "x".repeat(8000))),
				Map.of("subject", "Not scanned"));
		String prompt = WorkInboxRoutine.prompt(mail, 1, "2026-10-12");
		assertTrue(prompt.contains("Kept subject"));
		assertFalse(prompt.contains("Not scanned"));
		assertTrue(prompt.contains("untrusted source material"));
		assertTrue(prompt.contains("may be truncated"));
		assertTrue(prompt.length() < 2500);
	}

	private void backgroundAvailable() {
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(true);
		utility.when(() -> Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT)).thenReturn("https://semoss.test/Monolith");
		models.when(() -> BrainTopicModel.engine(any(User.class))).thenReturn("text-model");
		CollaborationDbUtils.update("INSERT INTO COLLAB_OWNER (OWNER_ID, OWNER_TYPE, MS_USER_ID) VALUES (?, ?, ?)",
				"owner-one", AuthProvider.NATIVE.getLabel(), "ms-owner");
		User microsoftUser = owner("unused");
		microsoftUser.setAccessToken(microsoftToken("ms-owner"));
		Subscription subscription = mock(Subscription.class);
		when(subscription.getResource()).thenReturn("me/messages");
		when(subscription.getUser()).thenReturn(microsoftUser);
		subscriptions.when(() -> MicrosoftGraphSubscriptionRegistry.forUser("ms-owner")).thenReturn(List.of(subscription));
	}

	private static AccessToken microsoftToken(String id) {
		AccessToken token = new AccessToken(); token.setId(id); token.setProvider(AuthProvider.MICROSOFT);
		token.setAccess_token("secret-token"); token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, "refresh-secret");
		return token;
	}

	private static User owner(String id) {
		AccessToken token = new AccessToken(); token.setId(id); token.setProvider(AuthProvider.NATIVE);
		User owner = new User(); owner.setAccessToken(token); return owner;
	}

	private static Map<String, Object> config(boolean enabled) {
		Map<String, Object> config = new HashMap<>(WorkInboxRoutine.defaults());
		config.put("timezone", "America/New_York"); config.put("enabled", enabled); return config;
	}
}
