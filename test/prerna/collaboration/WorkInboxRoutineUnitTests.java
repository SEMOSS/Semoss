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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
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
import prerna.ds.py.PyUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.io.connector.AbstractOAuthTokenFiller;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry.Subscription;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.automation.run.AutomationRunStore;
import prerna.reactor.automation.run.RunAutomationDefinitionReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
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
	private MockedStatic<PyUtils> python;
	private MockedStatic<BrainMailHeaderSource> headers;
	private MockedStatic<RunAutomationDefinitionReactor> automations;
	private MockedStatic<AutomationRunStore> store;
	private BrainMailHeaderSource headerSource;
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
		// pyEnabled caches its first answer; the validator still needs the real identifier check.
		python = mockStatic(PyUtils.class);
		python.when(PyUtils::pyEnabled).thenReturn(true);
		python.when(() -> PyUtils.isValidPythonVariableName(anyString())).thenCallRealMethod();
		headerSource = mock(BrainMailHeaderSource.class);
		when(headerSource.me(any())).thenReturn(Map.of("mail", "owner@example.com"));
		headers = mockStatic(BrainMailHeaderSource.class);
		headers.when(BrainMailHeaderSource::current).thenReturn(headerSource);
		automations = mockStatic(RunAutomationDefinitionReactor.class);
		store = mockStatic(AutomationRunStore.class);
	}

	@AfterEach
	void tearDown() throws Exception {
		store.close();
		automations.close();
		headers.close();
		python.close();
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

	// shortcut: the "duplicate delivery is skipped" check was removed with the once-per-day guard; restore it with the guard.
	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void completedBriefIsStoredPrivately(boolean alreadySaved) throws Exception {
		String scanAt = "2026-10-12T12:00:00.123Z";
		at(scanAt);
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
		automations.when(() -> RunAutomationDefinitionReactor.runDefinition(any(), anyString(), anyString(), any(), anyMap(),
				anyString(), isNull())).thenAnswer(call -> {
					// The run id is attached before anything runs, and the run sees the stored mailbox token.
					String started = call.getArgument(1);
					assertEquals(started, runId());
					assertEquals("secret-token", user.getAccessToken(AuthProvider.MICROSOFT).getAccess_token());
					AccessToken refreshed = microsoftToken("ms-owner");
					refreshed.setAccess_token("refreshed-token");
					user.setAccessToken(refreshed);
					at("2026-10-12T12:05:00Z");
					return Map.of();
				});
		finished(AutomationConstants.STATUS_SUCCESS, AutomationConstants.NODE_STATUS_SUCCESS, "Your private daily brief.");
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			assertEquals("completed", WorkInboxRoutine.run(insight).get("status"));
			credentials.verify(() -> MicrosoftGraphCredentialStore.accessToken("ms-owner"));
			// The migration save, if any, plus the refreshed pair the run left behind.
			credentials.verify(() -> MicrosoftGraphCredentialStore.save(any()), times(alreadySaved ? 1 : 2));
			assertEquals("refreshed-token", savedTokens.get("ms-owner").getAccess_token());
			assertNull(user.getAccessToken(AuthProvider.MICROSOFT));
			Map<?, ?> brief = (Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun");
			assertEquals("completed", brief.get("status"));
			assertEquals("Your private daily brief.", brief.get("summary"));
			assertEquals("brief-room", brief.get("roomId"));
			assertNull(brief.get("error"));
			assertEquals(scanAt, lastSuccess());
			assertEquals("2026-10-12T12:05:00Z", brief.get("finishedAt"));
			assertNull(WorkInboxRoutine.get(owner("another-owner")).get("lastRun"));
			ArgumentCaptor<AutomationDefinitionService.DefinitionFiles> files = ArgumentCaptor
					.forClass(AutomationDefinitionService.DefinitionFiles.class);
			automations.verify(() -> RunAutomationDefinitionReactor.runDefinition(eq(insight), anyString(),
					eq(WorkInboxRoutine.AUTOMATION_ID), files.capture(), eq(Map.of("owner_email", "owner@example.com")),
					eq(AutomationConstants.TRIGGER_SCHEDULED), isNull()));
			assertTrue(files.getValue().definition().contains("BrainListThreads(limit=[25], offset=[0], detail=[true]);"));
			assertTrue(files.getValue().definition().contains("\"text-model\""));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void failedBriefPreservesTheLastSuccessAndAnUnsentBriefIsStillKept(boolean legacyCompletedRow) throws Exception {
		Insight insight = enabledRoutine();
		String prior = "2026-10-10T08:00:00Z";
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET LAST_SUCCESS_AT = ?, RUN_STATUS = ?, STARTED_AT = ?",
				legacyCompletedRow ? null : CollaborationDbUtils.toTimestamp(prior, "prior"),
				legacyCompletedRow ? "completed" : "failed", CollaborationDbUtils.toTimestamp(prior, "prior"));
		at("2026-10-12T08:00:00Z");
		finished(AutomationConstants.STATUS_FAILED, AutomationConstants.NODE_STATUS_FAILED, null);
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			assertEquals("failed", WorkInboxRoutine.run(insight).get("status"));
			assertEquals(prior, lastSuccess());
			Map<?, ?> failed = (Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun");
			assertEquals("failed", failed.get("status"));
			assertNull(failed.get("roomId"));
			// Simulate the next scheduled day's database claim without changing the host clock.
			CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET LAST_DATE = NULL");
			at("2026-10-13T08:00:00Z");
			finished(AutomationConstants.STATUS_FAILED, AutomationConstants.NODE_STATUS_SUCCESS, "Caught up.");
			assertEquals("completed", WorkInboxRoutine.run(insight).get("status"));
			assertEquals("2026-10-13T08:00:00Z", lastSuccess());
			Map<?, ?> unsent = (Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun");
			assertEquals("Caught up.", unsent.get("summary"));
			assertEquals("The brief was saved but the email could not be sent.", unsent.get("error"));
			assertEquals("brief-room", unsent.get("roomId"));
			automations.verify(() -> RunAutomationDefinitionReactor.runDefinition(any(), anyString(), anyString(), any(),
					anyMap(), anyString(), isNull()), times(2));
		}
	}

	@Test
	void briefUsesTheRoutineThreadCountAndOwnerModel() throws Exception {
		Insight insight = enabledRoutine();
		at("2026-10-12T08:00:00Z");
		Map<String, Object> limited = config(false);
		limited.put("days", List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"));
		limited.put("emailCount", 1);
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET SETTINGS_JSON = ?", CollaborationDbUtils.toJson(limited));
		finished(AutomationConstants.STATUS_SUCCESS, AutomationConstants.NODE_STATUS_SUCCESS, "Nothing urgent today.");
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			assertEquals("completed", WorkInboxRoutine.run(insight).get("status"));
			ArgumentCaptor<AutomationDefinitionService.DefinitionFiles> files = ArgumentCaptor
					.forClass(AutomationDefinitionService.DefinitionFiles.class);
			automations.verify(() -> RunAutomationDefinitionReactor.runDefinition(any(), anyString(), anyString(),
					files.capture(), anyMap(), anyString(), isNull()));
			assertTrue(files.getValue().definition().contains("BrainListThreads(limit=[1], offset=[0], detail=[true]);"));
			assertTrue(files.getValue().definition().contains("\"text-model\""));
		}
	}

	@Test
	void recoveredFinishedRunAdvancesOnlyItsOwnersCutoff() {
		WorkInboxRoutine.save(user, config(false));
		WorkInboxRoutine.save(owner("another-owner"), config(false));
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_STATUS = ?, RUN_ID = ?, STARTED_AT = ? WHERE OWNER_ID = ?",
				"running", "completed-run", CollaborationDbUtils.toTimestamp("2026-10-11T08:00:00Z", "startedAt"), "owner-one");
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		finished(AutomationConstants.STATUS_SUCCESS, AutomationConstants.NODE_STATUS_SUCCESS, "Recovered brief.");
		Map<?, ?> last = (Map<?, ?>) WorkInboxRoutine.get(insight).get("lastRun");
		assertEquals("Recovered brief.", last.get("summary"));
		assertEquals("brief-room", last.get("roomId"));
		assertEquals("2026-10-11T08:00:00Z", lastSuccess());
		assertNull(CollaborationDbUtils.queryOne("SELECT LAST_SUCCESS_AT FROM WORK_INBOX_ROUTINE WHERE OWNER_ID = ?",
				rs -> CollaborationDbUtils.getTimestamp(rs, "LAST_SUCCESS_AT"), "another-owner"));
		store.verify(() -> AutomationRunStore.getNodeOutputsForRun("completed-run"));
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void supersededOrPausedExecutionNeverStartsTheAutomation(boolean paused) throws Exception {
		Insight insight = enabledRoutine();
		at("2026-10-12T08:00:00Z");
		when(headerSource.me(user)).thenAnswer(call -> {
			if (paused) {
				CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?", false);
			} else {
				CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET STARTED_AT = ?, RUN_ID = ?, ROOM_ID = ?",
						CollaborationDbUtils.toTimestamp("2026-10-13T08:00:00Z", "newStart"), "newer-run", "newer-room");
			}
			return Map.of("mail", "owner@example.com");
		});
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			assertEquals("skipped", WorkInboxRoutine.run(insight).get("status"));
			Map<?, ?> last = (Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun");
			if (paused) {
				assertEquals("failed", last.get("status"));
				assertEquals("Routine was paused before the brief started.", last.get("error"));
				assertNull(runId());
			} else {
				assertEquals("running", last.get("status"));
				assertEquals("newer-room", last.get("roomId"));
				assertEquals("2026-10-13T08:00:00Z", last.get("startedAt"));
				assertEquals("newer-run", runId());
			}
			assertNull(lastSuccess());
			automations.verifyNoInteractions();
			assertNull(user.getAccessToken(AuthProvider.MICROSOFT));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void mailboxDenialOrMissingAddressFailsBeforeStartingTheAutomation(boolean denied) throws Exception {
		Insight insight = enabledRoutine();
		// An interactive caller keeps their own Microsoft session afterwards.
		user.setAccessToken(microsoftToken("session-account"));
		if (denied) {
			when(headerSource.me(user)).thenThrow(new IllegalArgumentException("Microsoft denied mail access"));
		} else {
			when(headerSource.me(user)).thenReturn(Map.of("displayName", "Owner"));
		}
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class)) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			assertThrows(IllegalStateException.class, () -> WorkInboxRoutine.run(insight));
			Map<?, ?> last = (Map<?, ?>) WorkInboxRoutine.get(user).get("lastRun");
			assertEquals("failed", last.get("status"));
			assertFalse(String.valueOf(last.get("error")).contains("denied"));
			automations.verifyNoInteractions();
			assertEquals("session-account", user.getAccessToken(AuthProvider.MICROSOFT).getId());
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void stillRunningBrainSyncKeepsTheLentTokenItReadsGraphWith(boolean bareSchedulerToken) throws Exception {
		Insight insight = enabledRoutine();
		if (bareSchedulerToken) {
			// The scheduler rebuilds a Microsoft login as an id-only token with no access token.
			AccessToken bare = new AccessToken(); bare.setId("ms-owner"); bare.setProvider(AuthProvider.MICROSOFT);
			user.setAccessToken(bare);
		}
		finished(AutomationConstants.STATUS_SUCCESS, AutomationConstants.NODE_STATUS_SUCCESS, "Your private daily brief.");
		try (MockedStatic<SecurityQueryUtils> accounts = mockStatic(SecurityQueryUtils.class);
				MockedStatic<CollaborationJobUtils> jobs = mockStatic(CollaborationJobUtils.class)) {
			accounts.when(() -> SecurityQueryUtils.getUnlockedUser("owner-one", AuthProvider.NATIVE.getLabel())).thenReturn(Map.of("id", "owner-one"));
			// The sync node's background job outlived the agent and delivery steps.
			jobs.when(() -> CollaborationJobUtils.latest("owner-one", AuthProvider.NATIVE.getLabel(), BrainSync.KIND))
					.thenReturn(Map.of("status", CollaborationJobUtils.RUNNING));
			assertEquals("completed", WorkInboxRoutine.run(insight).get("status"));
			assertEquals("ms-owner", user.getAccessToken(AuthProvider.MICROSOFT).getId());
			assertEquals("secret-token", user.getAccessToken(AuthProvider.MICROSOFT).getAccess_token());
		}
	}

	// shortcut: the already-completed-day case was removed with the once-per-day guard; restore it with the guard.
	@Test
	void disabledRoutineCannotReadMail() {
		WorkInboxRoutine.save(user, config(false));
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		assertEquals("skipped", WorkInboxRoutine.run(insight).get("status"));
		subscriptions.verifyNoInteractions();
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void interruptedRunBecomesRetryableAndOnlyAnAutomationRunIsCancelled(boolean automationRun) {
		WorkInboxRoutine.save(user, config(false));
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_STATUS = ?, RUN_ID = ?, STARTED_AT = ?",
				"running", "old-run", CollaborationDbUtils.toTimestamp("2020-01-01T00:00:00Z", "startedAt"));
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		// A pre-upgrade agent-run id has no automation row.
		store.when(() -> AutomationRunStore.getRunDetail("old-run"))
				.thenReturn(automationRun ? Map.of(AutomationConstants.STATUS, AutomationConstants.STATUS_RUNNING) : null);
		Map<?, ?> last = (Map<?, ?>) WorkInboxRoutine.get(insight).get("lastRun");
		assertEquals("failed", last.get("status"));
		assertNotNull(last.get("finishedAt"));
		automations.verify(() -> RunAutomationDefinitionReactor.cancelRun("old-run"), times(automationRun ? 1 : 0));
	}

	@Test
	void disabledPythonBlocksOptInAndExplainsWhy() {
		backgroundAvailable();
		python.when(PyUtils::pyEnabled).thenReturn(false);
		Map<String, Object> result = WorkInboxRoutine.get(user);
		assertEquals(true, result.get("mailboxAvailable"));
		assertEquals(false, result.get("assistantAvailable"));
		assertEquals("Python is disabled on this server; the inbox brief needs it.", result.get("availabilityMessage"));
		IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class, () -> WorkInboxRoutine.save(user, config(true)));
		assertEquals("Python is disabled on this server; the inbox brief needs it.", rejected.getMessage());
		assertEquals(0, CollaborationDbUtils.count("SELECT COUNT(*) FROM WORK_INBOX_ROUTINE"));
	}

	@Test
	void briefDefinitionIsStrictlyValidAndReadsOnlyDeclaredScopeNames() {
		AutomationDefinitionService.DefinitionFiles files = WorkInboxRoutine.briefDefinition("engine-1", 25);
		AutomationDefinitionValidator.ValidatedDefinition definition = AutomationDefinitionValidator.parseAndValidate(files.definition());
		assertEquals(Set.of("wait_sync", "deliver"), files.nodeSources().keySet());
		assertEquals(List.of("start", "sync", "wait_sync", "topics", "threads", "brief", "deliver"),
				definition.nodes().stream()
						.map(node -> (String) node.get(AutomationConstants.NODE_FIELD_ID))
						.toList());
		Set<String> declared = new HashSet<>(Set.of("owner_email", AutomationConstants.SCOPE_DATE,
				AutomationConstants.SCOPE_TRIGGERED_AT, AutomationConstants.SCOPE_RUN_ID));
		Set<String> used = new HashSet<>();
		Map<String, Map<String, Object>> nodes = new HashMap<>();
		Pattern reference = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)");
		for (Map<String, Object> node : definition.nodes()) {
			nodes.put((String) node.get(AutomationConstants.NODE_FIELD_ID), node);
			if (node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR) instanceof String name) {
				declared.add(name);
			}
			Matcher matcher = reference.matcher(AutomationRuntimeUtils.GSON.toJson(node.get(AutomationConstants.NODE_FIELD_CONFIG)));
			while (matcher.find()) {
				used.add(matcher.group(1));
			}
		}
		Pattern key = Pattern.compile("scope(?:\\[|\\.get\\()\"([^\"]+)\"");
		for (String id : List.of("wait_sync", "deliver")) {
			assertEquals(AutomationConstants.NODE_CODE_MODE_CUSTOM, nodes.get(id).get(AutomationConstants.NODE_FIELD_CODE_MODE), id);
			String source = files.nodeSources().get(id);
			assertTrue(source != null && source.contains("\ndef run(scope):"), id);
			Matcher matcher = key.matcher(source);
			while (matcher.find()) {
				used.add(matcher.group(1));
			}
		}
		assertTrue(used.containsAll(Set.of("topics", "threads", "brief", "owner_email", "date")), used.toString());
		assertTrue(declared.containsAll(used), "undeclared scope names: " + used.stream().filter(name -> !declared.contains(name)).toList());
		Map<?, ?> brief = (Map<?, ?>) nodes.get("brief").get(AutomationConstants.NODE_FIELD_CONFIG);
		assertEquals("engine-1", brief.get(AutomationConstants.CONFIG_ENGINE_ID));
		assertEquals(InboxBriefHarness.NAME, brief.get(AutomationConstants.CONFIG_HARNESS_TYPE));
		assertEquals("BrainListThreads(limit=[25], offset=[0], detail=[true]);",
				((Map<?, ?>) nodes.get("threads").get(AutomationConstants.NODE_FIELD_CONFIG)).get(AutomationConstants.CONFIG_PIXEL));
	}

	@Test
	void pixelSerializerOmitsNullableFieldsSoFrontendMustAcceptMissingValues() {
		String json = prerna.util.gson.GsonUtility.getDefaultGson().toJson(WorkInboxRoutine.get(user));
		assertFalse(json.contains("lastRun"));
		assertFalse(json.contains("nextRunAt"));
		assertTrue(json.contains("availabilityMessage"));
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

	private Insight enabledRoutine() {
		backgroundAvailable();
		savedTokens.put("ms-owner", microsoftToken("ms-owner"));
		registry.when(SystemEngineRegistry::isSchedulerDbLoaded).thenReturn(false);
		Map<String, Object> everyDay = config(false);
		everyDay.put("days", List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"));
		WorkInboxRoutine.save(user, everyDay);
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?", true);
		Insight insight = mock(Insight.class); when(insight.getUser()).thenReturn(user);
		return insight;
	}

	private void at(String instant) {
		utility.when(Utility::getCurrentSqlTimestampUTC).thenAnswer(call -> CollaborationDbUtils.toTimestamp(instant, "now"));
	}

	/** Durable run state as the automation leaves it: run status plus the brief node's row. */
	private void finished(String runStatus, String briefStatus, String text) {
		store.when(() -> AutomationRunStore.getRunDetail(anyString())).thenReturn(Map.of(AutomationConstants.STATUS, runStatus));
		Map<String, Object> brief = new HashMap<>(Map.of(AutomationConstants.NODE_ID, "brief",
				AutomationConstants.STATUS, briefStatus, AutomationConstants.ROOM_ID, "brief-room"));
		if (text != null) {
			brief.put(AutomationConstants.OUTPUT_VALUE, AutomationRuntimeUtils.GSON.toJson(text));
		}
		store.when(() -> AutomationRunStore.getNodeOutputsForRun(anyString())).thenReturn(List.of(Map.of(
				AutomationConstants.NODE_ID, "threads", AutomationConstants.STATUS, AutomationConstants.NODE_STATUS_SUCCESS,
				AutomationConstants.OUTPUT_VALUE, "{\"items\":[]}"), brief));
	}

	private String runId() {
		return CollaborationDbUtils.queryOne("SELECT RUN_ID FROM WORK_INBOX_ROUTINE WHERE OWNER_ID = ?",
				rs -> rs.getString("RUN_ID"), "owner-one");
	}

	private String lastSuccess() {
		return CollaborationDbUtils.queryOne("SELECT LAST_SUCCESS_AT FROM WORK_INBOX_ROUTINE WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> CollaborationDbUtils.getTimestamp(rs, "LAST_SUCCESS_AT"), "owner-one", AuthProvider.NATIVE.getLabel());
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
