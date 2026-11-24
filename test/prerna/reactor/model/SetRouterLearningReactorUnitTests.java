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
package prerna.reactor.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerBuilder;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.om.Insight;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;

/** The one switch writes the improvement section and binds one job and is idempotent; learning needs subtask mode. */
class SetRouterLearningReactorUnitTests {

	@TempDir
	Path folder;

	private final IModelRouterEngine engine = mock(IModelRouterEngine.class);
	private final Map<String, Map<String, String>> jobs = new LinkedHashMap<>();
	private final List<String> calls = new ArrayList<>();
	private final List<String[]> saves = new ArrayList<>();
	private int nextJobId;
	private MockedStatic<UpdateModelRouterConfigReactor> updater;
	private MockedStatic<SecurityAdminUtils> admin;

	@BeforeEach
	void setUp() throws Exception {
		updater = ApplyRouterCandidateReactorUnitTests.stubUpdater(Map.of("rt", engine), folder, saves);
		admin = mockStatic(SecurityAdminUtils.class);
		admin.when(() -> SecurityAdminUtils.userIsAdmin(any())).thenReturn(true);
		when(engine.readConfigJson()).thenReturn(ApplyRouterCandidateReactorUnitTests.config("v1", null));
	}

	@AfterEach
	void tearDown() {
		updater.close();
		admin.close();
	}

	/** The switch with the scheduler faked. */
	private Map<?, ?> set(boolean enabled, String... extra) {
		List<String> kv = new ArrayList<>(List.of("engine", "rt", "enabled", String.valueOf(enabled), "optimizerModelId", "judge-model"));
		kv.addAll(List.of(extra));
		return (Map<?, ?>) ApplyRouterCandidateReactorUnitTests.with(new SetRouterLearningReactor() {
			@Override
			BoundJob boundJob(String engineId, String jobId) {
				return jobs.values().stream().filter(job -> job.get("jobId").equals(jobId))
						.map(job -> new BoundJob(recipe(engineId).equals(job.get("recipe")) ? job.get("cron") : null))
						.findFirst().orElse(null);
			}

			@Override
			String createJob(String name, String cron, String recipe) {
				String id = "job-" + ++nextJobId;
				calls.add("create " + name + " " + cron);
				jobs.put(name, new HashMap<>(Map.of("jobId", id, "recipe", recipe, "cron", cron)));
				return id;
			}

			@Override
			void removeJob(String jobId) {
				calls.add("remove " + jobId);
				jobs.values().removeIf(job -> jobId.equals(job.get("jobId")));
			}
		}, mock(Insight.class), kv.toArray(new String[0])).execute().getValue();
	}

	private JsonObject improvement() throws Exception {
		return JsonParser.parseString(engine.readConfigJson()).getAsJsonObject().getAsJsonObject("improvement");
	}

	@Test
	void turnsLearningOnSchedulesTheJobsAndRepeatingChangesNothing() throws Exception {
		Map<?, ?> result = set(true, "maxQualityLoss", "0.05", "minNewTasks", "250");

		assertEquals(List.of("router-learning-optimize-rt"), result.get("scheduled"));
		assertTrue(improvement().get("enabled").getAsBoolean());
		assertEquals("judge-model", improvement().get("optimizer_model").getAsString());
		assertEquals(0.05, improvement().get("max_quality_loss").getAsDouble());
		assertEquals(250, improvement().get("min_new_tasks").getAsInt());
		assertEquals("job-1", improvement().get("scheduler_job_id").getAsString());
		assertEquals("RunRouterLearningIfDue(engine=[\"rt\"]);", jobs.get("router-learning-optimize-rt").get("recipe"));
		assertFalse(jobs.containsKey("router-learning-watch-rt"));

		int made = calls.size();
		set(true, "maxQualityLoss", "0.05", "minNewTasks", "250");
		assertEquals(1, saves.size());
		assertEquals(made, calls.size());
	}

	@Test
	void prototypeWatchIsLeftAloneAndLearningSettingsAreValidated() throws Exception {
		jobs.put("router-learning-watch-rt", new HashMap<>(Map.of("jobId", "w", "recipe", "CheckRouterRegression(engine=[\"rt\"]);", "cron", "0 0 * * * ?")));

		set(true);
		assertEquals(List.of("create router-learning-optimize-rt 0 0 3 * * ?"), calls);
		assertTrue(jobs.containsKey("router-learning-watch-rt"));
		assertFalse(improvement().has("min_new_tasks"));
		assertFalse(improvement().has("max_quality_loss"));

		assertThrows(IllegalArgumentException.class, () -> set(true, "minNewTasks", "-1"));
		assertThrows(IllegalArgumentException.class, () -> set(true, "minNewTasks", "2.5"));
		assertThrows(IllegalArgumentException.class, () -> set(true, "maxQualityLoss", "abc"));
		assertThrows(IllegalArgumentException.class, () -> set(true, "maxQualityLoss", "2"));
		assertThrows(IllegalArgumentException.class, () -> ModelRouterEngine.parseAndValidateConfig(
				ApplyRouterCandidateReactorUnitTests.config("v1", "\"improvement\":{\"min_new_tasks\":-1}"), "t", "rt"));
		assertThrows(IllegalArgumentException.class, () -> ModelRouterEngine.parseAndValidateConfig(
				ApplyRouterCandidateReactorUnitTests.config("v1", "\"improvement\":{\"max_quality_loss\":1.5}"), "t", "rt"));
		set(true, "minNewTasks", "0");
		assertEquals(0, improvement().get("min_new_tasks").getAsInt());
	}

	@Test
	void aConfigWithTheRemovedGuardrailWatchAndContextStepKeysStillLoads() {
		assertEquals(0.0, ModelRouterEngine.parseAndValidateConfig(ApplyRouterCandidateReactorUnitTests.config("v1",
				"\"subtask_context_steps\":3,\"improvement\":{\"enabled\":true,\"guardrails\":{\"min_tasks\":20,\"max_quality_drop\":0.02},"
						+ "\"watch\":{\"min_samples\":30}}"),
				"t", "rt").improvement().max_quality_loss);
	}

	@Test
	void theLearningJobIsDailyByDefaultFollowsFrequencyKeepsAnEditedCronAndGoesWithTheSwitch() throws Exception {
		set(true);
		assertEquals("0 0 3 * * ?", jobs.get("router-learning-optimize-rt").get("cron"));

		calls.clear();
		set(true);
		assertTrue(calls.isEmpty());

		set(true, "frequency", "weekly");
		assertEquals("job-2", improvement().get("scheduler_job_id").getAsString());
		assertEquals("0 0 3 ? * SUN", jobs.get("router-learning-optimize-rt").get("cron"));
		set(true, "frequency", "hourly");
		assertEquals("0 0 * * * ?", jobs.get("router-learning-optimize-rt").get("cron"));

		jobs.get("router-learning-optimize-rt").put("cron", "0 30 6 * * ?");
		calls.clear();
		set(true);
		assertTrue(calls.isEmpty(), "an edited cron stays when no frequency is given");

		set(true, "frequency", "off");
		assertEquals("0 0 0 1 1 ? 2099", jobs.get("router-learning-optimize-rt").get("cron"), "off keeps the job for the volume trigger on a cron that never fires");
		assertThrows(IllegalArgumentException.class, () -> set(true, "frequency", "monthly"));

		set(false);
		assertTrue(jobs.isEmpty());
	}

	@Test
	void resumesAPausedRouterAndTurningOffRemovesOnlyTheBoundJob() throws Exception {
		when(engine.readConfigJson()).thenReturn(ApplyRouterCandidateReactorUnitTests.config("v1", "\"improvement\":{\"enabled\":true,\"paused\":true}"));
		set(true);
		assertFalse(improvement().get("paused").getAsBoolean());

		assertEquals(List.of(), set(false).get("scheduled"));
		assertTrue(jobs.isEmpty());
		assertFalse(improvement().get("enabled").getAsBoolean());
		assertFalse(improvement().has("scheduler_job_id"));
		int made = calls.size();
		set(false);
		assertEquals(made, calls.size());
	}

	@Test
	void aSchedulerIdIsVerifiedAgainstTheActualRouterRecipe() throws Exception {
		Scheduler scheduler = mock(Scheduler.class);
		JobKey key = JobKey.jobKey("job-1", SetRouterLearningReactor.JOB_GROUP);
		when(scheduler.getJobDetail(key)).thenReturn(JobBuilder.newJob(RunPixelJobFromDB.class).withIdentity(key)
				.usingJobData(JobConfigKeys.PIXEL, SetRouterLearningReactor.recipe("rt")).build());
		doReturn(List.of(TriggerBuilder.newTrigger().forJob(key).build(),
				TriggerBuilder.newTrigger().forJob(key).withSchedule(CronScheduleBuilder.cronSchedule("0 0 3 * * ?")).build()))
				.when(scheduler).getTriggersOfJob(key);
		assertEquals("0 0 3 * * ?", SetRouterLearningReactor.lookupJob(scheduler, "rt", "job-1").cron());
		assertFalse(SetRouterLearningReactor.lookupJob(scheduler, "other", "job-1").verified());
	}

	@Test
	void aMissingJobIsRecreatedButAnIdForAnotherRouterIsNotUsed() throws Exception {
		when(engine.readConfigJson()).thenReturn(ApplyRouterCandidateReactorUnitTests.config("v1",
				"\"improvement\":{\"enabled\":true,\"scheduler_job_id\":\"old\"}"));
		jobs.put("other", Map.of("jobId", "old", "recipe", SetRouterLearningReactor.recipe("other"), "cron", "0 0 3 * * ?"));
		assertThrows(IllegalStateException.class, () -> set(true));
		assertTrue(calls.isEmpty());
		jobs.clear();
		set(true);
		assertEquals("job-1", improvement().get("scheduler_job_id").getAsString());
	}

	@Test
	void rejectsRoutersThatAreNotInSubtaskMode() throws Exception {
		String keyword = "{\"mode\":\"keyword\",\"routes\":[{\"engine_id\":\"m1\"}]}";
		when(engine.readConfigJson()).thenReturn(keyword);
		assertThrows(IllegalArgumentException.class, () -> set(true));
		assertThrows(IllegalArgumentException.class, () -> set(false));
		assertEquals(keyword, engine.readConfigJson());
		// learning needs subtask mode in the config itself
		assertThrows(IllegalArgumentException.class, () -> ModelRouterEngine.parseAndValidateConfig(
				"{\"routes\":[{\"engine_id\":\"m1\"}],\"improvement\":{\"enabled\":true}}", "t", "rt"));
		assertTrue(saves.isEmpty());
	}

	@Test
	void onlyAnAdminOutsideAnAgentRunCanSwitch() {
		admin.when(() -> SecurityAdminUtils.userIsAdmin(any())).thenReturn(false);
		assertThrows(IllegalArgumentException.class, () -> set(true));
		Insight agentRun = mock(Insight.class);
		when(agentRun.getRoomId()).thenReturn("room-1");
		assertThrows(SecurityException.class, () -> ApplyRouterCandidateReactorUnitTests.with(new SetRouterLearningReactor(), agentRun).execute());
	}
}
