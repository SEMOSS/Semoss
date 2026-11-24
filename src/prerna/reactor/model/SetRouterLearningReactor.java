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

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine;
import prerna.engine.impl.model.ModelRouterEngine.Improvement;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * The one switch of a self-improving router (admins only): writes the {@code improvement} section through
 * {@link UpdateModelRouterConfigReactor#save} and creates or removes the scheduled learning job
 * ({@link RunRouterLearningIfDueReactor}; it starts the {@code router-optimizer} agent only when there is something new to learn from)
 * The learning job runs on the cron of {@code frequency} or, with frequency off, on a
 * cron that never fires; the router also triggers it when enough new tasks came in ({@link RouterLearningTrigger}). Jobs go through the
 * scheduler's own pixels, so its permission checks apply; a job with the right recipe is left alone, so the same call twice changes
 * nothing and a cron an admin edited stays. An agent run is refused: it must not set the limit its own candidates are judged by.
 */
public class SetRouterLearningReactor extends AbstractReactor {

	static final String JOB_GROUP = "router-learning";
	private static final String JOB_PREFIX = "router-learning-optimize-";
	/** A cron that never fires (Quartz needs a valid one): frequency off, learning starts only by volume. */
	static final String NEVER_CRON = "0 0 0 1 1 ? 2099";
	static final Map<String, String> FREQUENCY_CRONS = Map.of("hourly", "0 0 * * * ?", "daily", "0 0 3 * * ?", "weekly", "0 0 3 ? * SUN");
	static final String DEFAULT_FREQUENCY = "daily";
	private static final String REPLAY = "replay";
	private static final String FREQUENCY = "frequency";
	private static final String ENABLED = "enabled";
	private static final String OPTIMIZER_MODEL = "optimizerModelId";
	private static final String MAX_QUALITY_LOSS = "maxQualityLoss";
	private static final String MIN_NEW_TASKS = "minNewTasks";
	private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9_-]+");

	public SetRouterLearningReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ENABLED, OPTIMIZER_MODEL, MAX_QUALITY_LOSS, MIN_NEW_TASKS, FREQUENCY, REPLAY };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		if (this.insight.getRoomId() != null) {
			throw new SecurityException("An agent run cannot change router learning");
		}
		organizeKeys();
		String engineId = ValueUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		if (engineId == null || !SAFE_ID.matcher(engineId).matches()) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have edit access to it");
		}
		IModelRouterEngine model = UpdateModelRouterConfigReactor.editableRouter(this.insight, engineId);
		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throw new IllegalArgumentException("Only an admin can switch router learning");
		}
		boolean enabled = Boolean.parseBoolean(this.keyValue.get(ENABLED));
		String optimizerModel = ValueUtils.trimToNull(this.keyValue.get(OPTIMIZER_MODEL));
		if (enabled && (optimizerModel == null || !SAFE_ID.matcher(optimizerModel).matches())) {
			throw new IllegalArgumentException(OPTIMIZER_MODEL + " is required to turn learning on");
		}
		String frequency = ValueUtils.trimToNull(this.keyValue.get(FREQUENCY));
		if (frequency != null && !"off".equals(frequency) && !FREQUENCY_CRONS.containsKey(frequency)) {
			throw new IllegalArgumentException(FREQUENCY + " must be off, hourly, daily or weekly");
		}
		ReentrantLock lock = RouterVersions.lock(engineId);
		lock.lock();
		try {
			return configure(engineId, model, enabled, optimizerModel, frequency);
		} finally {
			lock.unlock();
		}
	}

	private NounMetadata configure(String engineId, IModelRouterEngine model, boolean enabled, String optimizerModel, String frequency) {
		JsonObject config = JsonParser.parseString(UpdateModelRouterConfigReactor.readConfig(model)).getAsJsonObject();
		JsonObject before = config.deepCopy();
		boolean subtaskMode = config.has("mode") && "subtask".equals(config.get("mode").getAsString());
		if (!subtaskMode && (enabled || !config.has("improvement"))) {
			throw new IllegalArgumentException("Engine " + engineId + " is not in subtask mode; learning applies to subtask routers only");
		}
		JsonObject improvement = config.has("improvement") ? config.getAsJsonObject("improvement") : new JsonObject();
		improvement.addProperty("enabled", enabled);
		if (enabled) {
			improvement.addProperty("paused", false);
			improvement.addProperty("optimizer_model", optimizerModel);
			putNumber(improvement, "max_quality_loss", this.keyValue.get(MAX_QUALITY_LOSS), 1);
			putNumber(improvement, "min_new_tasks", this.keyValue.get(MIN_NEW_TASKS), Integer.MAX_VALUE);
		}
		if (enabled || before.has("improvement")) {
			config.add("improvement", improvement);
		}
		Map<String, Object> replay = getGenRowStruct(REPLAY) == null ? null : getGenericMap(REPLAY, null);
		if (enabled && replay != null) {
			JsonObject settings = improvement.get(REPLAY) instanceof JsonObject existing ? existing : new JsonObject();
			new Gson().toJsonTree(replay).getAsJsonObject().entrySet().forEach(entry -> settings.add(entry.getKey(), entry.getValue()));
			improvement.add(REPLAY, settings);
			ModelRouterEngine.parseAndValidateConfig(config.toString(), "router", engineId);
		}
		var binding = improvement.get(Improvement.SCHEDULER_JOB_ID);
		String jobId = binding == null || binding.isJsonNull() ? null : binding.getAsString();
		String name = JOB_PREFIX + engineId;
		String requested = frequency == null ? null : "off".equals(frequency) ? NEVER_CRON : FREQUENCY_CRONS.get(frequency);
		if (!enabled) {
			saveIfChanged(engineId, model, config, before);
		}
		BoundJob job = boundJob(engineId, jobId);
		if (job != null && !job.verified()) {
			throw new IllegalStateException("Scheduler binding does not match this router; no scheduler job was changed");
		}
		boolean replace = enabled && job != null && requested != null && !requested.equals(job.cron());
		if (job != null && (!enabled || replace)) {
			removeJob(jobId);
		}
		if (enabled && (job == null || replace)) {
			jobId = createJob(name, requested == null ? FREQUENCY_CRONS.get(DEFAULT_FREQUENCY) : requested, recipe(engineId));
			improvement.addProperty(Improvement.SCHEDULER_JOB_ID, jobId);
			try {
				saveIfChanged(engineId, model, config, before);
			} catch (RuntimeException failure) {
				try {
					removeJob(jobId);
				} catch (RuntimeException cleanupFailure) {
					failure.addSuppressed(cleanupFailure);
				}
				throw failure;
			}
		} else if (enabled || improvement.remove(Improvement.SCHEDULER_JOB_ID) != null) {
			saveIfChanged(engineId, model, config, before);
		}
		return new NounMetadata(Map.of("enabled", enabled, "scheduled", enabled ? List.of(name) : List.of()),
				PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private void saveIfChanged(String engineId, IModelRouterEngine model, JsonObject config, JsonObject before) {
		if (!config.equals(before)) {
			UpdateModelRouterConfigReactor.save(this.insight, engineId, model, config.toString(), "SetRouterLearning: update learning settings");
		}
	}

	/** Sets a number given as text, from 0 to {@code max} (a whole number when {@code max} is above 1), in the improvement section; leaves it alone when no text is given. */
	private static void putNumber(JsonObject improvement, String key, String value, double max) {
		if (value != null) {
			try {
				double number = Double.parseDouble(value.trim());
				if (number >= 0 && number <= max && (max <= 1 || number == Math.rint(number))) {
					improvement.addProperty(key, max <= 1 ? (Number) number : (Number) (long) number);
					return;
				}
			} catch (NumberFormatException e) {
				// refused below
			}
			throw new IllegalArgumentException(key + " must be " + (max <= 1 ? "a number from 0 to 1" : "a whole number, 0 or more"));
		}
	}

	static String recipe(String engineId) {
		return "RunRouterLearningIfDue(engine=[\"" + engineId + "\"]);";
	}

	record BoundJob(String cron) {
		boolean verified() {
			return cron != null;
		}
	}

	BoundJob boundJob(String engineId, String jobId) {
		return findBoundJob(engineId, jobId);
	}

	static BoundJob findBoundJob(String engineId, String jobId) {
		if (jobId == null) {
			return null;
		}
		try {
			return lookupJob(SchedulerFactorySingleton.getInstance().getScheduler(), engineId, jobId);
		} catch (SchedulerException e) {
			throw new IllegalStateException("Could not read the router's scheduler job", e);
		}
	}

	static BoundJob lookupJob(Scheduler scheduler, String engineId, String jobId) throws SchedulerException {
		if (!SAFE_ID.matcher(jobId).matches()) {
			return new BoundJob(null);
		}
		JobKey key = JobKey.jobKey(jobId, JOB_GROUP);
		JobDetail job = scheduler.getJobDetail(key);
		if (job == null) {
			return null;
		}
		if (!RunPixelJobFromDB.class.equals(job.getJobClass()) || !recipe(engineId).equals(job.getJobDataMap().getString(JobConfigKeys.PIXEL))) {
			return new BoundJob(null);
		}
		List<CronTrigger> calendars = scheduler.getTriggersOfJob(key).stream().filter(CronTrigger.class::isInstance)
				.map(CronTrigger.class::cast).toList();
		return new BoundJob(calendars.size() == 1 ? calendars.getFirst().getCronExpression() : null);
	}

	String createJob(String name, String cron, String recipe) {
		Object value = run("ScheduleJob(jobName=[\"" + name + "\"], jobGroup=[\"" + JOB_GROUP + "\"], cronExpression=[\"" + cron
				+ "\"], recipe=[\"" + Utility.encodeURIComponent(recipe) + "\"]);");
		if (value instanceof Map<?, ?> result && result.get(JobConfigKeys.JOB_ID) instanceof String id && SAFE_ID.matcher(id).matches()) {
			return id;
		}
		throw new IllegalStateException("Scheduler did not return a job ID");
	}

	void removeJob(String jobId) {
		Object value = run("RemoveJobFromDB(jobId=[\"" + jobId + "\"], jobGroup=[\"" + JOB_GROUP + "\"]);");
		if (!(value instanceof Map<?, ?> result) || !(result.get("success") instanceof List<?> ids) || !ids.contains(jobId)) {
			throw new IllegalStateException("Scheduler did not confirm job removal");
		}
	}

	private Object run(String pixel) {
		NounMetadata result = this.insight.runPixel(pixel).getResults().getFirst();
		if (result.getOpType().contains(PixelOperationType.ERROR)) {
			throw new IllegalStateException("Scheduler call failed: " + result.getValue());
		}
		return result.getValue();
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Admin Only: Turns router learning on or off and reconciles the optimizer job")
				.key(ReactorKeysEnum.ENGINE, "The model router engine id")
				.key(ENABLED, "Whether router learning is enabled")
				.key(OPTIMIZER_MODEL, "The agent and judge model; required to enable learning")
				.key(MAX_QUALITY_LOSS, "Maximum observed quality loss (0 to 1, default 0)")
				.key(MIN_NEW_TASKS, "Minimum new runs before a learning attempt (default 100)")
				.key(FREQUENCY, "Calendar schedule: off, hourly, daily or weekly. The volume trigger remains enabled")
				.key(REPLAY, key -> key.type(Map.class).description("Optional replay settings to update when enabling learning"))
				.output(Map.class, "Whether learning is enabled and the scheduled jobs");
	}
}
