package prerna.reactor.agent.scheduler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.json.JSONObject;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import com.github.f4b6a3.uuid.alt.GUID;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.run.AgentRunHandle;
import prerna.reactor.agent.run.AgentRunRecord;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.reactor.agent.runtime.SemossAgentHarness;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/** Existing-scheduler implementation for Playground agent tasks. */
public final class AgentScheduleService {

	public static final String JOB_TAG = "PLAYGROUND_AGENT_TASK";
	public static final String JOB_GROUP = PlaygroundUtils.PLAYGROUND_PROJECT_ID;
	public static final String SCHEDULE_TYPE_ONCE = "ONCE";
	public static final String SCHEDULE_TYPE_RECURRING = "RECURRING";
	public static final String ROOM_MODE_FRESH = "FRESH";
	public static final String ROOM_MODE_CONTINUE = "CONTINUE";
	public static final String PARAM_SCHEDULED_RUN = "scheduledAgentRun";

	private static final String CLAIM_TIMEOUT_PROPERTY = "AGENT_SCHEDULE_CLAIM_TIMEOUT_SECONDS";
	private static final long DEFAULT_CLAIM_TIMEOUT_SECONDS = 300L;
	private static final int UI_STATE_VERSION = 1;
	private static final Gson GSON = new Gson();
	private static final TypeToken<Map<String, Object>> MAP_TYPE = new TypeToken<>() {
	};

	private AgentScheduleService() {
	}

	public static Map<String, Object> create(Insight insight, String name, String taskPrompt, String agentId,
			String modelId, String scheduleType, String cronExpression, String runAt, String timeZoneId, String roomMode,
			String continuingRoomId) {
		requireSchedulerEnabled();
		User user = requireUser(insight);
		String ownerId = ownerId(user);
		String normalizedTaskPrompt = required(taskPrompt, "prompt");
		String normalizedAgentId = trimToNull(agentId);
		String normalizedModelId = trimToNull(modelId);
		validateAgentAndModelAccess(user, normalizedAgentId, normalizedModelId);

		String normalizedScheduleType = normalizeScheduleType(scheduleType);
		ZoneId zoneId = resolveZone(timeZoneId);
		String normalizedCron = resolveCron(normalizedScheduleType, cronExpression, runAt, zoneId);
		String normalizedRoomMode = normalizeRoomMode(roomMode);
		String normalizedContinuingRoomId = trimToNull(continuingRoomId);
		validateRoomChoice(insight, normalizedRoomMode, normalizedContinuingRoomId);

		String scheduleId = UUID.randomUUID().toString();
		String jobName = trimToNull(name);
		if (jobName == null) {
			jobName = normalizedTaskPrompt.substring(0, Math.min(normalizedTaskPrompt.length(), 100));
		}

		Map<String, Object> uiState = new LinkedHashMap<>();
		uiState.put("version", UI_STATE_VERSION);
		uiState.put("prompt", normalizedTaskPrompt);
		uiState.put("agentId", normalizedAgentId);
		uiState.put("modelId", normalizedModelId);
		uiState.put("scheduleType", normalizedScheduleType);
		uiState.put("runAt", SCHEDULE_TYPE_ONCE.equals(normalizedScheduleType) ? required(runAt, "runAt") : null);
		uiState.put("timezone", zoneId.getId());
		uiState.put("roomMode", normalizedRoomMode);
		uiState.put("continuingRoomId", normalizedContinuingRoomId);
		uiState.put("activeRunId", null);
		uiState.put("activeRunStartedAt", null);

		String recipe = recipe(scheduleId);
		String providerInfo = providerInfo(user);
		TimeZone quartzTimeZone = TimeZone.getTimeZone(zoneId);
		JobKey jobKey = JobKey.jobKey(scheduleId, JOB_GROUP);
		Scheduler scheduler = null;
		try {
			scheduler = getStartedScheduler();
			if (scheduler.checkExists(jobKey)) {
				throw new IllegalStateException("Generated schedule id already exists");
			}
			JobDataMap dataMap = buildJobDataMap(scheduleId, jobName, JOB_GROUP, normalizedCron,
					quartzTimeZone, recipe, "", false, GSON.toJson(uiState), providerInfo);
			scheduleQuartzJob(scheduler, jobKey, dataMap, normalizedCron, quartzTimeZone);

			boolean saved = SchedulerDatabaseUtility.insertIntoJobRecipesTable(ownerId, scheduleId, jobName, JOB_GROUP,
					normalizedCron, quartzTimeZone, recipe, "", JOB_TAG, false, GSON.toJson(uiState), List.of(JOB_TAG));
			if (!saved) {
				IllegalStateException failure = new IllegalStateException("Failed to persist the agent schedule");
				if (!SchedulerDatabaseUtility.updateJobTags(scheduleId, null)) {
					failure.addSuppressed(new IllegalStateException("Failed to remove partially saved schedule tags"));
				}
				if (!SchedulerDatabaseUtility.removeFromJobRecipesTable(scheduleId, JOB_GROUP)) {
					failure.addSuppressed(new IllegalStateException("Failed to remove the partially saved schedule row"));
				}
				try {
					if (!scheduler.deleteJob(jobKey)) {
						failure.addSuppressed(new IllegalStateException("Failed to remove the partially created Quartz job"));
					}
				} catch (SchedulerException cleanupError) {
					failure.addSuppressed(cleanupError);
				}
				throw failure;
			}
			return getOwnedSchedule(insight, scheduleId).toMap(insight);
		} catch (SchedulerException e) {
			throw new IllegalStateException("Failed to create agent schedule: " + e.getMessage(), e);
		}
	}

	public static List<Map<String, Object>> list(Insight insight) {
		requireSchedulerEnabled();
		User user = requireUser(insight);
		String ownerId = ownerId(user);
		List<ScheduleRecord> records = new ArrayList<>();
		String sql = "SELECT jr.USER_ID, jr.JOB_ID, jr.JOB_NAME, jr.JOB_GROUP, jr.CRON_EXPRESSION, "
				+ "jr.CRON_TIMEZONE, jr.PIXEL_RECIPE, jr.PIXEL_RECIPE_PARAMETERS, jr.JOB_CATEGORY, "
				+ "jr.TRIGGER_ON_LOAD, jr.UI_STATE FROM SMSS_JOB_RECIPES jr WHERE jr.USER_ID=? "
				+ "AND EXISTS (SELECT 1 FROM SMSS_JOB_TAGS jt WHERE jt.JOB_ID=jr.JOB_ID AND jt.JOB_TAG=?) "
				+ "ORDER BY jr.JOB_NAME, jr.JOB_ID";
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, ownerId);
			statement.setString(2, JOB_TAG);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					records.add(readRecord(result));
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Failed to list agent schedules", e);
		} finally {
			closeIfPooled(db, connection);
		}
		Map<String, Map<String, Object>> latestBySchedule = readLatestHistory(ownerId, insight);
		Scheduler scheduler = null;
		String schedulerError = null;
		try {
			scheduler = getStartedScheduler();
		} catch (SchedulerException e) {
			schedulerError = e.getMessage();
		}
		List<Map<String, Object>> schedules = new ArrayList<>(records.size());
		for (ScheduleRecord record : records) {
			schedules.add(record.toMap(insight, scheduler, schedulerError, latestBySchedule.get(record.jobId())));
		}
		return schedules;
	}

	public static List<Map<String, Object>> history(Insight insight, String scheduleId, int limit) {
		requireSchedulerEnabled();
		String normalizedScheduleId = required(scheduleId, "scheduleId");
		ScheduleRecord record = getOwnedSchedule(insight, normalizedScheduleId);
		int boundedLimit = Math.max(1, Math.min(limit, 200));
		return readHistory(record, boundedLimit, insight);
	}

	public static Map<String, Object> manage(Insight insight, String scheduleId, String action,
			Map<String, String> updates) {
		requireSchedulerEnabled();
		String normalizedId = required(scheduleId, "scheduleId");
		String normalizedAction = required(action, "action").toUpperCase(Locale.ROOT);
		ScheduleRecord record = getOwnedSchedule(insight, normalizedId);
		try {
			Scheduler scheduler = getStartedScheduler();
			JobKey jobKey = JobKey.jobKey(record.jobId(), record.jobGroup());
			switch (normalizedAction) {
			case "PAUSE":
				scheduler.pauseTrigger(triggerKey(jobKey));
				break;
			case "RESUME":
				scheduler.resumeTrigger(triggerKey(jobKey));
				break;
			case "RUN_NOW":
				scheduler.triggerJob(jobKey);
				break;
			case "DELETE":
				boolean wasPaused = scheduler.getTriggerState(triggerKey(jobKey)) == Trigger.TriggerState.PAUSED;
				boolean deleted = scheduler.deleteJob(jobKey);
				if (!deleted) {
					throw new IllegalStateException("Agent schedule could not be deleted from Quartz");
				}
				try {
					deletePersistedSchedule(record);
				} catch (RuntimeException failure) {
					try {
						restoreQuartzSchedule(scheduler, record, wasPaused, insight.getUser());
					} catch (SchedulerException restoreError) {
						failure.addSuppressed(restoreError);
					}
					throw failure;
				}
				return Map.of("scheduleId", normalizedId, "status", "DELETED");
			case "UPDATE":
				return update(insight, record, updates != null ? updates : Collections.emptyMap(), scheduler);
			default:
				throw new IllegalArgumentException("action must be UPDATE, PAUSE, RESUME, DELETE, or RUN_NOW");
			}
			return getOwnedSchedule(insight, normalizedId).toMap(insight);
		} catch (SchedulerException e) {
			throw new IllegalStateException("Failed to manage agent schedule: " + e.getMessage(), e);
		}
	}

	public static Map<String, Object> runScheduled(Insight insight, String scheduleId, String jobGroup) {
		requireSchedulerMode(insight);
		String normalizedId = required(scheduleId, "scheduleId");
		if (!JOB_GROUP.equals(required(jobGroup, "jobGroup"))) {
			throw new SecurityException("Invalid agent schedule group");
		}
		ScheduleRecord record = getOwnedSchedule(insight, normalizedId);
		Map<String, Object> uiState = record.uiState();
		String prompt = required(stringValue(uiState.get("prompt")), "UI_STATE.prompt");
		String agentId = trimToNull(stringValue(uiState.get("agentId")));
		String modelId = trimToNull(stringValue(uiState.get("modelId")));
		validateAgentAndModelAccess(insight.getUser(), agentId, modelId);
		String roomMode = normalizeRoomMode(stringValue(uiState.get("roomMode")));
		String continuingRoomId = trimToNull(stringValue(uiState.get("continuingRoomId")));
		Room continuingRoom = validateRoomChoice(insight, roomMode, continuingRoomId);

		RunClaim claim = claimRun(insight, record);
		if (claim.skipped()) {
			Map<String, Object> skipped = new LinkedHashMap<>();
			skipped.put("status", "SKIPPED");
			skipped.put("runId", claim.runId());
			skipped.put("roomId", claim.roomId());
			skipped.put("skipReason", "ACTIVE_RUN");
			return skipped;
		}

		String roomId = continuingRoom != null ? continuingRoom.getId() : UUID.randomUUID().toString();
		try {
			IModelEngine model = modelId != null ? Utility.getModel(modelId) : null;
			if (continuingRoom == null) {
				RoomUtils.createRoomIfNotExists(roomId, insight, model, record.jobName(), agentId,
						freshRoomOptions(agentId), null,
						PlaygroundUtils.PLAYGROUND_PROJECT_ID, null);
			}
			AgentRunRequest request = new AgentRunRequest(roomId, prompt, modelId, SemossAgentHarness.NAME,
					agentId,
					AgentRunContext.DEFAULT_MAX_TURNS, AgentRunContext.DEFAULT_MAX_REFLECTIONS, Collections.emptyMap(),
					Map.of(PARAM_SCHEDULED_RUN, true), Collections.emptyList(), Collections.emptyList(), insight);
			AgentRunHandle handle = AgentRunService.get().runWithId(claim.runId(), request);
			Map<String, Object> output = new LinkedHashMap<>();
			output.put("status", handle.status().name());
			output.put("runId", handle.runId());
			output.put("roomId", handle.roomId());
			return output;
		} catch (Exception e) {
			clearFailedClaim(record, claim.runId());
			throw new IllegalStateException("Failed to submit scheduled agent run: " + e.getMessage(), e);
		}
	}

	private static Map<String, Object> freshRoomOptions(String agentId) {
		Map<String, Object> options = new LinkedHashMap<>();
		options.put("harnessType", SemossAgentHarness.NAME);
		options.put("instructions", "");
		options.put("mcp", Collections.emptyList());
		options.put("predefinedPrompts", Collections.emptyList());
		if (agentId != null) {
			options.put("workspace", Map.of("workspace_id", agentId));
		}
		return options;
	}

	private static Map<String, Object> update(Insight insight, ScheduleRecord record, Map<String, String> updates,
			Scheduler scheduler) throws SchedulerException {
		Map<String, Object> state = new LinkedHashMap<>(record.uiState());
		putIfPresent(state, "prompt", updates.get("prompt"));
		putNullableIfPresent(state, "agentId", updates, "agentId");
		putNullableIfPresent(state, "modelId", updates, "modelId");
		putIfPresent(state, "scheduleType", updates.get("scheduleType"));
		putNullableIfPresent(state, "runAt", updates, "runAt");
		putIfPresent(state, "timezone", updates.get("timezone"));
		putIfPresent(state, "roomMode", updates.get("roomMode"));
		putNullableIfPresent(state, "continuingRoomId", updates, "continuingRoomId");
		state.put("version", UI_STATE_VERSION);

		String prompt = required(stringValue(state.get("prompt")), "prompt");
		String agentId = trimToNull(stringValue(state.get("agentId")));
		String modelId = trimToNull(stringValue(state.get("modelId")));
		validateAgentAndModelAccess(insight.getUser(), agentId, modelId);
		String scheduleType = normalizeScheduleType(stringValue(state.get("scheduleType")));
		ZoneId zoneId = resolveZone(stringValue(state.get("timezone")));
		String cron = record.cronExpression();
		if (updates.containsKey("cronExpression") || updates.containsKey("runAt")
				|| updates.containsKey("scheduleType")) {
			cron = resolveCron(scheduleType, updates.get("cronExpression"), updates.get("runAt"), zoneId);
		}
		String roomMode = normalizeRoomMode(stringValue(state.get("roomMode")));
		String continuingRoomId = trimToNull(stringValue(state.get("continuingRoomId")));
		validateRoomChoice(insight, roomMode, continuingRoomId);

		String name = trimToNull(updates.get("name"));
		if (name == null) {
			name = record.jobName();
		}
		TimeZone timeZone = TimeZone.getTimeZone(zoneId);
		JobKey jobKey = JobKey.jobKey(record.jobId(), record.jobGroup());
		boolean wasPaused = scheduler.getTriggerState(triggerKey(jobKey)) == Trigger.TriggerState.PAUSED;
		String serializedState = GSON.toJson(state);
		JobDataMap dataMap = buildJobDataMap(record.jobId(), name, record.jobGroup(), cron, timeZone,
				record.recipe(), record.recipeParameters(), false, serializedState, providerInfo(insight.getUser()));
		try {
			updateQuartzJob(scheduler, jobKey, dataMap, cron, timeZone);
			if (wasPaused) {
				scheduler.pauseTrigger(triggerKey(jobKey));
			}
		} catch (SchedulerException e) {
			try {
				restoreQuartzSchedule(scheduler, record, wasPaused, insight.getUser());
			} catch (SchedulerException restoreError) {
				e.addSuppressed(restoreError);
			}
			throw e;
		}
		boolean saved = SchedulerDatabaseUtility.updateJobRecipesTable(record.ownerId(), record.jobId(), name,
				record.jobGroup(), cron, timeZone, record.recipe(), record.recipeParameters(), JOB_TAG, false,
				serializedState, record.jobName(), record.jobGroup(), List.of(JOB_TAG));
		if (!saved) {
			IllegalStateException failure = new IllegalStateException("Failed to persist updated agent schedule");
			TimeZone previousTimeZone = TimeZone.getTimeZone(record.cronTimeZone());
			boolean dbRestored = SchedulerDatabaseUtility.updateJobRecipesTable(record.ownerId(), record.jobId(),
					record.jobName(), record.jobGroup(), record.cronExpression(), previousTimeZone, record.recipe(),
					record.recipeParameters(), record.category(), record.triggerOnLoad(), GSON.toJson(record.uiState()),
					record.jobName(), record.jobGroup(), List.of(JOB_TAG));
			if (!dbRestored) {
				failure.addSuppressed(new IllegalStateException("Failed to restore the previous scheduler database row"));
			}
			try {
				restoreQuartzSchedule(scheduler, record, wasPaused, insight.getUser());
			} catch (SchedulerException restoreError) {
				failure.addSuppressed(restoreError);
			}
			throw failure;
		}
		return getOwnedSchedule(insight, record.jobId()).toMap(insight);
	}

	private static void restoreQuartzSchedule(Scheduler scheduler, ScheduleRecord record, boolean paused, User user)
			throws SchedulerException {
		TimeZone timeZone = TimeZone.getTimeZone(record.cronTimeZone());
		JobKey jobKey = JobKey.jobKey(record.jobId(), record.jobGroup());
		JobDataMap dataMap = buildJobDataMap(record.jobId(), record.jobName(), record.jobGroup(),
				record.cronExpression(), timeZone, record.recipe(), record.recipeParameters(), record.triggerOnLoad(),
				GSON.toJson(record.uiState()), providerInfo(user));
		if (scheduler.checkExists(jobKey)) {
			updateQuartzJob(scheduler, jobKey, dataMap, record.cronExpression(), timeZone);
		} else {
			scheduleQuartzJob(scheduler, jobKey, dataMap, record.cronExpression(), timeZone);
		}
		if (paused) {
			scheduler.pauseTrigger(triggerKey(jobKey));
		}
	}

	private static Scheduler getStartedScheduler() throws SchedulerException {
		Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
		SchedulerDatabaseUtility.startScheduler(scheduler);
		return scheduler;
	}

	private static JobDataMap buildJobDataMap(String jobId, String jobName, String jobGroup, String cronExpression,
			TimeZone cronTimeZone, String recipe, String recipeParameters, boolean triggerOnLoad, String uiState,
			String providerInfo) {
		JobDataMap dataMap = new JobDataMap();
		dataMap.put(providerInfo, triggerOnLoad);
		dataMap.put(JobConfigKeys.JOB_ID, jobId);
		dataMap.put(JobConfigKeys.JOB_NAME, jobName);
		dataMap.put(JobConfigKeys.JOB_GROUP, jobGroup);
		dataMap.put(JobConfigKeys.JOB_CRON_EXPRESSION, cronExpression);
		dataMap.put(JobConfigKeys.JOB_CRON_TIMEZONE, cronTimeZone.getID());
		dataMap.put(JobConfigKeys.TRIGGER_ON_LOAD, triggerOnLoad);
		dataMap.put(JobConfigKeys.UI_STATE, uiState);
		dataMap.put(JobConfigKeys.JOB_CLASS_NAME, "RunPixelJob");
		dataMap.put(JobConfigKeys.PIXEL, recipe);
		dataMap.put(JobConfigKeys.PIXEL_PARAMETERS, recipeParameters);
		dataMap.put(JobConfigKeys.USER_ACCESS, providerInfo);
		return dataMap;
	}

	private static void scheduleQuartzJob(Scheduler scheduler, JobKey jobKey, JobDataMap dataMap,
			String cronExpression, TimeZone cronTimeZone) throws SchedulerException {
		JobDetail job = JobBuilder.newJob(RunPixelJobFromDB.class).withIdentity(jobKey).usingJobData(dataMap)
				.storeDurably().build();
		scheduler.scheduleJob(job, buildTrigger(jobKey, cronExpression, cronTimeZone));
	}

	private static void updateQuartzJob(Scheduler scheduler, JobKey jobKey, JobDataMap dataMap,
			String cronExpression, TimeZone cronTimeZone) throws SchedulerException {
		if (!scheduler.checkExists(jobKey)) {
			throw new IllegalArgumentException("Scheduled agent task " + jobKey + " does not exist");
		}

		JobDetail currentJob = scheduler.getJobDetail(jobKey);
		JobDataMap currentData = currentJob.getJobDataMap();
		currentData.clear();
		currentData.putAll(dataMap);
		scheduler.addJob(currentJob, true);

		TriggerKey triggerKey = triggerKey(jobKey);
		Trigger trigger = buildTrigger(jobKey, cronExpression, cronTimeZone);
		if (scheduler.checkExists(triggerKey)) {
			scheduler.rescheduleJob(triggerKey, trigger);
		} else {
			scheduler.scheduleJob(trigger);
		}
	}

	private static Trigger buildTrigger(JobKey jobKey, String cronExpression, TimeZone cronTimeZone) {
		return TriggerBuilder.newTrigger().withIdentity(triggerKey(jobKey)).forJob(jobKey)
				.withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).inTimeZone(cronTimeZone)).build();
	}

	private static TriggerKey triggerKey(JobKey jobKey) {
		return TriggerKey.triggerKey(jobKey.getName() + "Trigger", jobKey.getGroup() + "TriggerGroup");
	}

	private static void deletePersistedSchedule(ScheduleRecord record) {
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		boolean originalAutoCommit = true;
		try {
			originalAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			try (PreparedStatement deleteTags = connection
					.prepareStatement("DELETE FROM SMSS_JOB_TAGS WHERE JOB_ID=?")) {
				deleteTags.setString(1, record.jobId());
				deleteTags.executeUpdate();
			}
			try (PreparedStatement deleteRecipe = connection
					.prepareStatement("DELETE FROM SMSS_JOB_RECIPES WHERE JOB_ID=? AND JOB_GROUP=? AND USER_ID=?")) {
				deleteRecipe.setString(1, record.jobId());
				deleteRecipe.setString(2, record.jobGroup());
				deleteRecipe.setString(3, record.ownerId());
				if (deleteRecipe.executeUpdate() != 1) {
					throw new IllegalStateException("Agent schedule database row was not deleted");
				}
			}
			connection.commit();
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackError) {
				e.addSuppressed(rollbackError);
			}
			if (e instanceof RuntimeException runtime) {
				throw runtime;
			}
			throw new IllegalStateException("Failed to delete the agent schedule database row", e);
		} finally {
			try {
				connection.setAutoCommit(originalAutoCommit);
			} catch (SQLException e) {
				// Connection close below is the recovery boundary for pooled connections.
			}
			closeIfPooled(db, connection);
		}
	}

	private static RunClaim claimRun(Insight insight, ScheduleRecord expected) {
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		boolean originalAutoCommit = true;
		try {
			originalAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			Map<String, Object> state = lockUiState(connection, expected.jobId(), expected.jobGroup(),
					ownerId(insight.getUser()));
			String activeRunId = trimToNull(stringValue(state.get("activeRunId")));
			String activeStartedAt = trimToNull(stringValue(state.get("activeRunStartedAt")));
			if (activeRunId != null) {
				AgentRunRecord active = AgentRunStore.getRun(activeRunId, insight);
				if (active != null && isActive(active.status())) {
					connection.commit();
					return new RunClaim(activeRunId, active.roomId(), true);
				}
				if (active == null && !isStale(activeStartedAt)) {
					connection.commit();
					return new RunClaim(activeRunId, null, true);
				}
			}

			String runId = GUID.v7().toUUID().toString();
			state.put("activeRunId", runId);
			state.put("activeRunStartedAt", Instant.now().toString());
			updateUiState(connection, expected.jobId(), expected.jobGroup(), state);
			connection.commit();
			return new RunClaim(runId, null, false);
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackError) {
				e.addSuppressed(rollbackError);
			}
			if (e instanceof RuntimeException runtime) {
				throw runtime;
			}
			throw new IllegalStateException("Failed to claim agent schedule run", e);
		} finally {
			try {
				connection.setAutoCommit(originalAutoCommit);
			} catch (SQLException e) {
				// Connection close below is the recovery boundary for pooled connections.
			}
			closeIfPooled(db, connection);
		}
	}

	private static void clearFailedClaim(ScheduleRecord record, String runId) {
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		boolean originalAutoCommit = true;
		try {
			originalAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			Map<String, Object> state = lockUiState(connection, record.jobId(), record.jobGroup(), record.ownerId());
			if (runId.equals(stringValue(state.get("activeRunId")))) {
				state = new LinkedHashMap<>(state);
				state.put("activeRunId", null);
				state.put("activeRunStartedAt", null);
				updateUiState(connection, record.jobId(), record.jobGroup(), state);
			}
			connection.commit();
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackError) {
				e.addSuppressed(rollbackError);
			}
			// The bounded stale-claim timeout is the fallback when cleanup fails.
		} finally {
			try {
				connection.setAutoCommit(originalAutoCommit);
			} catch (SQLException e) {
				// Connection close below is the recovery boundary for pooled connections.
			}
			closeIfPooled(db, connection);
		}
	}

	private static Map<String, Object> lockUiState(Connection connection, String jobId, String jobGroup,
			String expectedOwnerId) throws SQLException {
		String sql = "SELECT USER_ID, UI_STATE FROM SMSS_JOB_RECIPES WHERE JOB_ID=? AND JOB_GROUP=? "
				+ "AND EXISTS (SELECT 1 FROM SMSS_JOB_TAGS jt WHERE jt.JOB_ID=SMSS_JOB_RECIPES.JOB_ID "
				+ "AND jt.JOB_TAG=?) FOR UPDATE";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, jobId);
			statement.setString(2, jobGroup);
			statement.setString(3, JOB_TAG);
			try (ResultSet result = statement.executeQuery()) {
				if (!result.next()) {
					throw new IllegalArgumentException("Agent schedule does not exist");
				}
				if (!expectedOwnerId.equals(result.getString("USER_ID"))) {
					throw new SecurityException("Agent schedule is owned by another user");
				}
				return parseUiState(readBlob(result, "UI_STATE"));
			}
		}
	}

	private static ScheduleRecord getOwnedSchedule(Insight insight, String scheduleId) {
		User user = requireUser(insight);
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		try {
			ScheduleRecord record = readOwnedRecord(connection, scheduleId, JOB_GROUP, ownerId(user), true);
			if (record == null) {
				throw new IllegalArgumentException("Agent schedule does not exist or is owned by another user");
			}
			return record;
		} catch (SQLException e) {
			throw new IllegalStateException("Failed to load agent schedule", e);
		} finally {
			closeIfPooled(db, connection);
		}
	}

	private static ScheduleRecord readOwnedRecord(Connection connection, String scheduleId, String jobGroup,
			String ownerId, boolean requireTag) throws SQLException {
		String sql = "SELECT USER_ID, JOB_ID, JOB_NAME, JOB_GROUP, CRON_EXPRESSION, CRON_TIMEZONE, PIXEL_RECIPE, "
				+ "PIXEL_RECIPE_PARAMETERS, JOB_CATEGORY, TRIGGER_ON_LOAD, UI_STATE FROM SMSS_JOB_RECIPES "
				+ "WHERE JOB_ID=? AND JOB_GROUP=? AND USER_ID=?"
				+ (requireTag
						? " AND EXISTS (SELECT 1 FROM SMSS_JOB_TAGS jt WHERE jt.JOB_ID=SMSS_JOB_RECIPES.JOB_ID AND jt.JOB_TAG=?)"
						: "");
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, scheduleId);
			statement.setString(2, jobGroup);
			statement.setString(3, ownerId);
			if (requireTag) {
				statement.setString(4, JOB_TAG);
			}
			try (ResultSet result = statement.executeQuery()) {
				return result.next() ? readRecord(result) : null;
			}
		}
	}

	private static ScheduleRecord readRecord(ResultSet result) throws SQLException {
		return new ScheduleRecord(result.getString("USER_ID"), result.getString("JOB_ID"), result.getString("JOB_NAME"),
				result.getString("JOB_GROUP"), result.getString("CRON_EXPRESSION"), result.getString("CRON_TIMEZONE"),
				readBlob(result, "PIXEL_RECIPE"), readBlob(result, "PIXEL_RECIPE_PARAMETERS"),
				result.getString("JOB_CATEGORY"), result.getBoolean("TRIGGER_ON_LOAD"),
				parseUiState(readBlob(result, "UI_STATE")));
	}

	private static List<Map<String, Object>> readHistory(ScheduleRecord record, int limit, Insight insight) {
		StringBuilder sql = new StringBuilder("SELECT EXECUTION_START, EXECUTION_END, EXECUTION_DELTA, SUCCESS, "
				+ "IS_LATEST, SCHEDULER_OUTPUT FROM SMSS_AUDIT_TRAIL WHERE JOB_ID=? AND JOB_GROUP=? "
				+ "ORDER BY EXECUTION_START DESC");
		SystemEngineRegistry.getSchedulerDb().getQueryUtil().addLimitOffsetToQuery(sql, limit, 0);
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		List<Map<String, Object>> history = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
			statement.setString(1, record.jobId());
			statement.setString(2, record.jobGroup());
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					history.add(readHistoryItem(result));
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Failed to load agent schedule history", e);
		} finally {
			closeIfPooled(db, connection);
		}
		for (Map<String, Object> invocation : history) {
			enrichAgentRunStatus(invocation, insight);
		}
		return history;
	}

	private static Map<String, Map<String, Object>> readLatestHistory(String ownerId, Insight insight) {
		String sql = "SELECT trail.JOB_ID, trail.EXECUTION_START, trail.EXECUTION_END, trail.EXECUTION_DELTA, "
				+ "trail.SUCCESS, trail.IS_LATEST, trail.SCHEDULER_OUTPUT FROM SMSS_AUDIT_TRAIL trail "
				+ "INNER JOIN SMSS_JOB_RECIPES jr ON jr.JOB_ID=trail.JOB_ID AND jr.JOB_GROUP=trail.JOB_GROUP "
				+ "WHERE jr.USER_ID=? AND jr.JOB_GROUP=? AND trail.IS_LATEST=? "
				+ "AND EXISTS (SELECT 1 FROM SMSS_JOB_TAGS jt WHERE jt.JOB_ID=jr.JOB_ID AND jt.JOB_TAG=?) "
				+ "ORDER BY trail.EXECUTION_START ASC";
		IRDBMSEngine db = SystemEngineRegistry.getSchedulerDb();
		Connection connection = SchedulerDatabaseUtility.connectToScheduler();
		Map<String, Map<String, Object>> latest = new LinkedHashMap<>();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, ownerId);
			statement.setString(2, JOB_GROUP);
			statement.setBoolean(3, true);
			statement.setString(4, JOB_TAG);
			try (ResultSet result = statement.executeQuery()) {
				while (result.next()) {
					Map<String, Object> item = readHistoryItem(result);
					enrichAgentRunStatus(item, insight);
					latest.put(result.getString("JOB_ID"), item);
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Failed to load latest agent schedule history", e);
		} finally {
			closeIfPooled(db, connection);
		}
		return latest;
	}

	private static Map<String, Object> readHistoryItem(ResultSet result) throws SQLException {
		Map<String, Object> item = new LinkedHashMap<>();
		item.put("executionStart", timestampString(result.getTimestamp("EXECUTION_START")));
		item.put("executionEnd", timestampString(result.getTimestamp("EXECUTION_END")));
		item.put("executionDelta", result.getString("EXECUTION_DELTA"));
		item.put("success", result.getBoolean("SUCCESS"));
		item.put("latest", result.getBoolean("IS_LATEST"));
		item.putAll(normalizeAuditOutput(result.getString("SCHEDULER_OUTPUT")));
		return item;
	}

	private static void enrichAgentRunStatus(Map<String, Object> invocation, Insight insight) {
		String runId = trimToNull(stringValue(invocation.get("runId")));
		if (runId == null) {
			return;
		}
		try {
			Map<String, Object> run = AgentRunService.get().getRun(runId, insight);
			copyIfPresent(run, invocation, "status");
			copyIfPresent(run, invocation, "roomId");
			copyIfPresent(run, invocation, "startedAt");
			copyIfPresent(run, invocation, "completedAt");
			Object error = run.get("errorMessage");
			if (error != null && !String.valueOf(error).isBlank()) {
				invocation.put("error", error);
			}
		} catch (RuntimeException e) {
			// Keep the scheduler submission status when AGENT_RUN is not available yet.
		}
	}

	private static Map<String, Object> normalizeAuditOutput(String output) {
		Map<String, Object> normalized = new LinkedHashMap<>();
		if (output == null || output.isBlank()) {
			return normalized;
		}
		try {
			Object parsed = GSON.fromJson(output, Object.class);
			Map<String, Object> result = findAgentResult(parsed);
			if (result != null) {
				copyIfPresent(result, normalized, "status");
				copyIfPresent(result, normalized, "runId");
				copyIfPresent(result, normalized, "roomId");
				copyIfPresent(result, normalized, "skipReason");
			}
		} catch (Exception e) {
			normalized.put("status", "FAILED");
			normalized.put("error", output);
		}
		return normalized;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> findAgentResult(Object value) {
		if (value instanceof Map<?, ?> map) {
			if (map.containsKey("status") && (map.containsKey("runId") || map.containsKey("skipReason"))) {
				return (Map<String, Object>) map;
			}
			for (Object child : map.values()) {
				Map<String, Object> found = findAgentResult(child);
				if (found != null) {
					return found;
				}
			}
		} else if (value instanceof List<?> list) {
			for (Object child : list) {
				Map<String, Object> found = findAgentResult(child);
				if (found != null) {
					return found;
				}
			}
		}
		return null;
	}

	private static Room validateRoomChoice(Insight insight, String roomMode, String continuingRoomId) {
		if (ROOM_MODE_FRESH.equals(roomMode)) {
			return null;
		}
		if (continuingRoomId == null) {
			throw new IllegalArgumentException("continuingRoomId is required when roomMode is CONTINUE");
		}
		Room room = RoomUtils.getOrLoadRoom(continuingRoomId, insight);
		if (!PlaygroundUtils.PLAYGROUND_PROJECT_ID.equals(room.getProjectId())) {
			throw new IllegalArgumentException("Continuing room must be a Playground room");
		}
		return room;
	}

	private static void validateAgentAndModelAccess(User user, String agentId, String modelId) {
		if (agentId != null && !SecurityProjectUtils.userCanViewProject(user, agentId)) {
			throw new SecurityException("Agent does not exist or is not accessible");
		}
		String effectiveModelId = modelId;
		if (effectiveModelId == null && agentId != null) {
			JSONObject config = ModelInferenceLogsUtils.getWorkspaceConfigJson(agentId);
			effectiveModelId = config != null ? trimToNull(config.optString("model_id", null)) : null;
		}
		if (effectiveModelId == null) {
			throw new IllegalArgumentException("A model is required when the selected agent has no default model");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, effectiveModelId)
				|| SecurityEngineUtils.getEngineType(effectiveModelId) != IEngine.CATALOG_TYPE.MODEL) {
			throw new SecurityException("Model does not exist or is not accessible");
		}
	}

	private static String resolveCron(String scheduleType, String cronExpression, String runAt, ZoneId zoneId) {
		if (SCHEDULE_TYPE_RECURRING.equals(scheduleType)) {
			String cron = required(cronExpression, "cronExpression");
			SchedulerDatabaseUtility.validateInput("Agent schedule", JOB_GROUP, cron);
			return cron;
		}
		String value = required(runAt, "runAt");
		ZonedDateTime dateTime;
		try {
			dateTime = ZonedDateTime.of(LocalDateTime.parse(value), zoneId);
		} catch (DateTimeParseException e) {
			try {
				dateTime = Instant.parse(value).atZone(zoneId);
			} catch (DateTimeParseException second) {
				throw new IllegalArgumentException("runAt must be an ISO date-time", second);
			}
		}
		if (!dateTime.toInstant().isAfter(Instant.now())) {
			throw new IllegalArgumentException("runAt must be in the future");
		}
		return String.format(Locale.ROOT, "%d %d %d %d %d ? %d", dateTime.getSecond(), dateTime.getMinute(),
				dateTime.getHour(), dateTime.getDayOfMonth(), dateTime.getMonthValue(), dateTime.getYear());
	}

	private static ZoneId resolveZone(String timeZoneId) {
		String value = trimToNull(timeZoneId);
		if (value == null) {
			value = Utility.getApplicationTimeZoneId();
		}
		try {
			return ZoneId.of(value);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid timezone: " + value, e);
		}
	}

	private static String normalizeScheduleType(String value) {
		String normalized = trimToNull(value);
		if (normalized == null) {
			return SCHEDULE_TYPE_RECURRING;
		}
		normalized = normalized.toUpperCase(Locale.ROOT);
		if (!SCHEDULE_TYPE_ONCE.equals(normalized) && !SCHEDULE_TYPE_RECURRING.equals(normalized)) {
			throw new IllegalArgumentException("scheduleType must be ONCE or RECURRING");
		}
		return normalized;
	}

	private static String normalizeRoomMode(String value) {
		String normalized = trimToNull(value);
		if (normalized == null) {
			return ROOM_MODE_FRESH;
		}
		normalized = normalized.toUpperCase(Locale.ROOT);
		if (!ROOM_MODE_FRESH.equals(normalized) && !ROOM_MODE_CONTINUE.equals(normalized)) {
			throw new IllegalArgumentException("roomMode must be FRESH or CONTINUE");
		}
		return normalized;
	}

	private static String providerInfo(User user) {
		List<String> providers = new ArrayList<>();
		for (AuthProvider provider : user.getLogins()) {
			AccessToken token = user.getAccessToken(provider);
			if (token != null && token.getId() != null) {
				providers.add(provider.name() + ":" + token.getId());
			}
		}
		if (providers.isEmpty()) {
			throw new SecurityException("Authenticated scheduler identity is required");
		}
		return String.join(",", providers);
	}

	private static String ownerId(User user) {
		if (user.getPrimaryLoginToken() == null || trimToNull(user.getPrimaryLoginToken().getId()) == null) {
			throw new SecurityException("Authenticated user is required");
		}
		return user.getPrimaryLoginToken().getId().trim();
	}

	private static User requireUser(Insight insight) {
		if (insight == null || insight.getUser() == null) {
			throw new SecurityException("Authenticated user is required");
		}
		ownerId(insight.getUser());
		return insight.getUser();
	}

	private static void requireSchedulerMode(Insight insight) {
		requireSchedulerEnabled();
		if (!Boolean.TRUE.equals(ThreadStore.isSchedulerMode()) && (insight == null || !insight.isSchedulerMode())) {
			throw new SecurityException("RunScheduledAgent may only run through the scheduler");
		}
	}

	private static void requireSchedulerEnabled() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
	}

	private static boolean isActive(AgentRunStatus status) {
		return status == AgentRunStatus.SUBMITTED || status == AgentRunStatus.RUNNING
				|| status == AgentRunStatus.INPUT_REQUIRED;
	}

	private static boolean isStale(String startedAt) {
		if (startedAt == null) {
			return true;
		}
		try {
			long timeout = getLongProperty(CLAIM_TIMEOUT_PROPERTY, DEFAULT_CLAIM_TIMEOUT_SECONDS);
			return Instant.parse(startedAt).plusSeconds(timeout).isBefore(Instant.now());
		} catch (Exception e) {
			return true;
		}
	}

	private static long getLongProperty(String key, long fallback) {
		String value = Utility.getDIHelperProperty(key);
		try {
			long parsed = value != null ? Long.parseLong(value.trim()) : fallback;
			return parsed > 0 ? parsed : fallback;
		} catch (NumberFormatException e) {
			return fallback;
		}
	}

	private static void updateUiState(Connection connection, String jobId, String jobGroup, Map<String, Object> state)
			throws SQLException, IOException {
		try (PreparedStatement statement = connection
				.prepareStatement("UPDATE SMSS_JOB_RECIPES SET UI_STATE=? WHERE JOB_ID=? AND JOB_GROUP=?")) {
			SchedulerDatabaseUtility.getQueryUtil().handleInsertionOfBlob(connection, statement, GSON.toJson(state), 1);
			statement.setString(2, jobId);
			statement.setString(3, jobGroup);
			if (statement.executeUpdate() != 1) {
				throw new IllegalStateException("Agent schedule UI_STATE update did not match one row");
			}
		}
	}

	private static String readBlob(ResultSet result, String column) throws SQLException {
		try {
			return SchedulerDatabaseUtility.getQueryUtil().handleBlobRetrieval(result, column);
		} catch (IOException e) {
			throw new SQLException("Failed to read " + column, e);
		}
	}

	private static Map<String, Object> parseUiState(String json) {
		if (json == null || json.isBlank()) {
			throw new IllegalStateException("Agent schedule UI_STATE is missing");
		}
		Map<String, Object> state = GSON.fromJson(json, MAP_TYPE.getType());
		if (state == null) {
			throw new IllegalStateException("Agent schedule UI_STATE is invalid");
		}
		return state;
	}

	private static String recipe(String scheduleId) {
		return "RunScheduledAgent(scheduleId=[\"" + scheduleId + "\"], jobGroup=[\"" + JOB_GROUP + "\"]);";
	}

	private static String required(String value, String name) {
		String normalized = trimToNull(value);
		if (normalized == null) {
			throw new IllegalArgumentException(name + " is required");
		}
		return normalized;
	}

	private static String trimToNull(String value) {
		if (value == null) {
			return null;
		}
		String normalized = value.trim();
		return normalized.isEmpty() ? null : normalized;
	}

	private static String stringValue(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	private static void putIfPresent(Map<String, Object> target, String key, String value) {
		if (value != null) {
			target.put(key, value);
		}
	}

	private static void putNullableIfPresent(Map<String, Object> target, String targetKey, Map<String, String> source,
			String sourceKey) {
		if (source.containsKey(sourceKey)) {
			target.put(targetKey, trimToNull(source.get(sourceKey)));
		}
	}

	private static void copyIfPresent(Map<String, Object> source, Map<String, Object> target, String key) {
		if (source.containsKey(key)) {
			target.put(key, source.get(key));
		}
	}

	private static String timestampString(Timestamp timestamp) {
		return timestamp != null ? timestamp.toInstant().toString() : null;
	}

	private static void closeIfPooled(IRDBMSEngine db, Connection connection) {
		if (db.isConnectionPooling() && connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// Nothing useful can be done after the operation is complete.
			}
		}
	}

	private record RunClaim(String runId, String roomId, boolean skipped) {
	}

	private record ScheduleRecord(String ownerId, String jobId, String jobName, String jobGroup, String cronExpression,
			String cronTimeZone, String recipe, String recipeParameters, String category, boolean triggerOnLoad,
			Map<String, Object> uiState) {

		Map<String, Object> toMap(Insight insight) {
			Scheduler scheduler = null;
			String schedulerError = null;
			try {
				scheduler = getStartedScheduler();
			} catch (SchedulerException e) {
				schedulerError = e.getMessage();
			}
			List<Map<String, Object>> latest = readHistory(this, 1, insight);
			return toMap(insight, scheduler, schedulerError, latest.isEmpty() ? null : latest.get(0));
		}

		Map<String, Object> toMap(Insight insight, Scheduler scheduler, String schedulerError,
				Map<String, Object> latestInvocation) {
			Map<String, Object> output = new LinkedHashMap<>();
			output.put("scheduleId", jobId);
			output.put("name", jobName);
			output.put("cronExpression", cronExpression);
			output.put("timezone", cronTimeZone);
			output.putAll(uiState);
			if (scheduler == null) {
				output.put("triggerState", "ERROR");
				output.put("schedulerError", schedulerError);
			} else {
				try {
					JobKey key = JobKey.jobKey(jobId, jobGroup);
					Trigger trigger = scheduler.getTrigger(triggerKey(key));
					Trigger.TriggerState state = scheduler.getTriggerState(triggerKey(key));
					output.put("paused", state == Trigger.TriggerState.PAUSED);
					output.put("triggerState", state.name());
					output.put("nextExecution", trigger != null && trigger.getNextFireTime() != null
							? trigger.getNextFireTime().toInstant().toString()
							: null);
				} catch (SchedulerException e) {
					output.put("triggerState", "ERROR");
					output.put("schedulerError", e.getMessage());
				}
			}
			String activeRunId = trimToNull(stringValue(uiState.get("activeRunId")));
			if (activeRunId != null) {
				try {
					Map<String, Object> run = AgentRunService.get().getRun(activeRunId, insight);
					output.put("runStatus", run.get("status"));
					output.put("activeRoomId", run.get("roomId"));
					output.put("runError", run.get("errorMessage"));
				} catch (Exception e) {
					output.put("runStatus", "CLAIMED");
				}
			}
			output.put("latestInvocation", latestInvocation);
			return output;
		}
	}
}
