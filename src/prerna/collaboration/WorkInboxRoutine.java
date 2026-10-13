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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.javatuples.Pair;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.py.PyUtils;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry.Subscription;
import prerna.om.Insight;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.run.AutomationRunStore;
import prerna.reactor.automation.run.RunAutomationDefinitionReactor;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.scheduler.ScheduleJobReactor;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.reactor.scheduler.SchedulerFactorySingleton;
import prerna.rpa.quartz.jobs.insight.RunPixelJobFromDB;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/** One private, opted-in inbox brief per owner, driven by the existing SEMOSS scheduler. */
public final class WorkInboxRoutine {
	public static final String ID = "inbox-daily-brief";
	/** AUTOMATION_ID of the brief's runs in the scheduler database. */
	public static final String AUTOMATION_ID = "collaboration:inbox-daily-brief";
	private static final String DEFINITION = "inbox-daily-brief.automation.json";
	private static final String GROUP = "SYSTEM__COLLABORATION_ROUTINES";
	private static final String RECIPE = "WorkRunInboxRoutine();";
	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";
	private static final List<String> WEEKDAYS = List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN");
	private static final Set<String> TERMINAL = Set.of(AutomationConstants.STATUS_SUCCESS, AutomationConstants.STATUS_FAILED,
			AutomationConstants.STATUS_CANCELLED, AutomationConstants.STATUS_INTERRUPTED);
	private static final String FAILED = "The inbox brief failed. Check your Microsoft connection and Collaboration model, then retry on the next scheduled day.";
	private static final String PYTHON_DISABLED = "Python is disabled on this server; the inbox brief needs it.";

	private WorkInboxRoutine() { }

	public static Map<String, Object> get(Insight insight) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		Map<String, Object> row = stored(owner);
		if (row != null && "running".equals(row.get("status"))) {
			String started = (String) row.get("startedAt");
			// Above the run's own bounds: about 5 minutes of sync wait plus 10 minutes of agent wait.
			boolean stale = started != null && Instant.parse(started).plusSeconds(1200).isBefore(Instant.now());
			String runId = (String) row.get("runId");
			Map<String, Object> run = runId == null ? null : AutomationRunStore.getRunDetail(runId);
			if (run != null && TERMINAL.contains(run.get(AutomationConstants.STATUS))) {
				finishFromRun(owner, runId, started);
			} else if (stale) {
				try {
					// Older agent-run ids have no automation row, and cancelling a missing row throws.
					if (run != null) {
						RunAutomationDefinitionReactor.cancelRun(runId);
					}
				} finally {
					finish(owner, "failed", null, "The inbox brief was interrupted. The next scheduled run will retry.", null, started);
				}
			}
		}
		return get(insight.getUser());
	}

	public static Map<String, Object> get(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> row = stored(owner);
		Map<String, Object> result = new LinkedHashMap<>(row == null ? defaults() : settings(row));
		boolean enabled = row != null && Boolean.TRUE.equals(row.get("enabled"));
		boolean schedulerAvailable = schedulerAvailable();
		String mailboxId = mailboxId(owner);
		boolean mailboxAvailable = mailboxToken(mailboxId, user) != null;
		boolean pythonAvailable = PyUtils.pyEnabled();
		boolean assistantAvailable = pythonAvailable && model(user) != null;
		result.put("id", ID);
		result.put("configured", row != null);
		result.put("enabled", enabled);
		result.put("schedulerAvailable", schedulerAvailable);
		result.put("mailboxAvailable", mailboxAvailable);
		result.put("assistantAvailable", assistantAvailable);
		result.put("availabilityMessage", !schedulerAvailable ? "The SEMOSS scheduler is unavailable. Ask your administrator to enable it."
				: !mailboxAvailable ? mailboxUnavailable(mailboxId)
				: !pythonAvailable ? PYTHON_DISABLED
				: !assistantAvailable ? "Ask your administrator to configure a Collaboration text model and grant your primary account access." : null);
		result.put("nextRunAt", enabled && schedulerAvailable ? nextRun(owner) : null);
		result.put("lastRun", row == null || row.get("status") == null ? null : lastRun(row));
		return result;
	}

	public static Map<String, Object> save(User user, Map<String, Object> changes) {
		Map<String, Object> config = validate(changes);
		boolean enabled = (Boolean) config.remove("enabled");
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String mailboxId = enabled ? mailboxId(owner) : null;
		AccessToken mailboxToken = enabled ? mailboxToken(mailboxId, user) : null;
		if (enabled) {
			if (!schedulerAvailable()) {
				throw new IllegalArgumentException("The SEMOSS scheduler is unavailable");
			}
			if (mailboxToken == null) {
				throw new IllegalArgumentException(mailboxUnavailable(mailboxId));
			}
			if (!PyUtils.pyEnabled()) {
				throw new IllegalArgumentException(PYTHON_DISABLED);
			}
			if (model(user) == null) {
				throw new IllegalArgumentException("A Collaboration text model is required for inbox briefs");
			}
		}
		synchronized (CollaborationDbUtils.ownerLock(ID, owner.getValue0(), owner.getValue1())) {
			try {
				// The row lock serializes schedule changes across servers; the executor claims this same row.
				CollaborationDbUtils.inTransaction(conn -> {
					int changed = CollaborationDbUtils.update(conn, "UPDATE WORK_INBOX_ROUTINE SET SETTINGS_JSON = ?, ENABLED = ?, UPDATED_AT = ?"
							+ OWNED, CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now(), owner.getValue0(), owner.getValue1());
					if (changed == 0) {
						CollaborationDbUtils.update(conn, "INSERT INTO WORK_INBOX_ROUTINE (OWNER_ID, OWNER_TYPE, SETTINGS_JSON, ENABLED, UPDATED_AT) VALUES (?, ?, ?, ?, ?)",
								owner.getValue0(), owner.getValue1(), CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now());
					}
					if (enabled) {
						MicrosoftGraphCredentialStore.save(mailboxToken);
						schedule(owner, config);
						CollaborationDbUtils.update(conn, "UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?" + OWNED,
								true, owner.getValue0(), owner.getValue1());
					} else {
						pause(owner);
					}
				});
			} catch (RuntimeException e) {
				// A scheduler write and the Collaboration transaction cannot commit together; leave consent revoked.
				try {
					CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET ENABLED = ?" + OWNED,
							false, owner.getValue0(), owner.getValue1());
				} catch (RuntimeException cleanup) {
					e.addSuppressed(cleanup);
				} finally {
					pause(owner);
				}
				throw e;
			}
		}
		return get(user);
	}

	public static Map<String, Object> run(Insight insight) {
		User user = insight.getUser();
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> row = stored(owner);
		if (row == null || !Boolean.TRUE.equals(row.get("enabled"))) {
			return Map.of("status", "skipped");
		}
		Map<String, Object> config = settings(row);
		LocalDate today = LocalDate.now(ZoneId.of((String) config.get("timezone")));
		if (!((List<?>) config.get("days")).contains(WEEKDAYS.get(today.getDayOfWeek().getValue() - 1))) {
			return Map.of("status", "skipped");
		}
		java.sql.Timestamp startedAt = CollaborationDbUtils.now();
		startedAt.setNanos(startedAt.getNanos() / 1_000_000 * 1_000_000);
		String started = CollaborationDbUtils.toIso(startedAt);
		// shortcut: the once-per-day guard is off for testing, so every scheduled time runs and two servers can
		// both deliver; before release restore " AND (LAST_DATE IS NULL OR LAST_DATE <> ?)" (+ today.toString()).
		// Preserve an existing successful brief's cutoff when upgrading an older routine row.
		int claimed = CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET LAST_SUCCESS_AT = CASE "
				+ "WHEN LAST_SUCCESS_AT IS NULL AND RUN_STATUS = 'completed' THEN STARTED_AT ELSE LAST_SUCCESS_AT END, "
				+ "LAST_DATE = ?, RUN_STATUS = ?, STARTED_AT = ?, "
				+ "FINISHED_AT = NULL, ROOM_ID = NULL, RUN_ID = NULL, SUMMARY = NULL, ERROR = NULL" + OWNED
				+ " AND ENABLED = ?", today.toString(), "running", startedAt,
				owner.getValue0(), owner.getValue1(), true);
		if (claimed == 0) {
			return Map.of("status", "skipped");
		}
		AccessToken previousToken = user.getAccessToken(AuthProvider.MICROSOFT);
		AuthProvider previousPrimary = user.getPrimaryLogin();
		String mailboxId = null;
		String attachedToken = null;
		try {
			if (SecurityQueryUtils.getUnlockedUser(owner.getValue0(), owner.getValue1()) == null) {
				throw new IllegalArgumentException("The routine owner account is no longer active");
			}
			mailboxId = mailboxId(owner);
			if (!MicrosoftGraphCredentialStore.isRefreshable(MicrosoftGraphCredentialStore.get(mailboxId))) {
				// Older opted-in routines may still hold their credentials in the subscription registry.
				AccessToken mailboxToken = mailboxToken(mailboxId, null);
				if (mailboxToken == null) {
					throw new IllegalArgumentException(mailboxUnavailable(mailboxId));
				}
				MicrosoftGraphCredentialStore.save(mailboxToken);
			}
			// Refresh and persist first, then lend the stored owner's delegated /me token to this session user,
			// whose Python process runs the automation; a new User's process would never be shut down.
			attachedToken = MicrosoftGraphCredentialStore.accessToken(mailboxId);
			user.setAccessToken(MicrosoftGraphCredentialStore.get(mailboxId));
			user.setPrimaryLogin(AuthProvider.getProviderFromString(owner.getValue1()));
			String recipient = BrainMailImport.first(BrainMailHeaderSource.current().me(user), "mail", "userPrincipalName");
			if (recipient == null) {
				throw new IllegalArgumentException(mailboxUnavailable(mailboxId));
			}
			String engine = model(user);
			if (engine == null) {
				throw new IllegalArgumentException("A Collaboration text model is required for inbox briefs");
			}
			AutomationDefinitionService.DefinitionFiles files = briefDefinition(engine, ((Number) config.get("emailCount")).intValue());
			String runId = UUID.randomUUID().toString();
			// Attach before running; no row means the routine was paused or a newer run took the day.
			int attached = CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_ID = ?" + OWNED
					+ " AND STARTED_AT = ? AND ENABLED = ? AND RUN_STATUS = 'running'", runId, owner.getValue0(), owner.getValue1(),
					startedAt, true);
			if (attached == 0) {
				finish(owner, "failed", null, "Routine was paused before the brief started.", null, started);
				return Map.of("status", "skipped");
			}
			RunAutomationDefinitionReactor.runDefinition(insight, runId, AUTOMATION_ID, files, Map.of("owner_email", recipient),
					AutomationConstants.TRIGGER_SCHEDULED, null);
			return Map.of("status", finishFromRun(owner, runId, started));
		} catch (Exception e) {
			boolean interrupted = Thread.interrupted() || e instanceof InterruptedException;
			try {
				// Provider errors can contain message bodies or credentials; expose only an action message.
				finish(owner, "failed", null, FAILED, null, started);
			} finally {
				// Interrupted socket I/O can close a shared database connection; restore after persistence.
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}
			throw new IllegalStateException("The inbox brief failed; check its status in Routines", e);
		} finally {
			if (attachedToken != null) {
				try {
					AccessToken current = user.getAccessToken(AuthProvider.MICROSOFT);
					// A Graph call during the run may have refreshed the lent token; keep the new pair for the next run.
					if (current != null && !Objects.equals(attachedToken, current.getAccess_token())
							&& MicrosoftGraphCredentialStore.isRefreshable(current) && mailboxId.equals(current.getId())) {
						MicrosoftGraphCredentialStore.save(current);
					}
				} finally {
					// An interactive caller's session must not keep the stored credential once nothing reads it.
					// A scheduled Microsoft login's own token has no access token, so it waits for the sync too.
					user.setPrimaryLogin(previousPrimary);
					if (!syncReading(user, owner)) {
						if (previousToken != null) {
							user.setAccessToken(previousToken);
						} else {
							user.dropAccessToken(AuthProvider.MICROSOFT);
						}
					}
				}
			}
		}
	}

	static Map<String, Object> defaults() {
		return new LinkedHashMap<>(Map.of("time", "08:00", "days", WEEKDAYS.subList(0, 5), "timezone", "UTC", "emailCount", 25));
	}

	static Map<String, Object> validate(Map<String, Object> input) {
		if (input == null || !(input.get("enabled") instanceof Boolean)) {
			throw new IllegalArgumentException("enabled must be true or false");
		}
		Set<String> allowed = Set.of("enabled", "time", "days", "timezone", "emailCount");
		if (!allowed.containsAll(input.keySet())) {
			throw new IllegalArgumentException("Unknown inbox routine setting");
		}
		String time = String.valueOf(input.get("time"));
		if (!time.matches("(?:[01][0-9]|2[0-3]):[0-5][0-9]")) {
			throw new IllegalArgumentException("time must be HH:mm in 24-hour format");
		}
		String timezone = String.valueOf(input.get("timezone"));
		try {
			ZoneId.of(timezone);
		} catch (Exception e) {
			throw new IllegalArgumentException("timezone must be a valid IANA time zone");
		}
		if (!(input.get("days") instanceof List<?> days) || days.isEmpty() || days.size() > 7
				|| !WEEKDAYS.containsAll(days) || days.stream().distinct().count() != days.size()) {
			throw new IllegalArgumentException("Select one or more distinct weekdays (MON through SUN)");
		}
		Object count = input.get("emailCount");
		if (!(count instanceof Number number) || number.doubleValue() != number.intValue() || number.intValue() < 1 || number.intValue() > 100) {
			throw new IllegalArgumentException("emailCount must be an integer from 1 to 100");
		}
		Map<String, Object> config = new LinkedHashMap<>();
		config.put("enabled", input.get("enabled"));
		config.put("time", time);
		config.put("timezone", timezone);
		config.put("days", WEEKDAYS.stream().filter(((List<?>) input.get("days"))::contains).toList());
		config.put("emailCount", ((Number) count).intValue());
		return config;
	}

	static String cron(Map<String, Object> config) {
		LocalTime time = LocalTime.parse((String) config.get("time"));
		return "0 " + time.getMinute() + " " + time.getHour() + " ? * "
				+ String.join(",", ((List<?>) config.get("days")).stream().map(String::valueOf).toList());
	}

	/** The server-held brief automation, with the owner's model and thread count filled in. */
	@SuppressWarnings("unchecked")
	static AutomationDefinitionService.DefinitionFiles briefDefinition(String engineId, int threadLimit) {
		Map<String, Object> definition;
		try (InputStream in = WorkInboxRoutine.class.getResourceAsStream(DEFINITION)) {
			if (in == null) {
				throw new IllegalStateException("The inbox brief definition is missing from this server build");
			}
			definition = AutomationRuntimeUtils.GSON.fromJson(new String(in.readAllBytes(), StandardCharsets.UTF_8),
					AutomationRuntimeUtils.MAP_TYPE);
		} catch (IOException e) {
			throw new IllegalStateException("Could not read the inbox brief definition", e);
		}
		Map<String, Object> graph = (Map<String, Object>) definition.get(AutomationConstants.DOC_GRAPH);
		for (Map<String, Object> node : (List<Map<String, Object>>) graph.get(AutomationConstants.DOC_NODES)) {
			Map<String, Object> config = (Map<String, Object>) node.get(AutomationConstants.NODE_FIELD_CONFIG);
			if ("brief".equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				config.put(AutomationConstants.CONFIG_ENGINE_ID, engineId);
			} else if ("threads".equals(node.get(AutomationConstants.NODE_FIELD_ID))) {
				config.put(AutomationConstants.CONFIG_PIXEL, "BrainListThreads(limit=[" + threadLimit + "], offset=[0], detail=[true]);");
			}
		}
		return AutomationDefinitionService.fromInline(AutomationRuntimeUtils.GSON.toJson(definition), null);
	}

	private static boolean schedulerAvailable() {
		return !Utility.schedulerForceDisable() && SystemEngineRegistry.isSchedulerDbLoaded()
				&& Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT) != null
				&& !Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT).isBlank();
	}

	private static String model(User user) {
		try {
			Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
			User scheduledUser = new User();
			AccessToken identity = new AccessToken();
			identity.setProvider(AuthProvider.getProviderFromString(owner.getValue1()));
			identity.setId(owner.getValue0());
			scheduledUser.setAccessToken(identity);
			try {
				return BrainTopicModel.engine(scheduledUser);
			} finally {
				scheduledUser.removeUserMemory();
			}
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	// The sync node's background job and its thread summaries read Graph through this same user and can outlive the
	// run; a scheduler session user ends with them, so it keeps the lent token until then.
	private static boolean syncReading(User user, Pair<String, String> owner) {
		Map<String, Object> sync = CollaborationJobUtils.latest(owner.getValue0(), owner.getValue1(), BrainSync.KIND);
		return sync != null && CollaborationJobUtils.RUNNING.equals(sync.get("status"))
				|| WorkThreadInsights.pendingCount(user) > 0;
	}

	private static String mailboxId(Pair<String, String> owner) {
		return CollaborationDbUtils.queryOne("SELECT MS_USER_ID FROM COLLAB_OWNER" + OWNED,
				rs -> rs.getString(1), owner.getValue0(), owner.getValue1());
	}

	private static String mailboxUnavailable(String msId) {
		return msId == null || msId.isBlank()
				? "Connect your Microsoft inbox in Brain and complete the initial mail import before enabling this routine."
				: "Your inbox is linked in Brain, but scheduled access needs a renewable Microsoft sign-in. "
						+ "Sign out of Microsoft and reconnect in Brain, then enable this routine.";
	}

	private static AccessToken mailboxToken(String msId, User user) {
		if (msId == null || msId.isBlank()) {
			return null;
		}
		AccessToken current = user == null ? null : user.getAccessToken(AuthProvider.MICROSOFT);
		if (MicrosoftGraphCredentialStore.isRefreshable(current) && msId.equals(current.getId())) {
			return current;
		}
		AccessToken saved = MicrosoftGraphCredentialStore.get(msId);
		if (MicrosoftGraphCredentialStore.isRefreshable(saved) && msId.equals(saved.getId())) {
			return saved;
		}
		for (Subscription candidate : MicrosoftGraphSubscriptionRegistry.forUser(msId)) {
			User credentialUser = candidate.getUser();
			try {
				AccessToken token = credentialUser.getAccessToken(AuthProvider.MICROSOFT);
				if (MicrosoftGraphCredentialStore.isRefreshable(token) && msId.equals(token.getId())) {
					return token;
				}
			} finally {
				credentialUser.removeUserMemory();
			}
		}
		return null;
	}

	private static void schedule(Pair<String, String> owner, Map<String, Object> config) {
		try {
			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
			SchedulerDatabaseUtility.startScheduler(scheduler);
			JobKey key = jobKey(owner);
			boolean existing = SchedulerDatabaseUtility.existsInJobRecipesTable(key.getName(), GROUP);
			String expression = cron(config);
			TimeZone zone = TimeZone.getTimeZone(ZoneId.of((String) config.get("timezone")));
			String provider = AuthProvider.getProviderFromString(owner.getValue1()).name() + ":" + owner.getValue0();
			JobDetail job = JobBuilder.newJob(RunPixelJobFromDB.class).withIdentity(key).storeDurably()
					.usingJobData(ScheduleJobReactor.getJobDataMap(key.getName(), "Daily inbox brief", GROUP, expression, zone,
							RECIPE, "", false, null, provider)).build();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey(owner)).forJob(key)
					.withSchedule(CronScheduleBuilder.cronSchedule(expression).inTimeZone(zone).withMisfireHandlingInstructionDoNothing()).build();
			scheduler.addJob(job, true);
			if (scheduler.checkExists(triggerKey(owner))) {
				scheduler.rescheduleJob(triggerKey(owner), trigger);
			} else {
				scheduler.scheduleJob(trigger);
			}
			scheduler.resumeJob(key);
			boolean saved = existing
					? SchedulerDatabaseUtility.updateJobRecipesTable(owner.getValue0(), key.getName(), "Daily inbox brief", GROUP,
							expression, zone, RECIPE, "", "Routine", false, null, "Daily inbox brief", GROUP, List.of("collaboration", ID))
					: SchedulerDatabaseUtility.insertIntoJobRecipesTable(owner.getValue0(), key.getName(), "Daily inbox brief", GROUP,
							expression, zone, RECIPE, "", "Routine", false, null, List.of("collaboration", ID));
			if (!saved) {
				scheduler.pauseJob(key);
				throw new IllegalStateException("Could not save the inbox routine schedule; the routine remains paused");
			}
		} catch (SchedulerException e) {
			throw new IllegalStateException("Could not schedule the inbox routine; the routine remains paused", e);
		}
	}

	private static void pause(Pair<String, String> owner) {
		if (!schedulerAvailable()) {
			return;
		}
		try {
			SchedulerFactorySingleton.getInstance().getScheduler().pauseJob(jobKey(owner));
		} catch (SchedulerException e) {
			// Opt-in was already revoked in the database, so a stale trigger cannot read mail.
		}
	}

	private static String nextRun(Pair<String, String> owner) {
		try {
			Trigger trigger = SchedulerFactorySingleton.getInstance().getScheduler().getTrigger(triggerKey(owner));
			Date next = trigger == null ? null : trigger.getNextFireTime();
			return next == null ? null : next.toInstant().toString();
		} catch (SchedulerException e) {
			return null;
		}
	}

	private static JobKey jobKey(Pair<String, String> owner) {
		return JobKey.jobKey(CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), ID), GROUP);
	}

	private static TriggerKey triggerKey(Pair<String, String> owner) {
		return TriggerKey.triggerKey(jobKey(owner).getName() + "Trigger", GROUP + "TriggerGroup");
	}

	private static Map<String, Object> stored(Pair<String, String> owner) {
		return CollaborationDbUtils.queryOne("SELECT * FROM WORK_INBOX_ROUTINE" + OWNED, rs -> {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("settings", CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "SETTINGS_JSON")));
			row.put("enabled", rs.getBoolean("ENABLED"));
			row.put("status", rs.getString("RUN_STATUS"));
			row.put("startedAt", CollaborationDbUtils.getTimestamp(rs, "STARTED_AT"));
			row.put("finishedAt", CollaborationDbUtils.getTimestamp(rs, "FINISHED_AT"));
			row.put("lastSuccessAt", CollaborationDbUtils.getTimestamp(rs, "LAST_SUCCESS_AT"));
			row.put("roomId", rs.getString("ROOM_ID"));
			row.put("runId", rs.getString("RUN_ID"));
			row.put("summary", CollaborationDbUtils.getString(rs, "SUMMARY"));
			row.put("error", CollaborationDbUtils.getString(rs, "ERROR"));
			return row;
		}, owner.getValue0(), owner.getValue1());
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> settings(Map<String, Object> row) {
		return (Map<String, Object>) row.get("settings");
	}

	private static Map<String, Object> lastRun(Map<String, Object> row) {
		Map<String, Object> last = new LinkedHashMap<>();
		for (String field : List.of("status", "startedAt", "finishedAt", "roomId", "summary", "error")) {
			last.put(field, row.get(field));
		}
		return last;
	}

	/**
	 * Records a finished automation run from its durable state: the brief node's text and room, and whether the
	 * whole run, including the email, succeeded.
	 */
	private static String finishFromRun(Pair<String, String> owner, String runId, String started) {
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		Map<String, Object> brief = null;
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if ("brief".equals(node.get(AutomationConstants.NODE_ID)) && node.get(AutomationConstants.PARENT_NODE_ID) == null) {
				brief = node;
			}
		}
		Object text = brief == null || brief.get(AutomationConstants.OUTPUT_VALUE) == null ? null
				: AutomationRuntimeUtils.GSON.fromJson((String) brief.get(AutomationConstants.OUTPUT_VALUE), Object.class);
		if (!AutomationConstants.NODE_STATUS_SUCCESS.equals(brief == null ? null : brief.get(AutomationConstants.STATUS))
				|| !(text instanceof String summary) || summary.isBlank()) {
			finish(owner, "failed", null, FAILED, null, started);
			return "failed";
		}
		boolean sent = run != null && AutomationConstants.STATUS_SUCCESS.equals(run.get(AutomationConstants.STATUS));
		finish(owner, "completed", summary, sent ? null : "The brief was saved but the email could not be sent.",
				(String) brief.get(AutomationConstants.ROOM_ID), started);
		return "completed";
	}

	private static void finish(Pair<String, String> owner, String status, String summary, String error, String roomId,
			String started) {
		CollaborationDbUtils.update("UPDATE WORK_INBOX_ROUTINE SET RUN_STATUS = ?, SUMMARY = ?, ERROR = ?, ROOM_ID = ?, FINISHED_AT = ?"
				+ ("completed".equals(status) ? ", LAST_SUCCESS_AT = STARTED_AT" : "") + OWNED
				+ " AND STARTED_AT = ? AND RUN_STATUS = 'running'", status, summary, error, roomId, CollaborationDbUtils.now(),
				owner.getValue0(), owner.getValue1(), CollaborationDbUtils.toTimestamp(started, "startedAt"));
	}
}
