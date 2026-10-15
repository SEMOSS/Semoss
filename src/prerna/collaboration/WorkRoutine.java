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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
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

/**
 * One private, opted-in routine per owner and kind, driven by the existing SEMOSS scheduler: consent, schedule,
 * stored Microsoft credential, run claim, and result are shared; each routine is a {@link Kind}.
 */
final class WorkRoutine {
	static final List<String> WEEKDAYS = List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN");
	private static final String GROUP = "SYSTEM__COLLABORATION_ROUTINES";
	private static final String OWNED = " WHERE OWNER_ID = ? AND OWNER_TYPE = ?";
	private static final Set<String> TERMINAL = Set.of(AutomationConstants.STATUS_SUCCESS, AutomationConstants.STATUS_FAILED,
			AutomationConstants.STATUS_CANCELLED, AutomationConstants.STATUS_INTERRUPTED);

	/**
	 * What one routine adds: its table (same columns as WORK_INBOX_ROUTINE), scheduler recipe, server-held automation
	 * and the node whose text is the result, the settings beyond time, days and timezone, and the automation inputs
	 * beyond owner_email and timezone. label and noun name it in messages, such as "inbox brief" and "brief".
	 */
	record Kind(String id, String table, String recipe, String jobName, String definition, String outputNode,
			String label, String noun, Map<String, Object> defaults,
			BiConsumer<Map<String, Object>, Map<String, Object>> settings,
			BiFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> inputs) {
		/** AUTOMATION_ID of the routine's runs in the scheduler database. */
		String automationId() {
			return "collaboration:" + id;
		}

		String failed() {
			return "The " + label + " failed. Check your Microsoft connection and Collaboration model, then retry on the next scheduled day.";
		}

		String pythonDisabled() {
			return "Python is disabled on this server; the " + label + " needs it.";
		}
	}

	private WorkRoutine() { }

	static Map<String, Object> get(Kind kind, Insight insight) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(insight.getUser());
		Map<String, Object> row = stored(kind, owner);
		if (row != null && "running".equals(row.get("status"))) {
			String started = (String) row.get("startedAt");
			// Above every routine's own bounds: about 5 minutes of sync wait, two 10-minute agent waits, and the mail reads.
			boolean stale = started != null && Instant.parse(started).plusSeconds(2100).isBefore(Instant.now());
			String runId = (String) row.get("runId");
			Map<String, Object> run = runId == null ? null : AutomationRunStore.getRunDetail(runId);
			if (run != null && TERMINAL.contains(run.get(AutomationConstants.STATUS))) {
				finishFromRun(kind, owner, runId, started);
			} else if (stale) {
				try {
					// Older agent-run ids have no automation row, and cancelling a missing row throws.
					if (run != null) {
						RunAutomationDefinitionReactor.cancelRun(runId);
					}
				} finally {
					finish(kind, owner, "failed", null, "The " + kind.label() + " was interrupted. The next scheduled run will retry.", null, started);
				}
			}
		}
		return get(kind, insight.getUser());
	}

	static Map<String, Object> get(Kind kind, User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> row = stored(kind, owner);
		Map<String, Object> result = new LinkedHashMap<>(row == null ? kind.defaults() : settings(row));
		boolean enabled = row != null && Boolean.TRUE.equals(row.get("enabled"));
		boolean schedulerAvailable = schedulerAvailable();
		String mailboxId = mailboxId(owner);
		boolean mailboxAvailable = mailboxToken(mailboxId, user) != null;
		boolean pythonAvailable = PyUtils.pyEnabled();
		boolean assistantAvailable = pythonAvailable && model(user) != null;
		result.put("id", kind.id());
		result.put("configured", row != null);
		result.put("enabled", enabled);
		result.put("schedulerAvailable", schedulerAvailable);
		result.put("mailboxAvailable", mailboxAvailable);
		result.put("assistantAvailable", assistantAvailable);
		result.put("availabilityMessage", !schedulerAvailable ? "The SEMOSS scheduler is unavailable. Ask your administrator to enable it."
				: !mailboxAvailable ? mailboxUnavailable(mailboxId)
				: !pythonAvailable ? kind.pythonDisabled()
				: !assistantAvailable ? "Ask your administrator to configure a Collaboration text model and grant your primary account access." : null);
		result.put("nextRunAt", enabled && schedulerAvailable ? nextRun(kind, owner) : null);
		result.put("lastRun", row == null || row.get("status") == null ? null : lastRun(row));
		return result;
	}

	static Map<String, Object> save(Kind kind, User user, Map<String, Object> changes) {
		Map<String, Object> config = validate(kind, changes);
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
				throw new IllegalArgumentException(kind.pythonDisabled());
			}
			if (model(user) == null) {
				throw new IllegalArgumentException("A Collaboration text model is required for " + kind.label() + "s");
			}
		}
		synchronized (CollaborationDbUtils.ownerLock(kind.id(), owner.getValue0(), owner.getValue1())) {
			try {
				// The row lock serializes schedule changes across servers; the executor claims this same row.
				CollaborationDbUtils.inTransaction(conn -> {
					int changed = CollaborationDbUtils.update(conn, "UPDATE " + kind.table() + " SET SETTINGS_JSON = ?, ENABLED = ?, UPDATED_AT = ?"
							+ OWNED, CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now(), owner.getValue0(), owner.getValue1());
					if (changed == 0) {
						CollaborationDbUtils.update(conn, "INSERT INTO " + kind.table() + " (OWNER_ID, OWNER_TYPE, SETTINGS_JSON, ENABLED, UPDATED_AT) VALUES (?, ?, ?, ?, ?)",
								owner.getValue0(), owner.getValue1(), CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now());
					}
					if (enabled) {
						MicrosoftGraphCredentialStore.save(mailboxToken);
						schedule(kind, owner, config);
						CollaborationDbUtils.update(conn, "UPDATE " + kind.table() + " SET ENABLED = ?" + OWNED,
								true, owner.getValue0(), owner.getValue1());
					} else {
						pause(kind, owner);
					}
				});
			} catch (RuntimeException e) {
				// A scheduler write and the Collaboration transaction cannot commit together; leave consent revoked.
				try {
					CollaborationDbUtils.update("UPDATE " + kind.table() + " SET ENABLED = ?" + OWNED,
							false, owner.getValue0(), owner.getValue1());
				} catch (RuntimeException cleanup) {
					e.addSuppressed(cleanup);
				} finally {
					pause(kind, owner);
				}
				throw e;
			}
		}
		return get(kind, user);
	}

	static Map<String, Object> run(Kind kind, Insight insight) {
		User user = insight.getUser();
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> row = stored(kind, owner);
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
		// Preserve an existing successful run's cutoff when upgrading an older routine row.
		// shortcut: once-per-day guard off for testing (no "AND (LAST_DATE IS NULL OR LAST_DATE <> ?)"); restore before release.
		int claimed = CollaborationDbUtils.update("UPDATE " + kind.table() + " SET LAST_SUCCESS_AT = CASE "
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
				throw new IllegalArgumentException("A Collaboration text model is required for " + kind.label() + "s");
			}
			AutomationDefinitionService.DefinitionFiles files = definition(kind, engine);
			Map<String, Object> inputs = new LinkedHashMap<>();
			inputs.put("owner_email", recipient);
			inputs.put("timezone", config.get("timezone"));
			inputs.putAll(kind.inputs().apply(row, config));
			String runId = UUID.randomUUID().toString();
			// Attach before running; no row means the routine was paused or a newer run took the day.
			int attached = CollaborationDbUtils.update("UPDATE " + kind.table() + " SET RUN_ID = ?" + OWNED
					+ " AND STARTED_AT = ? AND ENABLED = ? AND RUN_STATUS = 'running'", runId, owner.getValue0(), owner.getValue1(),
					startedAt, true);
			if (attached == 0) {
				finish(kind, owner, "failed", null, "Routine was paused before the " + kind.noun() + " started.", null, started);
				return Map.of("status", "skipped");
			}
			RunAutomationDefinitionReactor.runDefinition(insight, runId, kind.automationId(), files, inputs,
					AutomationConstants.TRIGGER_SCHEDULED, null);
			return Map.of("status", finishFromRun(kind, owner, runId, started));
		} catch (Exception e) {
			boolean interrupted = Thread.interrupted() || e instanceof InterruptedException;
			try {
				// Provider errors can contain message bodies or credentials; expose only an action message.
				finish(kind, owner, "failed", null, kind.failed(), null, started);
			} finally {
				// Interrupted socket I/O can close a shared database connection; restore after persistence.
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
			}
			throw new IllegalStateException("The " + kind.label() + " failed; check its status in Routines", e);
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

	static Map<String, Object> validate(Kind kind, Map<String, Object> input) {
		if (input == null || !(input.get("enabled") instanceof Boolean)) {
			throw new IllegalArgumentException("enabled must be true or false");
		}
		if (!input.keySet().stream().allMatch(key -> "enabled".equals(key) || kind.defaults().containsKey(key))) {
			throw new IllegalArgumentException("Unknown " + kind.label() + " setting");
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
		Map<String, Object> config = new LinkedHashMap<>();
		config.put("enabled", input.get("enabled"));
		config.put("time", time);
		config.put("timezone", timezone);
		config.put("days", WEEKDAYS.stream().filter(((List<?>) input.get("days"))::contains).toList());
		kind.settings().accept(input, config);
		return config;
	}

	static String cron(Map<String, Object> config) {
		LocalTime time = LocalTime.parse((String) config.get("time"));
		return "0 " + time.getMinute() + " " + time.getHour() + " ? * "
				+ String.join(",", ((List<?>) config.get("days")).stream().map(String::valueOf).toList());
	}

	/** The routine's server-held automation, with the owner's model filled in on its agent steps. */
	static AutomationDefinitionService.DefinitionFiles definition(Kind kind, String engineId) {
		JsonObject definition;
		try (InputStream in = WorkRoutine.class.getResourceAsStream(kind.definition())) {
			if (in == null) {
				throw new IllegalStateException("The " + kind.label() + " definition is missing from this server build");
			}
			definition = JsonParser.parseString(new String(in.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
		} catch (IOException e) {
			throw new IllegalStateException("Could not read the " + kind.label() + " definition", e);
		}
		for (JsonElement element : definition.getAsJsonObject(AutomationConstants.DOC_GRAPH)
				.getAsJsonArray(AutomationConstants.DOC_NODES)) {
			JsonObject node = element.getAsJsonObject();
			if (AutomationConstants.NODE_AGENT_RUN.equals(node.get(AutomationConstants.NODE_FIELD_TYPE).getAsString())) {
				node.getAsJsonObject(AutomationConstants.NODE_FIELD_CONFIG).addProperty(AutomationConstants.CONFIG_ENGINE_ID,
						engineId);
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

	private static void schedule(Kind kind, Pair<String, String> owner, Map<String, Object> config) {
		try {
			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
			SchedulerDatabaseUtility.startScheduler(scheduler);
			JobKey key = jobKey(kind, owner);
			boolean existing = SchedulerDatabaseUtility.existsInJobRecipesTable(key.getName(), GROUP);
			String expression = cron(config);
			TimeZone zone = TimeZone.getTimeZone(ZoneId.of((String) config.get("timezone")));
			String provider = AuthProvider.getProviderFromString(owner.getValue1()).name() + ":" + owner.getValue0();
			JobDetail job = JobBuilder.newJob(RunPixelJobFromDB.class).withIdentity(key).storeDurably()
					.usingJobData(ScheduleJobReactor.getJobDataMap(key.getName(), kind.jobName(), GROUP, expression, zone,
							kind.recipe(), "", false, null, provider)).build();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey(kind, owner)).forJob(key)
					.withSchedule(CronScheduleBuilder.cronSchedule(expression).inTimeZone(zone).withMisfireHandlingInstructionDoNothing()).build();
			scheduler.addJob(job, true);
			if (scheduler.checkExists(triggerKey(kind, owner))) {
				scheduler.rescheduleJob(triggerKey(kind, owner), trigger);
			} else {
				scheduler.scheduleJob(trigger);
			}
			scheduler.resumeJob(key);
			boolean saved = existing
					? SchedulerDatabaseUtility.updateJobRecipesTable(owner.getValue0(), key.getName(), kind.jobName(), GROUP,
							expression, zone, kind.recipe(), "", "Routine", false, null, kind.jobName(), GROUP, List.of("collaboration", kind.id()))
					: SchedulerDatabaseUtility.insertIntoJobRecipesTable(owner.getValue0(), key.getName(), kind.jobName(), GROUP,
							expression, zone, kind.recipe(), "", "Routine", false, null, List.of("collaboration", kind.id()));
			if (!saved) {
				scheduler.pauseJob(key);
				throw new IllegalStateException("Could not save the " + kind.label() + " schedule; the routine remains paused");
			}
		} catch (SchedulerException e) {
			throw new IllegalStateException("Could not schedule the " + kind.label() + "; the routine remains paused", e);
		}
	}

	private static void pause(Kind kind, Pair<String, String> owner) {
		if (!schedulerAvailable()) {
			return;
		}
		try {
			SchedulerFactorySingleton.getInstance().getScheduler().pauseJob(jobKey(kind, owner));
		} catch (SchedulerException e) {
			// Opt-in was already revoked in the database, so a stale trigger cannot read mail.
		}
	}

	private static String nextRun(Kind kind, Pair<String, String> owner) {
		try {
			Trigger trigger = SchedulerFactorySingleton.getInstance().getScheduler().getTrigger(triggerKey(kind, owner));
			Date next = trigger == null ? null : trigger.getNextFireTime();
			return next == null ? null : next.toInstant().toString();
		} catch (SchedulerException e) {
			return null;
		}
	}

	private static JobKey jobKey(Kind kind, Pair<String, String> owner) {
		return JobKey.jobKey(CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), kind.id()), GROUP);
	}

	private static TriggerKey triggerKey(Kind kind, Pair<String, String> owner) {
		return TriggerKey.triggerKey(jobKey(kind, owner).getName() + "Trigger", GROUP + "TriggerGroup");
	}

	static Map<String, Object> stored(Kind kind, Pair<String, String> owner) {
		return CollaborationDbUtils.queryOne("SELECT * FROM " + kind.table() + OWNED, rs -> {
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
		// The owner-scoped row selects the run; never accept a run id from the client.
		if ("completed".equals(row.get("status")) && row.get("runId") instanceof String runId) {
			for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
				if ("deliver".equals(node.get(AutomationConstants.NODE_ID))
						&& node.get(AutomationConstants.PARENT_NODE_ID) == null
						&& AutomationConstants.NODE_STATUS_SUCCESS.equals(node.get(AutomationConstants.STATUS))
						&& node.get(AutomationConstants.OUTPUT_VALUE) instanceof String output) {
					try {
						JsonElement value = JsonParser.parseString(output);
						JsonElement html = value.isJsonObject() ? value.getAsJsonObject().get("html") : null;
						if (html != null && html.isJsonPrimitive() && html.getAsJsonPrimitive().isString()
								&& !html.getAsString().isBlank()) {
							last.put("html", html.getAsString());
						}
					} catch (JsonParseException e) {
						// Older or unavailable delivery output leaves the saved Markdown as the fallback.
					}
				}
			}
		}
		return last;
	}

	/**
	 * Records a finished automation run from its durable state: the result node's text and room, and whether the
	 * whole run, including the email, succeeded.
	 */
	private static String finishFromRun(Kind kind, Pair<String, String> owner, String runId, String started) {
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		Map<String, Object> result = null;
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if (kind.outputNode().equals(node.get(AutomationConstants.NODE_ID)) && node.get(AutomationConstants.PARENT_NODE_ID) == null) {
				result = node;
			}
		}
		Object text = result == null || result.get(AutomationConstants.OUTPUT_VALUE) == null ? null
				: AutomationRuntimeUtils.GSON.fromJson((String) result.get(AutomationConstants.OUTPUT_VALUE), Object.class);
		if (!AutomationConstants.NODE_STATUS_SUCCESS.equals(result == null ? null : result.get(AutomationConstants.STATUS))
				|| !(text instanceof String summary) || summary.isBlank()) {
			finish(kind, owner, "failed", null, kind.failed(), null, started);
			return "failed";
		}
		boolean sent = run != null && AutomationConstants.STATUS_SUCCESS.equals(run.get(AutomationConstants.STATUS));
		finish(kind, owner, "completed", summary, sent ? null : "The " + kind.noun() + " was saved but the email could not be sent.",
				(String) result.get(AutomationConstants.ROOM_ID), started);
		return "completed";
	}

	private static void finish(Kind kind, Pair<String, String> owner, String status, String summary, String error,
			String roomId, String started) {
		CollaborationDbUtils.update("UPDATE " + kind.table() + " SET RUN_STATUS = ?, SUMMARY = ?, ERROR = ?, ROOM_ID = ?, FINISHED_AT = ?"
				+ ("completed".equals(status) ? ", LAST_SUCCESS_AT = STARTED_AT" : "") + OWNED
				+ " AND STARTED_AT = ? AND RUN_STATUS = 'running'", status, summary, error, roomId, CollaborationDbUtils.now(),
				owner.getValue0(), owner.getValue1(), CollaborationDbUtils.toTimestamp(started, "startedAt"));
	}
}
