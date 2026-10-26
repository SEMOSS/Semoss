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
import java.util.ArrayList;
import java.util.Arrays;
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

	// why a run failed, so Routines can name the fix
	static final String CAUSE_CONNECTION = "connection";
	static final String CAUSE_MODEL = "model";
	static final String CAUSE_AGENT = "agent";
	static final String CAUSE_DELIVERY = "delivery";
	static final String CAUSE_INTERRUPTED = "interrupted";

	// how long Run now waits for the job to mark its run: 20 x 250 ms
	private static final int RUN_NOW_WAIT_STEPS = 20;
	private static final long RUN_NOW_WAIT_STEP_MS = 250;

	/** A run that cannot start; its message is ours, so it is safe to show the owner. */
	private static final class RoutineFailure extends IllegalArgumentException {
		private static final long serialVersionUID = 1L;
		private final String failureCause;

		RoutineFailure(String failureCause, String message) {
			super(message);
			this.failureCause = failureCause;
		}
	}

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
			BiFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> inputs,
			Agent agent) {
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

	/** Extra config for a routine's agent steps, from its row's settings and the owner; null for none. */
	@FunctionalInterface
	interface Agent {
		Map<String, Object> config(User user, Map<String, Object> settings);
	}

	/**
	 * One routine row: the kind, its owner, and for kinds with many routines per owner (a ROUTINE_ID column) which one.
	 * SQL params are the row's own values followed by the owner key.
	 */
	record Slot(Kind kind, Pair<String, String> owner, String routineId) {
		String where() {
			return OWNED + (routineId == null ? "" : " AND ROUTINE_ID = ?");
		}

		Object[] args(Object[] before, Object... after) {
			// Arrays.asList: summary, error and room id can be null
			List<Object> args = new ArrayList<>(Arrays.asList(before));
			args.add(owner.getValue0());
			args.add(owner.getValue1());
			if (routineId != null) {
				args.add(routineId);
			}
			args.addAll(Arrays.asList(after));
			return args.toArray();
		}

		String recipe() {
			return routineId == null ? kind.recipe() : kind.recipe().replace("{routineId}", routineId);
		}

		String automationId() {
			return routineId == null ? kind.automationId() : kind.automationId() + ":" + routineId;
		}
	}

	private static Object[] of(Object... values) {
		return values;
	}

	private WorkRoutine() { }

	static Map<String, Object> get(Kind kind, Insight insight) {
		return get(kind, insight, null);
	}

	static Map<String, Object> get(Kind kind, Insight insight, String routineId) {
		Slot slot = new Slot(kind, CollaborationDbUtils.ownerOf(insight.getUser()), routineId);
		settle(slot);
		return get(kind, insight.getUser(), routineId);
	}

	/** Records a finished or stale run on a row still marked running. */
	static void settle(Slot slot) {
		Kind kind = slot.kind();
		Map<String, Object> row = stored(slot);
		if (row != null && "running".equals(row.get("status"))) {
			String started = (String) row.get("startedAt");
			// Above every routine's own bounds: about 5 minutes of sync wait, two 10-minute agent waits, and the mail reads.
			boolean stale = started != null && Instant.parse(started).plusSeconds(2100).isBefore(Instant.now());
			String runId = (String) row.get("runId");
			Map<String, Object> run = runId == null ? null : AutomationRunStore.getRunDetail(runId);
			if (run != null && TERMINAL.contains(run.get(AutomationConstants.STATUS))) {
				finishFromRun(slot, runId, started);
			} else if (stale) {
				try {
					// Older agent-run ids have no automation row, and cancelling a missing row throws.
					if (run != null) {
						RunAutomationDefinitionReactor.cancelRun(runId);
					}
				} finally {
					finish(slot, "failed", null, "The " + kind.label() + " was interrupted. The next scheduled run will retry.", null, started,
							CAUSE_INTERRUPTED);
				}
			}
		}
	}

	static Map<String, Object> get(Kind kind, User user) {
		return get(kind, user, null);
	}

	static Map<String, Object> get(Kind kind, User user, String routineId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Slot slot = new Slot(kind, owner, routineId);
		Map<String, Object> row = stored(slot);
		Map<String, Object> result = new LinkedHashMap<>(row == null ? kind.defaults() : settings(kind, row));
		boolean enabled = row != null && Boolean.TRUE.equals(row.get("enabled"));
		boolean schedulerAvailable = schedulerAvailable();
		String mailboxId = mailboxId(owner);
		boolean mailboxAvailable = mailboxToken(mailboxId, user) != null;
		boolean pythonAvailable = PyUtils.pyEnabled();
		boolean assistantAvailable = pythonAvailable && model(user) != null;
		result.put("id", routineId == null ? kind.id() : routineId);
		result.put("configured", row != null);
		result.put("enabled", enabled);
		result.put("schedulerAvailable", schedulerAvailable);
		result.put("mailboxAvailable", mailboxAvailable);
		result.put("assistantAvailable", assistantAvailable);
		result.put("availabilityMessage", !schedulerAvailable ? "The SEMOSS scheduler is unavailable. Ask your administrator to enable it."
				: !mailboxAvailable ? mailboxUnavailable(mailboxId)
				: !pythonAvailable ? kind.pythonDisabled()
				: !assistantAvailable ? "Ask your administrator to configure a Collaboration text model and grant your primary account access." : null);
		result.put("nextRunAt", enabled && schedulerAvailable ? nextRun(slot) : null);
		result.put("lastRun", row == null || row.get("status") == null ? null : lastRun(row));
		result.put("flow", flow(serverDefinition(kind)));
		return result;
	}

	static Map<String, Object> save(Kind kind, User user, Map<String, Object> changes) {
		return save(kind, user, null, changes);
	}

	/** Saves one routine; a kind with many routines per owner names it by routineId, and a new id adds the row. */
	static Map<String, Object> save(Kind kind, User user, String routineId, Map<String, Object> changes) {
		Map<String, Object> config = validate(kind, changes);
		boolean enabled = (Boolean) config.remove("enabled");
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Slot slot = new Slot(kind, owner, routineId);
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
							+ slot.where(), slot.args(of(CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now())));
					if (changed == 0) {
						if (routineId == null) {
							CollaborationDbUtils.update(conn, "INSERT INTO " + kind.table() + " (OWNER_ID, OWNER_TYPE, SETTINGS_JSON, ENABLED, UPDATED_AT) VALUES (?, ?, ?, ?, ?)",
									owner.getValue0(), owner.getValue1(), CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now());
						} else {
							CollaborationDbUtils.update(conn, "INSERT INTO " + kind.table() + " (OWNER_ID, OWNER_TYPE, ROUTINE_ID, SETTINGS_JSON, ENABLED, UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?)",
									owner.getValue0(), owner.getValue1(), routineId, CollaborationDbUtils.toJson(config), false, CollaborationDbUtils.now());
						}
					}
					if (enabled) {
						MicrosoftGraphCredentialStore.save(mailboxToken);
						schedule(slot, config);
						CollaborationDbUtils.update(conn, "UPDATE " + kind.table() + " SET ENABLED = ?" + slot.where(), slot.args(of(true)));
					} else {
						pause(slot);
					}
				});
			} catch (RuntimeException e) {
				// A scheduler write and the Collaboration transaction cannot commit together; leave consent revoked.
				try {
					CollaborationDbUtils.update("UPDATE " + kind.table() + " SET ENABLED = ?" + slot.where(), slot.args(of(false)));
				} catch (RuntimeException cleanup) {
					e.addSuppressed(cleanup);
				} finally {
					pause(slot);
				}
				throw e;
			}
		}
		return get(kind, user, routineId);
	}

	/** Starts a run now on the scheduler, as a scheduled run would, so it runs as the owner off this request. */
	static void runNow(Kind kind, User user, String routineId) {
		Slot slot = new Slot(kind, CollaborationDbUtils.ownerOf(user), routineId);
		Map<String, Object> row = stored(slot);
		if (row == null) {
			throw new IllegalArgumentException("Routine not found");
		}
		if (!Boolean.TRUE.equals(row.get("enabled"))) {
			throw new IllegalArgumentException("Turn the routine on before running it");
		}
		if ("running".equals(row.get("status"))) {
			throw new IllegalArgumentException("The " + kind.label() + " is already running");
		}
		if (!schedulerAvailable()) {
			throw new IllegalArgumentException("The SEMOSS scheduler is unavailable");
		}
		try {
			SchedulerFactorySingleton.getInstance().getScheduler().triggerJob(jobKey(slot));
		} catch (SchedulerException e) {
			throw new IllegalStateException("Could not start the " + kind.label(), e);
		}
		// the job marks the run on its own thread; wait briefly so the caller's next read shows it running
		Object before = row.get("startedAt");
		for (int i = 0; i < RUN_NOW_WAIT_STEPS; i++) {
			Map<String, Object> now = stored(slot);
			if (now == null || ("running".equals(now.get("status")) && !Objects.equals(before, now.get("startedAt")))) {
				return;
			}
			try {
				Thread.sleep(RUN_NOW_WAIT_STEP_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	/** Deletes one routine of a many-per-owner kind, with its schedule; its past runs stay in the scheduler history. */
	static void delete(Kind kind, User user, String routineId) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Slot slot = new Slot(kind, owner, Objects.requireNonNull(routineId, "routineId"));
		synchronized (CollaborationDbUtils.ownerLock(kind.id(), owner.getValue0(), owner.getValue1())) {
			if (stored(slot) == null) {
				throw new IllegalArgumentException("Routine not found");
			}
			if (schedulerAvailable()) {
				try {
					Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
					JobKey key = jobKey(slot);
					scheduler.deleteJob(key);
					SchedulerDatabaseUtility.removeFromJobRecipesTable(key.getName(), GROUP);
				} catch (SchedulerException e) {
					throw new IllegalStateException("Could not remove the " + kind.label() + " schedule", e);
				}
			}
			CollaborationDbUtils.update("DELETE FROM " + kind.table() + slot.where(), slot.args(of()));
		}
	}

	static Map<String, Object> run(Kind kind, Insight insight) {
		return run(kind, insight, null, false);
	}

	/** Runs one routine; now runs it on a day off its schedule too (Run now). */
	static Map<String, Object> run(Kind kind, Insight insight, String routineId, boolean now) {
		User user = insight.getUser();
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Slot slot = new Slot(kind, owner, routineId);
		Map<String, Object> row = stored(slot);
		if (row == null || !Boolean.TRUE.equals(row.get("enabled"))) {
			return Map.of("status", "skipped");
		}
		Map<String, Object> config = settings(kind, row);
		LocalDate today = LocalDate.now(ZoneId.of((String) config.get("timezone")));
		if (!now && !((List<?>) config.get("days")).contains(WEEKDAYS.get(today.getDayOfWeek().getValue() - 1))) {
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
				+ "FINISHED_AT = NULL, ROOM_ID = NULL, RUN_ID = NULL, SUMMARY = NULL, ERROR = NULL, ERROR_CAUSE = NULL" + slot.where()
				// a run already going keeps the row; Run now while it runs is skipped
				+ " AND ENABLED = ?" + (now ? " AND (RUN_STATUS IS NULL OR RUN_STATUS <> 'running')" : ""),
				slot.args(of(today.toString(), "running", startedAt), true));
		if (claimed == 0) {
			return Map.of("status", "skipped");
		}
		AccessToken previousToken = user.getAccessToken(AuthProvider.MICROSOFT);
		AuthProvider previousPrimary = user.getPrimaryLogin();
		String mailboxId = null;
		String attachedToken = null;
		try {
			if (SecurityQueryUtils.getUnlockedUser(owner.getValue0(), owner.getValue1()) == null) {
				throw new RoutineFailure(CAUSE_CONNECTION, "The routine owner account is no longer active.");
			}
			mailboxId = mailboxId(owner);
			if (!MicrosoftGraphCredentialStore.isRefreshable(MicrosoftGraphCredentialStore.get(mailboxId))) {
				// Older opted-in routines may still hold their credentials in the subscription registry.
				AccessToken mailboxToken = mailboxToken(mailboxId, null);
				if (mailboxToken == null) {
					throw new RoutineFailure(CAUSE_CONNECTION, mailboxUnavailable(mailboxId));
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
				throw new RoutineFailure(CAUSE_CONNECTION, mailboxUnavailable(mailboxId));
			}
			String engine = model(user);
			if (engine == null) {
				throw new RoutineFailure(CAUSE_MODEL, "A Collaboration text model is required for " + kind.label() + "s.");
			}
			// a kind's agent step can need the routine's own id, such as for its reference documents
			Map<String, Object> agentSettings = new LinkedHashMap<>(config);
			if (routineId != null) {
				agentSettings.put("routineId", routineId);
			}
			AutomationDefinitionService.DefinitionFiles files = definition(kind, engine,
					kind.agent() == null ? null : kind.agent().config(user, agentSettings));
			Map<String, Object> inputs = new LinkedHashMap<>();
			inputs.put("owner_email", recipient);
			inputs.put("timezone", config.get("timezone"));
			inputs.putAll(kind.inputs().apply(row, config));
			String runId = UUID.randomUUID().toString();
			// Attach before running; no row means the routine was paused or a newer run took the day.
			int attached = CollaborationDbUtils.update("UPDATE " + kind.table() + " SET RUN_ID = ?" + slot.where()
					+ " AND STARTED_AT = ? AND ENABLED = ? AND RUN_STATUS = 'running'", slot.args(of(runId), startedAt, true));
			if (attached == 0) {
				finish(slot, "failed", null, "Routine was paused before the " + kind.noun() + " started.", null, started,
						CAUSE_INTERRUPTED);
				return Map.of("status", "skipped");
			}
			RunAutomationDefinitionReactor.runDefinition(insight, runId, slot.automationId(), files, inputs,
					now ? AutomationConstants.TRIGGER_MANUAL : AutomationConstants.TRIGGER_SCHEDULED, null);
			return Map.of("status", finishFromRun(slot, runId, started));
		} catch (Exception e) {
			boolean interrupted = Thread.interrupted() || e instanceof InterruptedException;
			try {
				// Provider errors can contain message bodies or credentials; expose only an action message or our own.
				String cause = e instanceof RoutineFailure failure ? failure.failureCause : null;
				finish(slot, "failed", null, cause == null ? kind.failed() : e.getMessage(), null, started, cause);
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
		return definition(kind, engineId, null);
	}

	/** The same, with extra config (agent, skills, instructions) set on every agent step. */
	static AutomationDefinitionService.DefinitionFiles definition(Kind kind, String engineId, Map<String, Object> agentConfig) {
		JsonObject definition = serverDefinition(kind);
		for (JsonElement element : definition.getAsJsonObject(AutomationConstants.DOC_GRAPH)
				.getAsJsonArray(AutomationConstants.DOC_NODES)) {
			JsonObject node = element.getAsJsonObject();
			String nodeType = node.get(AutomationConstants.NODE_FIELD_TYPE).getAsString();
			if (kind == WorkInboxRoutine.KIND && AutomationConstants.NODE_START.equals(nodeType)) {
				for (JsonElement global : node.getAsJsonObject(AutomationConstants.NODE_FIELD_CONFIG)
						.getAsJsonArray(AutomationConstants.CONFIG_GLOBALS)) {
					JsonObject value = global.getAsJsonObject();
					if ("brief_model".equals(value.get("name").getAsString())) {
						value.addProperty(AutomationConstants.CONFIG_DEFAULT_VALUE, engineId);
					}
				}
			}
			if (AutomationConstants.NODE_AGENT_RUN.equals(nodeType)) {
				JsonObject config = node.getAsJsonObject(AutomationConstants.NODE_FIELD_CONFIG);
				config.addProperty(AutomationConstants.CONFIG_ENGINE_ID, engineId);
				if (agentConfig != null) {
					agentConfig.forEach((key, value) -> config.add(key, AutomationRuntimeUtils.GSON.toJsonTree(value)));
				}
			}
		}
		return AutomationDefinitionService.fromInline(AutomationRuntimeUtils.GSON.toJson(definition), null);
	}

	private static JsonObject serverDefinition(Kind kind) {
		JsonObject definition;
		try (InputStream in = WorkRoutine.class.getResourceAsStream(kind.definition())) {
			if (in == null) {
				throw new IllegalStateException("The " + kind.label() + " definition is missing from this server build");
			}
			definition = JsonParser.parseString(new String(in.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject();
		} catch (IOException e) {
			throw new IllegalStateException("Could not read the " + kind.label() + " definition", e);
		}
		return definition;
	}

	/** Allowlist the graph's display fields; never return source, config, or run data. */
	static Map<String, Object> flow(JsonObject definition) {
		JsonObject graph = definition.getAsJsonObject(AutomationConstants.DOC_GRAPH);
		List<Map<String, Object>> nodes = new ArrayList<>();
		for (JsonElement element : graph.getAsJsonArray(AutomationConstants.DOC_NODES)) {
			JsonObject node = element.getAsJsonObject();
			Map<String, Object> preview = new LinkedHashMap<>();
			for (String field : List.of("id", "type", "label")) {
				preview.put(field, node.get(field).getAsString());
			}
			if (node.get("position") instanceof JsonObject position) {
				JsonElement x = position.get("x");
				JsonElement y = position.get("y");
				if (x != null && x.isJsonPrimitive() && x.getAsJsonPrimitive().isNumber()
						&& y != null && y.isJsonPrimitive() && y.getAsJsonPrimitive().isNumber()
						&& Double.isFinite(x.getAsDouble()) && Double.isFinite(y.getAsDouble())) {
					preview.put("position", Map.of("x", x.getAsDouble(), "y", y.getAsDouble()));
				}
			}
			nodes.add(preview);
		}
		List<Map<String, Object>> edges = new ArrayList<>();
		for (JsonElement element : graph.getAsJsonArray(AutomationConstants.DOC_EDGES)) {
			JsonObject edge = element.getAsJsonObject();
			Map<String, Object> preview = new LinkedHashMap<>();
			for (String field : List.of("source", "target", "sourcePort", "targetPort")) {
				preview.put(field, edge.get(field).getAsString());
			}
			edges.add(preview);
		}
		return Map.of("nodes", nodes, "edges", edges);
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

	private static void schedule(Slot slot, Map<String, Object> config) {
		Kind kind = slot.kind();
		Pair<String, String> owner = slot.owner();
		// a named routine shows by its own name in the scheduler
		String jobName = config.get("name") instanceof String name && !name.isBlank() ? name : kind.jobName();
		try {
			Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
			SchedulerDatabaseUtility.startScheduler(scheduler);
			JobKey key = jobKey(slot);
			boolean existing = SchedulerDatabaseUtility.existsInJobRecipesTable(key.getName(), GROUP);
			String expression = cron(config);
			TimeZone zone = TimeZone.getTimeZone(ZoneId.of((String) config.get("timezone")));
			String provider = AuthProvider.getProviderFromString(owner.getValue1()).name() + ":" + owner.getValue0();
			JobDetail job = JobBuilder.newJob(RunPixelJobFromDB.class).withIdentity(key).storeDurably()
					.usingJobData(ScheduleJobReactor.getJobDataMap(key.getName(), jobName, GROUP, expression, zone,
							slot.recipe(), "", false, null, provider)).build();
			Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey(slot)).forJob(key)
					.withSchedule(CronScheduleBuilder.cronSchedule(expression).inTimeZone(zone).withMisfireHandlingInstructionDoNothing()).build();
			scheduler.addJob(job, true);
			if (scheduler.checkExists(triggerKey(slot))) {
				scheduler.rescheduleJob(triggerKey(slot), trigger);
			} else {
				scheduler.scheduleJob(trigger);
			}
			scheduler.resumeJob(key);
			boolean saved = existing
					? SchedulerDatabaseUtility.updateJobRecipesTable(owner.getValue0(), key.getName(), jobName, GROUP,
							expression, zone, slot.recipe(), "", "Routine", false, null, jobName, GROUP, List.of("collaboration", kind.id()))
					: SchedulerDatabaseUtility.insertIntoJobRecipesTable(owner.getValue0(), key.getName(), jobName, GROUP,
							expression, zone, slot.recipe(), "", "Routine", false, null, List.of("collaboration", kind.id()));
			if (!saved) {
				scheduler.pauseJob(key);
				throw new IllegalStateException("Could not save the " + kind.label() + " schedule; the routine remains paused");
			}
		} catch (SchedulerException e) {
			throw new IllegalStateException("Could not schedule the " + kind.label() + "; the routine remains paused", e);
		}
	}

	private static void pause(Slot slot) {
		if (!schedulerAvailable()) {
			return;
		}
		try {
			SchedulerFactorySingleton.getInstance().getScheduler().pauseJob(jobKey(slot));
		} catch (SchedulerException e) {
			// Opt-in was already revoked in the database, so a stale trigger cannot read mail.
		}
	}

	private static String nextRun(Slot slot) {
		try {
			Trigger trigger = SchedulerFactorySingleton.getInstance().getScheduler().getTrigger(triggerKey(slot));
			Date next = trigger == null ? null : trigger.getNextFireTime();
			return next == null ? null : next.toInstant().toString();
		} catch (SchedulerException e) {
			return null;
		}
	}

	private static JobKey jobKey(Slot slot) {
		Pair<String, String> owner = slot.owner();
		String id = slot.routineId() == null
				? CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), slot.kind().id())
				: CollaborationDbUtils.deterministicId(owner.getValue0(), owner.getValue1(), slot.kind().id(), slot.routineId());
		return JobKey.jobKey(id, GROUP);
	}

	private static TriggerKey triggerKey(Slot slot) {
		return TriggerKey.triggerKey(jobKey(slot).getName() + "Trigger", GROUP + "TriggerGroup");
	}

	static Map<String, Object> stored(Kind kind, Pair<String, String> owner) {
		return stored(new Slot(kind, owner, null));
	}

	/** The routine ids of a many-per-owner kind for one owner, oldest first. */
	static List<String> routineIds(Kind kind, Pair<String, String> owner) {
		return CollaborationDbUtils.query("SELECT ROUTINE_ID FROM " + kind.table() + OWNED + " ORDER BY UPDATED_AT",
				rs -> rs.getString(1), owner.getValue0(), owner.getValue1());
	}

	static Map<String, Object> stored(Slot slot) {
		return CollaborationDbUtils.queryOne("SELECT * FROM " + slot.kind().table() + slot.where(), rs -> {
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
			row.put("errorCause", rs.getString("ERROR_CAUSE"));
			return row;
		}, slot.args(of()));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> settings(Kind kind, Map<String, Object> row) {
		// Older rows inherit new defaults and no longer expose retired settings.
		Map<String, Object> config = new LinkedHashMap<>(kind.defaults());
		config.putAll((Map<String, Object>) row.get("settings"));
		config.keySet().retainAll(kind.defaults().keySet());
		return config;
	}

	private static Map<String, Object> lastRun(Map<String, Object> row) {
		Map<String, Object> last = new LinkedHashMap<>();
		for (String field : List.of("status", "startedAt", "finishedAt", "roomId", "summary", "error", "errorCause")) {
			last.put(field, row.get(field));
		}
		// a running run's room exists from its start, so Routines can open it to watch
		if ("running".equals(row.get("status")) && last.get("roomId") == null && row.get("runId") instanceof String runId) {
			for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
				if (node.get(AutomationConstants.PARENT_NODE_ID) == null
						&& node.get(AutomationConstants.ROOM_ID) instanceof String roomId && !roomId.isBlank()) {
					last.put("roomId", roomId);
					break;
				}
			}
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
	private static String finishFromRun(Slot slot, String runId, String started) {
		Kind kind = slot.kind();
		Map<String, Object> run = AutomationRunStore.getRunDetail(runId);
		Map<String, Object> result = null;
		for (Map<String, Object> node : AutomationRunStore.getNodeOutputsForRun(runId)) {
			if (kind.outputNode().equals(node.get(AutomationConstants.NODE_ID)) && node.get(AutomationConstants.PARENT_NODE_ID) == null) {
				result = node;
			}
		}
		Object text = result == null || result.get(AutomationConstants.OUTPUT_VALUE) == null ? null
				: AutomationRuntimeUtils.GSON.fromJson((String) result.get(AutomationConstants.OUTPUT_VALUE), Object.class);
		boolean succeeded = AutomationConstants.NODE_STATUS_SUCCESS.equals(result == null ? null : result.get(AutomationConstants.STATUS));
		if (!succeeded || !(text instanceof String summary) || summary.isBlank()) {
			// an agent step that finished with no text is the model, not the connection
			String error = succeeded && kind.agent() != null
					? "The assistant finished without an answer. Run it again; if it keeps happening, make the task smaller."
					: kind.failed();
			finish(slot, "failed", null, error, result == null ? null : (String) result.get(AutomationConstants.ROOM_ID), started,
					CAUSE_AGENT);
			return "failed";
		}
		boolean sent = run != null && AutomationConstants.STATUS_SUCCESS.equals(run.get(AutomationConstants.STATUS));
		finish(slot, "completed", summary, sent ? null : "The " + kind.noun() + " was saved but could not be delivered.",
				(String) result.get(AutomationConstants.ROOM_ID), started, sent ? null : CAUSE_DELIVERY);
		return "completed";
	}

	private static void finish(Slot slot, String status, String summary, String error, String roomId, String started,
			String cause) {
		CollaborationDbUtils.update("UPDATE " + slot.kind().table()
				+ " SET RUN_STATUS = ?, SUMMARY = ?, ERROR = ?, ERROR_CAUSE = ?, ROOM_ID = ?, FINISHED_AT = ?"
				+ ("completed".equals(status) ? ", LAST_SUCCESS_AT = STARTED_AT" : "") + slot.where()
				+ " AND STARTED_AT = ? AND RUN_STATUS = 'running'",
				slot.args(of(status, summary, error, cause, roomId, CollaborationDbUtils.now()),
						CollaborationDbUtils.toTimestamp(started, "startedAt")));
	}
}
