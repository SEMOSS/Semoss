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
package prerna.reactor.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.scheduler.SchedulerDatabaseUtility.SchedulerAuditStats;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SchedulerStatsReactor extends AbstractReactor {

	public static final String WINDOW = "window";
	private static final String DEFAULT_WINDOW = "24h";

	// Quartz trigger states (see org.quartz.impl.jdbcjobstore.Constants)
	private static final String STATE_WAITING = "WAITING";
	private static final String STATE_ACQUIRED = "ACQUIRED";
	private static final String STATE_EXECUTING = "EXECUTING";
	private static final String STATE_PAUSED = "PAUSED";
	private static final String STATE_PAUSED_BLOCKED = "PAUSED_BLOCKED";

	public SchedulerStatsReactor() {
		this.keysToGet = new String[] { WINDOW };
	}

	@Override
	public NounMetadata execute() {
		if (Utility.schedulerForceDisable()) {
			throw new IllegalArgumentException("Scheduler is not enabled");
		}
		organizeKeys();

		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be an admin to view scheduler stats");
		}

		Duration window = parseWindow(this.keyValue.get(WINDOW));
		Instant now = Instant.now();
		Instant windowStart = now.minus(window);

		SchedulerAuditStats audit = SchedulerDatabaseUtility.computeSchedulerAuditStats(windowStart);
		Map<String, Long> triggerStateCounts = SchedulerDatabaseUtility.getTriggerStateCounts();
		long overdueJobs = SchedulerDatabaseUtility.getOverdueTriggerCount(now.toEpochMilli());
		Long nextRunMillis = SchedulerDatabaseUtility.getNextScheduledRunTime(now.toEpochMilli());

		long activeJobs = countOf(triggerStateCounts, STATE_WAITING) + countOf(triggerStateCounts, STATE_ACQUIRED)
				+ countOf(triggerStateCounts, STATE_EXECUTING);
		long pausedJobs = countOf(triggerStateCounts, STATE_PAUSED) + countOf(triggerStateCounts, STATE_PAUSED_BLOCKED);

		double successRate = audit.totalRuns() == 0 ? 1.0
				: (audit.totalRuns() - audit.failures()) / (double) audit.totalRuns();
		successRate = Math.round(successRate * 1000.0) / 1000.0;

		Map<String, Object> stats = new LinkedHashMap<>();
		stats.put("totalRuns", audit.totalRuns());
		stats.put("failures", audit.failures());
		stats.put("successRate", successRate);
		stats.put("avgDurationMs", audit.avgDurationMs());
		stats.put("p95DurationMs", audit.p95DurationMs());
		stats.put("activeJobs", activeJobs);
		stats.put("pausedJobs", pausedJobs);
		stats.put("overdueJobs", overdueJobs);
		stats.put("nextRunAt", nextRunMillis == null ? null
				: DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(nextRunMillis)));

		if (audit.worstConsecutiveFailures() > 0 && audit.worstJobId() != null) {
			Map<String, Object> worst = new LinkedHashMap<>();
			worst.put("jobId", audit.worstJobId());
			worst.put("name", audit.worstJobName());
			worst.put("consecutiveFailures", audit.worstConsecutiveFailures());
			stats.put("worstJob", worst);
		} else {
			stats.put("worstJob", null);
		}

		return new NounMetadata(stats, PixelDataType.MAP);
	}

	static Duration parseWindow(String raw) {
		String window = (raw == null || raw.trim().isEmpty()) ? DEFAULT_WINDOW : raw.trim().toLowerCase();
		int len = window.length();
		if (len < 2) {
			throw new IllegalArgumentException(
					"Invalid window '" + raw + "', expected <number><unit> with unit in s/m/h/d (e.g. 24h)");
		}
		char unit = window.charAt(len - 1);
		long value;
		try {
			value = Long.parseLong(window.substring(0, len - 1));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(
					"Invalid window '" + raw + "', expected <number><unit> with unit in s/m/h/d (e.g. 24h)");
		}
		if (value <= 0) {
			throw new IllegalArgumentException("Window must be positive: " + raw);
		}
		switch (unit) {
		case 's':
			return Duration.ofSeconds(value);
		case 'm':
			return Duration.ofMinutes(value);
		case 'h':
			return Duration.ofHours(value);
		case 'd':
			return Duration.ofDays(value);
		default:
			throw new IllegalArgumentException(
					"Unknown window unit '" + unit + "' in '" + raw + "', expected one of s/m/h/d");
		}
	}

	private static long countOf(Map<String, Long> counts, String state) {
		Long v = counts.get(state);
		return v == null ? 0L : v;
	}

	@Override
	public String getReactorDescription() {
		return """
				Returns a snapshot of scheduler health: execution counts, failure count, \
				success rate, average and p95 duration over the given window, plus current \
				trigger state (active, paused, overdue, next run time) and the job with \
				the longest current run of consecutive failures within the window.""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (WINDOW.equals(key)) {
			return """
					Time window applied to execution-derived stats (totalRuns, failures, \
					successRate, durations, worstJob). Format: <number><unit> with unit in \
					s (seconds), m (minutes), h (hours), or d (days). Defaults to '24h'. \
					Trigger-state stats (active/paused/overdue/nextRunAt) reflect current state \
					and are not window-filtered.""";
		}
		return super.getDescriptionForKey(key);
	}
}
