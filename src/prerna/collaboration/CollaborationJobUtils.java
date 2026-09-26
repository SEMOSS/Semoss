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

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;

// COLLAB_JOB: one running job per owner and kind, run on a small background pool on this server
public final class CollaborationJobUtils {

	private static final Logger classLogger = LogManager.getLogger(CollaborationJobUtils.class);

	public static final String RUNNING = "running";
	public static final String DONE = "done";
	public static final String FAILED = "failed";
	private static final String COLUMNS = "JOB_ID, KIND, STATUS, STEP, PROGRESS, PARAMS_JSON, COUNTS_JSON, ERROR, "
			+ "STARTED_AT, FINISHED_AT";

	// jobs alive in this JVM; a RUNNING row not in here was cut off by a restart
	private static final Set<String> ALIVE = ConcurrentHashMap.newKeySet();
	private static final ExecutorService POOL = Executors.newFixedThreadPool(2, r -> {
		Thread t = new Thread(r, "collaboration-job");
		t.setDaemon(true);
		return t;
	});

	private CollaborationJobUtils() {
	}

	/** Passed to the work so it can report where it is. */
	public static final class Job {
		private final String ownerId;
		private final String ownerType;
		private final String jobId;
		private final Map<String, Object> counts = new LinkedHashMap<>();

		private Job(String ownerId, String ownerType, String jobId) {
			this.ownerId = ownerId;
			this.ownerType = ownerType;
			this.jobId = jobId;
		}

		public String ownerId() {
			return ownerId;
		}

		public String ownerType() {
			return ownerType;
		}

		public void step(String step, int progress) {
			CollaborationDbUtils.update("UPDATE COLLAB_JOB SET STEP = ?, PROGRESS = ?, COUNTS_JSON = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND JOB_ID = ?", step, Math.max(0, Math.min(100, progress)),
					CollaborationDbUtils.toJson(counts), ownerId, ownerType, jobId);
		}

		public void count(String key, Object value) {
			counts.put(key, value);
		}
	}

	@FunctionalInterface
	public interface Work {
		void run(Job job) throws Exception;
	}

	// the running job of this kind, or a new one started with work
	public static Map<String, Object> start(String ownerId, String ownerType, String kind, Map<String, Object> params,
			Work work) {
		synchronized (CollaborationDbUtils.ownerLock("job", ownerId, ownerType)) {
			Map<String, Object> latest = latest(ownerId, ownerType, kind);
			if (latest != null && RUNNING.equals(latest.get("status"))) {
				return latest;
			}
			String jobId = UUID.randomUUID().toString();
			CollaborationDbUtils.update("INSERT INTO COLLAB_JOB (OWNER_ID, OWNER_TYPE, JOB_ID, KIND, STATUS, STEP, "
					+ "PROGRESS, PARAMS_JSON, STARTED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", ownerId, ownerType, jobId,
					kind, RUNNING, "queued", 0, CollaborationDbUtils.toJson(params), CollaborationDbUtils.now());
			ALIVE.add(jobId);
			Job job = new Job(ownerId, ownerType, jobId);
			POOL.submit(() -> {
				try {
					work.run(job);
					finish(job, DONE, null);
				} catch (Throwable e) {
					classLogger.warn("Collaboration job {} ({}) failed", jobId, kind, e);
					finish(job, FAILED, e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
				} finally {
					ALIVE.remove(jobId);
				}
			});
			return get(ownerId, ownerType, jobId);
		}
	}

	public static Map<String, Object> latest(User user, String kind) {
		var owner = CollaborationDbUtils.ownerOf(user);
		return latest(owner.getValue0(), owner.getValue1(), kind);
	}

	// newest job of the kind (any kind when null), with a restart-orphaned RUNNING row marked failed
	public static Map<String, Object> latest(String ownerId, String ownerType, String kind) {
		String sql = "SELECT " + COLUMNS + " FROM COLLAB_JOB WHERE OWNER_ID = ? AND OWNER_TYPE = ?"
				+ (kind == null ? "" : " AND KIND = ?") + " ORDER BY STARTED_AT DESC";
		Object[] params = kind == null ? new Object[] { ownerId, ownerType } : new Object[] { ownerId, ownerType, kind };
		Map<String, Object> job = CollaborationDbUtils.queryOne(CollaborationDbUtils.page(sql, 1, 0),
				CollaborationJobUtils::map, params);
		if (job != null && RUNNING.equals(job.get("status")) && !ALIVE.contains(job.get("id"))) {
			CollaborationDbUtils.update("UPDATE COLLAB_JOB SET STATUS = ?, ERROR = ?, FINISHED_AT = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND JOB_ID = ? AND STATUS = ?", FAILED,
					"Stopped by a server restart; start it again", CollaborationDbUtils.now(), ownerId, ownerType,
					job.get("id"), RUNNING);
			return get(ownerId, ownerType, (String) job.get("id"));
		}
		return job;
	}

	static Map<String, Object> get(String ownerId, String ownerType, String jobId) {
		return CollaborationDbUtils.queryOne("SELECT " + COLUMNS + " FROM COLLAB_JOB WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND JOB_ID = ?", CollaborationJobUtils::map, ownerId, ownerType, jobId);
	}

	private static void finish(Job job, String status, String error) {
		Timestamp now = CollaborationDbUtils.now();
		CollaborationDbUtils.update("UPDATE COLLAB_JOB SET STATUS = ?, STEP = ?, PROGRESS = ?, COUNTS_JSON = ?, "
				+ "ERROR = ?, FINISHED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND JOB_ID = ?", status,
				DONE.equals(status) ? DONE : "stopped", DONE.equals(status) ? 100 : null,
				CollaborationDbUtils.toJson(job.counts), error, now, job.ownerId, job.ownerType, job.jobId);
	}

	private static Map<String, Object> map(java.sql.ResultSet rs) throws java.sql.SQLException {
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("id", rs.getString("JOB_ID"));
		row.put("kind", rs.getString("KIND"));
		row.put("status", rs.getString("STATUS"));
		row.put("step", rs.getString("STEP"));
		row.put("progress", CollaborationDbUtils.getInteger(rs, "PROGRESS"));
		row.put("params", CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "PARAMS_JSON")));
		row.put("counts", CollaborationDbUtils.parseMap(CollaborationDbUtils.getString(rs, "COUNTS_JSON")));
		row.put("error", CollaborationDbUtils.getString(rs, "ERROR"));
		row.put("startedAt", CollaborationDbUtils.getTimestamp(rs, "STARTED_AT"));
		row.put("finishedAt", CollaborationDbUtils.getTimestamp(rs, "FINISHED_AT"));
		return row;
	}
}
