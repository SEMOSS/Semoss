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
package prerna.usertracking;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.util.Utility;

/**
 * Records SQL or a query struct changing a database as one
 * {@link AuditEventType#DATABASE_WRITE} event per request in
 * {@code USER_AUDIT_EVENTS}. The event says who wrote to which database, how
 * many statements ran and what the request left in the database. The SQL text
 * is kept by the engine proxy in {@code AUDIT_LOGS}, not here, though the
 * database's error message for a failed write, which the event keeps, can quote
 * part of it, as H2's messages do. The event holds the SHA-256 of the SQL, so a
 * script file can be matched to its run, whenever the engine keeps its input
 * and output ({@link IEngine#keepInputOutput()}). A database always does; a
 * vector database built on a database engine follows its KEEP_INPUT_OUTPUT
 * setting.
 */
public final class DatabaseWriteAudit {

	private static final Logger classLogger = LogManager.getLogger(DatabaseWriteAudit.class);

	/** How the statements of a request were committed */
	public enum Transaction {
		/** Each statement committed on its own */
		PER_STATEMENT,
		/** Every statement ran in one transaction that committed once */
		ONE_TRANSACTION
	}

	/** What a request left in the database */
	public enum Outcome {
		/** Every statement's change was committed */
		COMMITTED,
		/**
		 * The statements before the failing one were committed and the rest did not run
		 */
		PARTIAL,
		/** No change from the request was kept */
		ROLLED_BACK,
		/**
		 * The commit or rollback did not report back, so the database has to be checked
		 * to know what was kept
		 */
		UNKNOWN
	}

	/**
	 * One request's write: the reactor that ran it, how many statements it had, how
	 * far it got and what it left in the database.
	 *
	 * @param reactor         the reactor that ran the write, such as ExecQuery
	 * @param transaction     how the statements were committed
	 * @param statements      number of statements in the request
	 * @param succeeded       number of statements that ran without error
	 * @param failedStatement 1-based position of the statement that failed, or null
	 *                        when none did
	 * @param outcome         what the request left in the database
	 * @param sql             the SQL sent to the database, used only for its hash,
	 *                        or null when it is not hashed
	 * @param errorMessage    why the write failed, or null
	 * @param timeoutSeconds  the time limit the request set, or null for none
	 */
	public record Write(String reactor, Transaction transaction, int statements, int succeeded, Integer failedStatement,
			Outcome outcome, String sql, String errorMessage, Integer timeoutSeconds) {

		/**
		 * A write whose statements each committed on their own, in order, stopping at
		 * the first failure.
		 *
		 * @param reactor         the reactor that ran the write
		 * @param sql             the SQL sent to the database, or null when it is not
		 *                        hashed
		 * @param statements      number of statements in the request
		 * @param succeeded       number of statements that ran without error
		 * @param committedWrites number of those that changed the database, which
		 *                        excludes reads in a mixed script
		 * @param error           the failure that stopped the request, or null
		 * @return the write
		 */
		public static Write perStatement(String reactor, String sql, int statements, int succeeded, int committedWrites,
				Throwable error) {
			Outcome outcome = error == null ? Outcome.COMMITTED
					: committedWrites > 0 ? Outcome.PARTIAL : Outcome.ROLLED_BACK;
			return new Write(reactor, Transaction.PER_STATEMENT, statements, succeeded,
					error == null ? null : succeeded + 1, outcome, sql, messageOf(error), null);
		}
	}

	private DatabaseWriteAudit() {
	}

	/**
	 * Records one database write. Never throws: a failure to record is logged and
	 * leaves the write itself untouched.
	 *
	 * @param insight the insight the write ran in, which gives the actor
	 * @param engine  the database written to
	 * @param write   what ran and how it ended
	 */
	public static void record(Insight insight, IEngine engine, Write write) {
		record(insight, engine, write, null);
	}

	/**
	 * Records one database write an admin made through an admin reactor, marked as
	 * an admin's like the other admin events. Never throws.
	 *
	 * @param insight the insight the write ran in, which gives the admin
	 * @param engine  the database written to
	 * @param write   what ran and how it ended
	 */
	public static void recordAsAdmin(Insight insight, IEngine engine, Write write) {
		record(insight, engine, write, true);
	}

	private static void record(Insight insight, IEngine engine, Write write, Boolean actorIsAdmin) {
		if (engine == null || write == null || !Utility.isUserTrackingEnabled()) {
			return;
		}
		try {
			String engineId = engine.getEngineId();
			UserAuditTrailUtils.AuditEvent event = new UserAuditTrailUtils.AuditEvent()
					.eventType(AuditEventType.DATABASE_WRITE).status(status(write.outcome()))
					.actorUser(insight == null ? null : insight.getUser()).actorIsAdmin(actorIsAdmin)
					.target(AuditTargetType.DATABASE, engineId, engine.getEngineName())
					.context(null, engineId, insight == null ? null : insight.getInsightId(), null)
					.details(details(write, engine.keepInputOutput()));
			if (write.errorMessage() != null) {
				event.error(AuditErrorCode.OPERATION_FAILED, write.errorMessage());
			}
			UserAuditTrailUtils.recordEvent(event);
		} catch (Exception e) {
			classLogger.warn("Unable to record the audit event for a write to database {}", engine.getEngineId(), e);
		}
	}

	/**
	 * The status a write's outcome is stored with.
	 *
	 * @param outcome what the write left in the database
	 * @return SUCCESS when every change was committed, ERROR when the outcome is
	 *         unknown, FAILURE otherwise
	 */
	static AuditStatus status(Outcome outcome) {
		if (outcome == Outcome.COMMITTED) {
			return AuditStatus.SUCCESS;
		}
		return outcome == Outcome.UNKNOWN ? AuditStatus.ERROR : AuditStatus.FAILURE;
	}

	/**
	 * The event's details. They never contain SQL text.
	 *
	 * @param write           what ran and how it ended
	 * @param keepInputOutput whether the engine keeps its input and output, which
	 *                        decides whether the SQL's hash is kept; always true
	 *                        for a database
	 * @return the details, in a fixed order
	 */
	static Map<String, Object> details(Write write, boolean keepInputOutput) {
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("reactor", write.reactor());
		details.put("transaction", write.transaction().name());
		details.put("statements", write.statements());
		details.put("succeeded", write.succeeded());
		if (write.failedStatement() != null) {
			details.put("failedStatement", write.failedStatement());
		}
		details.put("outcome", write.outcome().name());
		if (write.timeoutSeconds() != null) {
			details.put("timeoutSeconds", write.timeoutSeconds());
		}
		if (keepInputOutput && write.sql() != null) {
			String hash = sqlHash(write.sql());
			if (hash != null) {
				details.put("sqlSha256", hash);
			}
		}
		return details;
	}

	/**
	 * The SHA-256 of the SQL. A digest that cannot be made leaves the hash out
	 * rather than losing the event.
	 *
	 * @param sql the SQL sent to the database
	 * @return the hex digest, or null when SHA-256 is unavailable
	 */
	private static String sqlHash(String sql) {
		try {
			return UserAuditTrailUtils.sha256Hex(sql);
		} catch (RuntimeException e) {
			classLogger.warn("Unable to hash the SQL of a database write; recording the write without its hash", e);
			return null;
		}
	}

	private static String messageOf(Throwable error) {
		if (error == null) {
			return null;
		}
		return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
	}
}
