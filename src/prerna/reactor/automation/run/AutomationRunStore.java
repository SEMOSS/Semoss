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
package prerna.reactor.automation.run;

import static prerna.reactor.automation.AutomationConstants.AGENT_RUN_ID;
import static prerna.reactor.automation.AutomationConstants.AUTOMATION_ID;
import static prerna.reactor.automation.AutomationConstants.CANCEL_REQUESTED;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_AT;
import static prerna.reactor.automation.AutomationConstants.COMPLETED_NODES;
import static prerna.reactor.automation.AutomationConstants.CREATED_BY;
import static prerna.reactor.automation.AutomationConstants.DEFINITION_HASH;
import static prerna.reactor.automation.AutomationConstants.DEFINITION_SNAPSHOT;
import static prerna.reactor.automation.AutomationConstants.DEFINITION_VERSION;
import static prerna.reactor.automation.AutomationConstants.DURATION_MS;
import static prerna.reactor.automation.AutomationConstants.ERROR_MESSAGE;
import static prerna.reactor.automation.AutomationConstants.EXECUTION_CONTRACT_SNAPSHOT;
import static prerna.reactor.automation.AutomationConstants.EXECUTION_CONTRACT_VERSION;
import static prerna.reactor.automation.AutomationConstants.EXECUTION_ORDER;
import static prerna.reactor.automation.AutomationConstants.FAILED_NODE_ID;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_AGENT_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_MODEL_MSG;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_ROOM;
import static prerna.reactor.automation.AutomationConstants.IDX_ANO_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_ARW_AGENT_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_ARW_RUN;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_PROJECT;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_STARTED;
import static prerna.reactor.automation.AutomationConstants.IDX_AR_STATUS;
import static prerna.reactor.automation.AutomationConstants.INPUT_SNAPSHOT;
import static prerna.reactor.automation.AutomationConstants.LAST_HEARTBEAT;
import static prerna.reactor.automation.AutomationConstants.MODEL_MESSAGE_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_FIELD_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_FIELD_LABEL;
import static prerna.reactor.automation.AutomationConstants.NODE_ID;
import static prerna.reactor.automation.AutomationConstants.NODE_LABEL;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_FAILED;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_PENDING;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_RUNNING;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_SKIPPED;
import static prerna.reactor.automation.AutomationConstants.NODE_STATUS_SUCCESS;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_KIND;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_PREVIEW;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_VALUE;
import static prerna.reactor.automation.AutomationConstants.OUTPUT_VAR_NAME;
import static prerna.reactor.automation.AutomationConstants.PK_AUTOMATION_RUNS;
import static prerna.reactor.automation.AutomationConstants.PK_AUTO_NODE_OUT;
import static prerna.reactor.automation.AutomationConstants.PK_AUTO_RUN_SOURCE;
import static prerna.reactor.automation.AutomationConstants.PK_AUTO_RUN_WAIT;
import static prerna.reactor.automation.AutomationConstants.PROJECT_ID;
import static prerna.reactor.automation.AutomationConstants.RESULT_SUMMARY_COL;
import static prerna.reactor.automation.AutomationConstants.ROOM_ID;
import static prerna.reactor.automation.AutomationConstants.RUN_ID;
import static prerna.reactor.automation.AutomationConstants.SOURCE_CODE;
import static prerna.reactor.automation.AutomationConstants.SOURCE_HASH;
import static prerna.reactor.automation.AutomationConstants.STALE_HEARTBEAT_THRESHOLD_MINUTES;
import static prerna.reactor.automation.AutomationConstants.STARTED_AT;
import static prerna.reactor.automation.AutomationConstants.STATUS;
import static prerna.reactor.automation.AutomationConstants.STATUS_INTERRUPTED;
import static prerna.reactor.automation.AutomationConstants.STATUS_RUNNING;
import static prerna.reactor.automation.AutomationConstants.STATUS_SUBMITTED;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_NODE_OUTPUTS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_RESULT_CHUNKS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_RUNS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_RUN_NODE_SOURCES;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_RUN_WAITS;
import static prerna.reactor.automation.AutomationConstants.TABLE_AUTOMATION_SAVED_RESULTS;
import static prerna.reactor.automation.AutomationConstants.TOTAL_NODES;
import static prerna.reactor.automation.AutomationConstants.TRIGGER_TYPE;
import static prerna.reactor.automation.AutomationConstants.WORKSPACE_ID;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.AutomationRuntime;
import prerna.reactor.automation.definition.AutomationDefinitionService;
import prerna.reactor.automation.utils.AutomationRuntimeUtils;
import prerna.reactor.scheduler.SchedulerOwlCreator;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Persists Automation run, source-snapshot, and node-output state in the
 * scheduler database.
 *
 * <p>
 * Reads use SEMOSS query structures and writes use parameterized statements.
 * The scheduler OWL owns the logical schema; startup initialization creates the
 * corresponding physical tables in the scheduler database.
 */
public final class AutomationRunStore {

	private static final Logger classLogger = LogManager.getLogger(AutomationRunStore.class);
	private static final long LEASE_SECONDS = 60;
	private static final int QUEUE_CLAIM_RETRIES = 16;
	private static final String LEASE_OWNER_ID =
			System.getProperty("semoss.automation.workerId", UUID.randomUUID().toString());
	private static final ThreadLocal<Map<String, String>> CLAIMED_LEASES =
			ThreadLocal.withInitial(HashMap::new);
	private static final Map<String, String> ACTIVE_RUN_LEASES = new ConcurrentHashMap<>();
	private static final ThreadLocal<ItemClaim> CLAIMED_ITEM = new ThreadLocal<>();
	/** Keeps dynamically materialized body rows after the stable parent graph rows. */
	private static final int LOOP_EXECUTION_ORDER_OFFSET = 1_000_000;

	/** Prefix identifying the automation tables inside the scheduler OWL schema. */
	private static final String AUTOMATION_TABLE_PREFIX = "AUTOMATION_";

	/**
	 * Columns created NOT NULL, by table. The OWL schema models column names and
	 * types only, so the nullability the tables were originally built with is kept
	 * here rather than being silently dropped.
	 */
	private static final Map<String, Set<String>> NOT_NULL_COLUMNS = notNullColumns();

	// Table name shortcuts for SelectQueryStruct (TABLE__COLUMN format)
	private static final String TABLE_RUNS = TABLE_AUTOMATION_RUNS;
	private static final String TABLE_RUN_SOURCES = TABLE_AUTOMATION_RUN_NODE_SOURCES;
	private static final String TABLE_NODE_OUTPUTS = TABLE_AUTOMATION_NODE_OUTPUTS;
	private static final String TABLE_RUN_WAITS = TABLE_AUTOMATION_RUN_WAITS;
	private static final String TABLE_SAVED_RESULTS = TABLE_AUTOMATION_SAVED_RESULTS;
	private static final String TABLE_RESULT_CHUNKS = TABLE_AUTOMATION_RESULT_CHUNKS;
	private static final String TABLE_ITEMS = AutomationConstants.TABLE_AUTOMATION_ITEMS;
	private static final String TABLE_ITEM_ATTEMPTS = AutomationConstants.TABLE_AUTOMATION_ITEM_ATTEMPTS;
	private static final String TABLE_EFFECTS = AutomationConstants.TABLE_AUTOMATION_EFFECTS;
	private static final String ACTIVE_ADMISSION_STATUSES =
			"('SUBMITTED','RUNNING','WAITING_FOR_INPUT','WAITING_FOR_ITEMS')";
	private static final String GLOBAL_ADMISSION_LOCK = "GLOBAL";

	private static Map<String, Set<String>> notNullColumns() {
		Map<String, Set<String>> required = new LinkedHashMap<>();
		required.put(TABLE_AUTOMATION_RUNS, Set.of(RUN_ID, PROJECT_ID, STATUS, TRIGGER_TYPE, STARTED_AT));
		required.put(TABLE_AUTOMATION_RUN_NODE_SOURCES, Set.of(RUN_ID, NODE_ID, SOURCE_HASH, SOURCE_CODE));
		required.put(TABLE_AUTOMATION_NODE_OUTPUTS, Set.of(RUN_ID, NODE_ID, EXECUTION_ORDER, STATUS));
		required.put(TABLE_AUTOMATION_RUN_WAITS,
				Set.of(AutomationConstants.WAIT_ID, RUN_ID, NODE_ID, AutomationConstants.WAIT_TYPE, AGENT_RUN_ID,
						ROOM_ID, STATUS, STARTED_AT, AutomationConstants.EXPIRES_AT));
		required.put(TABLE_AUTOMATION_SAVED_RESULTS,
				Set.of(AutomationConstants.RESULT_ID, PROJECT_ID, RUN_ID, NODE_ID, AutomationConstants.RESULT_KIND,
						AutomationConstants.FORMAT_VERSION, AutomationConstants.BYTE_COUNT,
						AutomationConstants.CHECKSUM, AutomationConstants.CREATED_AT,
						AutomationConstants.CAPTURE_STATE, AutomationConstants.RESULT_ROLE));
		required.put(TABLE_AUTOMATION_RESULT_CHUNKS,
				Set.of(AutomationConstants.RESULT_ID, AutomationConstants.CHUNK_INDEX,
						AutomationConstants.FIRST_ROW_INDEX, AutomationConstants.CHUNK_ROW_COUNT,
						AutomationConstants.BYTE_COUNT, AutomationConstants.CHECKSUM,
						AutomationConstants.CHUNK_VALUE));
		required.put(AutomationConstants.TABLE_AUTOMATION_ADMISSION_LOCK,
				Set.of(AutomationConstants.ADMISSION_LOCK_ID));
		required.put(AutomationConstants.TABLE_AUTOMATION_ITEMS,
				Set.of(AutomationConstants.ITEM_ID, RUN_ID, NODE_ID, AutomationConstants.ITEM_INDEX,
						AutomationConstants.ITEM_INPUT_SNAPSHOT, STATUS, AutomationConstants.ATTEMPT_NUMBER,
						AutomationConstants.MAX_ATTEMPTS, AutomationConstants.CREATED_AT));
		required.put(AutomationConstants.TABLE_AUTOMATION_ITEM_ATTEMPTS,
				Set.of(AutomationConstants.ATTEMPT_ID, AutomationConstants.ITEM_ID, RUN_ID,
						AutomationConstants.ATTEMPT_NUMBER, STATUS, AutomationConstants.STARTED_AT));
		required.put(AutomationConstants.TABLE_AUTOMATION_EFFECTS,
				Set.of(AutomationConstants.EFFECT_ID, RUN_ID, NODE_ID, AutomationConstants.EFFECT_STATE,
						AutomationConstants.OPERATION_KEY, AutomationConstants.PREPARED_AT));
		return Map.copyOf(required);
	}

	private AutomationRunStore() {
	}

	// AUTOMATION_RUNS
	private static final String INSERT_RUN = """
			INSERT INTO AUTOMATION_RUNS \
			(RUN_ID, PROJECT_ID, AUTOMATION_ID, DEFINITION_VERSION, DEFINITION_HASH, DEFINITION_SNAPSHOT, \
			EXECUTION_CONTRACT_VERSION, EXECUTION_CONTRACT_SNAPSHOT, INPUT_SNAPSHOT, \
			STATUS, TRIGGER_TYPE, \
			STARTED_AT, LAST_HEARTBEAT, TOTAL_NODES, COMPLETED_NODES, CREATED_BY, PRINCIPAL_ACCESS, ASYNC_ADMISSION) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""";

	private static final String UPDATE_RUN_STATUS = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			FAILED_NODE_ID = ?, ERROR_MESSAGE = ? \
			WHERE RUN_ID = ? AND PROJECT_ID = ? AND STATUS = ? AND LEASE_TOKEN = ?""";

	private static final String UPDATE_RUN_SUMMARY = "UPDATE AUTOMATION_RUNS SET RESULT_SUMMARY = ? WHERE RUN_ID = ?";

	private static final String UPDATE_HEARTBEAT = """
			UPDATE AUTOMATION_RUNS SET LAST_HEARTBEAT = ?, COMPLETED_NODES = ?, LEASE_EXPIRES_AT = ? \
			WHERE RUN_ID = ? AND STATUS = ? AND LEASE_TOKEN = ?""";

	private static final String TOUCH_HEARTBEAT = """
			UPDATE AUTOMATION_RUNS SET LAST_HEARTBEAT = ?, LEASE_EXPIRES_AT = ? \
			WHERE RUN_ID = ? AND STATUS = ? AND LEASE_TOKEN = ?""";

	private static final String SET_CANCEL_REQUESTED = "UPDATE AUTOMATION_RUNS SET CANCEL_REQUESTED = ? WHERE RUN_ID = ?";
	private static final String CANCEL_SUBMITTED_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, CANCEL_REQUESTED = ?, COMPLETED_AT = ?, ERROR_MESSAGE = ? \
			WHERE RUN_ID = ? AND PROJECT_ID = ? AND STATUS = ?""";
	private static final String CANCEL_RUNNING_RUN = """
			UPDATE AUTOMATION_RUNS SET CANCEL_REQUESTED = ? \
			WHERE RUN_ID = ? AND PROJECT_ID = ? AND STATUS = ?""";
	private static final String CANCEL_WAITING_ITEMS_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, CANCEL_REQUESTED = ?, COMPLETED_AT = ?, ERROR_MESSAGE = ?, \
			LEASE_OWNER = NULL, LEASE_TOKEN = NULL, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND PROJECT_ID = ? AND STATUS = ?""";

	private static final String CLAIM_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, STARTED_AT = ?, LAST_HEARTBEAT = ?, \
			ATTEMPT_ID = ?, ATTEMPT_NUMBER = COALESCE(ATTEMPT_NUMBER, 0) + 1, \
			LEASE_OWNER = ?, LEASE_TOKEN = ?, LEASE_EXPIRES_AT = ? \
			WHERE RUN_ID = ? AND STATUS = ?""";
	private static final String RELEASE_CLAIMED_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, STARTED_AT = ?, LAST_HEARTBEAT = ?, \
			ATTEMPT_ID = NULL, LEASE_OWNER = NULL, LEASE_TOKEN = NULL, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND STATUS = ? AND LEASE_TOKEN = ? \
			AND (CANCEL_REQUESTED IS NULL OR CANCEL_REQUESTED = ?)""";
	private static final String CANCEL_CLAIMED_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, ERROR_MESSAGE = ?, \
			LEASE_OWNER = NULL, LEASE_TOKEN = NULL, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND STATUS = ? AND LEASE_TOKEN = ? AND CANCEL_REQUESTED = ?""";
	private static final String FAIL_CLAIMED_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, ERROR_MESSAGE = ?, \
			LEASE_OWNER = NULL, LEASE_TOKEN = NULL, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND STATUS = ? AND LEASE_TOKEN = ? \
			AND (CANCEL_REQUESTED IS NULL OR CANCEL_REQUESTED = ?)""";

	private static final String MARK_STALE_SUBMITTED_INTERRUPTED = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			ERROR_MESSAGE = ?, LEASE_OWNER = NULL, LEASE_TOKEN = NULL, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND STATUS = ? \
			AND (LAST_HEARTBEAT IS NULL OR LAST_HEARTBEAT <= ?)""";
	private static final String MARK_EXPIRED_LEASE_INTERRUPTED = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, COMPLETED_AT = ?, \
			ERROR_MESSAGE = ?, LEASE_OWNER = NULL, LEASE_TOKEN = NULL, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND STATUS = ? \
			AND (LEASE_TOKEN = ? OR (LEASE_TOKEN IS NULL AND ? IS NULL)) \
			AND (LEASE_EXPIRES_AT IS NULL OR LEASE_EXPIRES_AT <= ? \
				OR LAST_HEARTBEAT IS NULL OR LAST_HEARTBEAT <= ?)""";

	// AUTOMATION_RUN_WAITS
	private static final String INSERT_RUN_WAIT = """
			INSERT INTO AUTOMATION_RUN_WAITS \
			(WAIT_ID, RUN_ID, NODE_ID, WAIT_TYPE, AGENT_RUN_ID, ROOM_ID, RESUME_NODE_ID, \
			STATUS, CREATED_BY, STARTED_AT, EXPIRES_AT) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";

	private static final String MARK_RUN_WAITING = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, LAST_HEARTBEAT = ?, LEASE_EXPIRES_AT = NULL \
			WHERE RUN_ID = ? AND PROJECT_ID = ? AND STATUS = ? AND LEASE_TOKEN = ?""";

	private static final String MARK_NODE_WAITING = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, DURATION_MS = ?, \
			OUTPUT_VAR_NAME = ?, OUTPUT_VALUE = ?, OUTPUT_PREVIEW = ?, AGENT_RUN_ID = ? \
			WHERE RUN_ID = ? AND NODE_ID = ? AND STATUS = ?""";

	private static final String CLAIM_WAITING_RUN = """
			UPDATE AUTOMATION_RUNS SET STATUS = ?, LAST_HEARTBEAT = ?, ATTEMPT_ID = ?, \
			ATTEMPT_NUMBER = COALESCE(ATTEMPT_NUMBER, 0) + 1, LEASE_OWNER = ?, LEASE_TOKEN = ?, \
			LEASE_EXPIRES_AT = ? \
			WHERE RUN_ID = ? AND PROJECT_ID = ? AND STATUS = ?""";

	private static final String CLAIM_RUN_WAIT = """
			UPDATE AUTOMATION_RUN_WAITS SET STATUS = ? \
			WHERE WAIT_ID = ? AND RUN_ID = ? AND STATUS = ?""";

	private static final String RESOLVE_RUN_WAIT = """
			UPDATE AUTOMATION_RUN_WAITS SET STATUS = ?, RESOLVED_AT = ?, RESOLVED_BY = ? \
			WHERE WAIT_ID = ? AND RUN_ID = ? AND STATUS = ?""";

	// AUTOMATION_RUN_NODE_SOURCES
	private static final String INSERT_RUN_NODE_SOURCE = """
			INSERT INTO AUTOMATION_RUN_NODE_SOURCES \
			(RUN_ID, NODE_ID, SOURCE_HASH, SOURCE_CODE) VALUES (?, ?, ?, ?)""";

	// AUTOMATION_NODE_OUTPUTS
	private static final String INSERT_NODE_OUTPUT = """
			INSERT INTO AUTOMATION_NODE_OUTPUTS \
			(RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS, ROOM_ID, WORKSPACE_ID) \
			VALUES (?, ?, ?, ?, ?, ?, ?)""";

	private static final String INSERT_LOOP_NODE_OUTPUT = """
			INSERT INTO AUTOMATION_NODE_OUTPUTS \
			(RUN_ID, NODE_ID, NODE_LABEL, EXECUTION_ORDER, STATUS, ROOM_ID, WORKSPACE_ID, \
			PARENT_NODE_ID, ITERATION_INDEX, SOURCE_NODE_ID) \
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";

	private static final String INCREMENT_RUN_TOTAL_NODES =
			"UPDATE AUTOMATION_RUNS SET TOTAL_NODES = TOTAL_NODES + ? WHERE RUN_ID = ?";
	private static final String FENCE_ACTIVE_LEASE = """
			UPDATE AUTOMATION_RUNS SET LAST_HEARTBEAT = LAST_HEARTBEAT \
			WHERE RUN_ID = ? AND STATUS = ? AND LEASE_TOKEN = ?""";

	private static final String SKIP_PENDING_NODE_OUTPUT = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, ERROR_MESSAGE = ? \
			WHERE RUN_ID = ? AND NODE_ID = ? AND STATUS = ?""";

	private static final String UPDATE_NODE_OUTPUT_SUCCESS = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, OUTPUT_VAR_NAME = ?, OUTPUT_KIND = ?, OUTPUT_VALUE = ?, OUTPUT_PREVIEW = ?, \
			MODEL_MESSAGE_ID = ?, AGENT_RUN_ID = ? \
			WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String INSERT_SAVED_VALUE = """
			INSERT INTO AUTOMATION_SAVED_RESULTS \
			(RESULT_ID, PROJECT_ID, RUN_ID, NODE_ID, PARENT_NODE_ID, ITERATION_INDEX, RESULT_KIND, \
			FORMAT_VERSION, ROW_COUNT, BYTE_COUNT, CHECKSUM, CREATED_AT, RETAIN_UNTIL, CAPTURE_STATE, \
			RESULT_ROLE, LOGICAL_NAME, PAYLOAD_VALUE) \
			SELECT ?, R.PROJECT_ID, N.RUN_ID, N.NODE_ID, N.PARENT_NODE_ID, N.ITERATION_INDEX, ?, \
			?, ?, ?, ?, ?, ?, ?, ?, ?, ? \
			FROM AUTOMATION_NODE_OUTPUTS N JOIN AUTOMATION_RUNS R ON R.RUN_ID = N.RUN_ID \
			WHERE N.RUN_ID = ? AND N.NODE_ID = ?""";

	private static final String INSERT_RESULT_CHUNK = """
			INSERT INTO AUTOMATION_RESULT_CHUNKS \
			(RESULT_ID, CHUNK_INDEX, FIRST_ROW_INDEX, CHUNK_ROW_COUNT, BYTE_COUNT, CHECKSUM, CHUNK_VALUE) \
			VALUES (?, ?, ?, ?, ?, ?, ?)""";

	private static final String DELETE_RESULT_CHUNKS =
			"DELETE FROM AUTOMATION_RESULT_CHUNKS WHERE RESULT_ID = ?";
	private static final String EXPIRE_SAVED_RESULT = """
			UPDATE AUTOMATION_SAVED_RESULTS SET CAPTURE_STATE = ?, PAYLOAD_VALUE = NULL \
			WHERE RESULT_ID = ? AND CAPTURE_STATE = ?""";

	private static final String UPDATE_NODE_OUTPUT_FAILED = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ? AND NODE_ID = ?""";

	// A null agent run id leaves the persisted one in place: a node that already
	// recorded its
	// child run keeps that link, which is what makes the child discoverable after
	// the parent stops.
	private static final String UPDATE_NODE_OUTPUT_FAILED_WITH_RESULT = """
			UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ?, COMPLETED_AT = ?, \
			DURATION_MS = ?, OUTPUT_VAR_NAME = ?, OUTPUT_KIND = ?, OUTPUT_VALUE = ?, OUTPUT_PREVIEW = ?, \
			AGENT_RUN_ID = COALESCE(?, AGENT_RUN_ID), ERROR_MESSAGE = ? \
			WHERE RUN_ID = ? AND NODE_ID = ?""";

	private static final String UPDATE_NODE_OUTPUT_AGENT_RUN_TRACE = "UPDATE AUTOMATION_NODE_OUTPUTS SET AGENT_RUN_ID = ? WHERE RUN_ID = ? AND NODE_ID = ?";

	private static final String UPDATE_NODE_STATUS = "UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, STARTED_AT = ? WHERE RUN_ID = ? AND NODE_ID = ?";

	private static final String SKIP_PENDING_NODE_OUTPUTS = "UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS = ?, ERROR_MESSAGE = ? WHERE RUN_ID = ? AND STATUS = ?";

	// -- Initialization
	// ------------------------------------------------------------

	/**
	 * Creates the physical automation tables in the scheduler DB. The scheduler's
	 * authoritative OWL schema is owned by
	 * {@link prerna.reactor.scheduler.SchedulerOwlCreator}. Called at platform
	 * startup after the scheduler DB and its OWL are initialized. Safe to call on
	 * every startup (uses IF NOT EXISTS / metadata checks).
	 */
	public static void initialize() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			classLogger.warn("Scheduler DB not available - automation tables will not be created");
			return;
		}

		Connection conn = null;
		boolean initialized = false;
		try {
			conn = schedulerDb.getConnection();
			AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
			String database = schedulerDb.getDatabase();
			String schema = schedulerDb.getSchema();
			boolean allowIfExists = queryUtil.allowsIfExistsTableSyntax();

			SchedulerOwlCreator owlCreator = new SchedulerOwlCreator(queryUtil);
			List<Pair<String, List<Pair<String, String>>>> automationTables = owlCreator.getDBSchema().stream()
					.filter(table -> table.getValue0().startsWith(AUTOMATION_TABLE_PREFIX)).toList();

			// create the tables and columns from the OWL creator schema
			AbstractOwlCreator.syncSchema(schedulerDb, conn, automationTables, NOT_NULL_COLUMNS);
			applyKeysAndIndexes(conn, queryUtil, database, schema, allowIfExists);
			ensureAdmissionLock(conn);

			if (!conn.getAutoCommit()) {
				conn.commit();
			}

			classLogger.info("Automation engine tables initialized successfully");
			initialized = true;
		} catch (Exception e) {
			classLogger.error("Failed to initialize automation engine tables", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
		if (initialized) {
			try {
				int recoveredHandoffs = recoverStrandedDurableItemHandoffs();
				int expired = expireSavedResults(Instant.now(), 10_000);
				int expiredItems = expireDurableItemPayloads(Instant.now(), 10_000);
				if (recoveredHandoffs > 0) {
					classLogger.info("Recovered {} stranded durable Automation item handoffs during startup",
							recoveredHandoffs);
				}
				if (expired > 0) {
					classLogger.info("Expired {} retained Automation result payloads during startup", expired);
				}
				if (expiredItems > 0) {
					classLogger.info("Expired {} durable Automation item payloads during startup", expiredItems);
				}
			} catch (RuntimeException e) {
				classLogger.error("Failed to expire retained Automation result payloads during startup", e);
			}
		}
	}

	/**
	 * Applies the keys and indexes the OWL schema does not model. Both helpers are
	 * no-ops when the constraint already exists.
	 */
	private static void applyKeysAndIndexes(Connection conn, AbstractSqlQueryUtil queryUtil, String database,
			String schema, boolean allowIfExists) throws SQLException {
		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_AUTOMATION_RUNS, database, schema, PK_AUTOMATION_RUNS,
				new String[] { RUN_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_AR_PROJECT, TABLE_AUTOMATION_RUNS,
				new String[] { PROJECT_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_AR_STATUS, TABLE_AUTOMATION_RUNS,
				new String[] { PROJECT_ID, STATUS });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_AR_STARTED, TABLE_AUTOMATION_RUNS,
				new String[] { PROJECT_ID, STARTED_AT });

		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_AUTOMATION_RUN_NODE_SOURCES, database, schema,
				PK_AUTO_RUN_SOURCE, new String[] { RUN_ID, NODE_ID });

		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_AUTOMATION_NODE_OUTPUTS, database, schema, PK_AUTO_NODE_OUT,
				new String[] { RUN_ID, NODE_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_RUN, TABLE_AUTOMATION_NODE_OUTPUTS,
				new String[] { RUN_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_ROOM, TABLE_AUTOMATION_NODE_OUTPUTS,
				new String[] { ROOM_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_MODEL_MSG, TABLE_AUTOMATION_NODE_OUTPUTS,
				new String[] { MODEL_MESSAGE_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ANO_AGENT_RUN, TABLE_AUTOMATION_NODE_OUTPUTS,
				new String[] { AGENT_RUN_ID });

		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_AUTOMATION_RUN_WAITS, database, schema, PK_AUTO_RUN_WAIT,
				new String[] { AutomationConstants.WAIT_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ARW_RUN, TABLE_AUTOMATION_RUN_WAITS,
				new String[] { RUN_ID, STATUS });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, IDX_ARW_AGENT_RUN, TABLE_AUTOMATION_RUN_WAITS,
				new String[] { AGENT_RUN_ID });

		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_SAVED_RESULTS, database, schema,
				AutomationConstants.PK_AUTO_SAVED_RESULT, new String[] { AutomationConstants.RESULT_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_ASR_RUN, TABLE_SAVED_RESULTS,
				new String[] { RUN_ID, NODE_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_ASR_PROJECT_CREATED,
				TABLE_SAVED_RESULTS, new String[] { PROJECT_ID, AutomationConstants.CREATED_AT });

		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_RESULT_CHUNKS, database, schema,
				AutomationConstants.PK_AUTO_RESULT_CHUNK,
				new String[] { AutomationConstants.RESULT_ID, AutomationConstants.CHUNK_INDEX });
		addPrimaryKeyIfNotExists(conn, queryUtil, AutomationConstants.TABLE_AUTOMATION_ADMISSION_LOCK, database,
				schema, AutomationConstants.PK_AUTO_ADMISSION_LOCK,
				new String[] { AutomationConstants.ADMISSION_LOCK_ID });
		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_ITEMS, database, schema, AutomationConstants.PK_AUTO_ITEM,
				new String[] { AutomationConstants.ITEM_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_AI_CLAIM, TABLE_ITEMS,
				new String[] { STATUS, AutomationConstants.NEXT_ATTEMPT_AT, AutomationConstants.CREATED_AT,
						AutomationConstants.ITEM_INDEX });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_AI_RUN, TABLE_ITEMS,
				new String[] { RUN_ID, NODE_ID, AutomationConstants.ITEM_INDEX });
		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_ITEM_ATTEMPTS, database, schema,
				AutomationConstants.PK_AUTO_ITEM_ATTEMPT, new String[] { AutomationConstants.ATTEMPT_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_AIA_ITEM,
				TABLE_ITEM_ATTEMPTS, new String[] { AutomationConstants.ITEM_ID, AutomationConstants.ATTEMPT_NUMBER });
		addPrimaryKeyIfNotExists(conn, queryUtil, TABLE_EFFECTS, database, schema,
				AutomationConstants.PK_AUTO_EFFECT, new String[] { AutomationConstants.EFFECT_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_AE_RUN, TABLE_EFFECTS,
				new String[] { RUN_ID, NODE_ID });
		createIndexIfNotExists(conn, queryUtil, allowIfExists, AutomationConstants.IDX_AE_ITEM, TABLE_EFFECTS,
				new String[] { AutomationConstants.ITEM_ID, AutomationConstants.ATTEMPT_ID });
	}

	/** Starts recurring lease reconciliation and retained-result expiration. */
	public static void startMaintenance() {
		AutomationRunRegistry.startMaintenance();
	}

	/** Stops Automation heartbeat and maintenance tasks during webapp shutdown. */
	public static void shutdownRuntime() {
		AutomationRunRegistry.shutdown();
	}

	/**
	 * Marks submitted or running rows whose heartbeat crossed the stale threshold
	 * as interrupted. Called during scheduler startup so abandoned runs do not
	 * remain active indefinitely.
	 */
	public static void markStaleRunsInterrupted() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return;
		}
		recoverStrandedDurableItemHandoffs();

		Timestamp threshold = toTimestamp(Instant.now().minusSeconds(STALE_HEARTBEAT_THRESHOLD_MINUTES * 60L));
		Timestamp now = toTimestamp(Instant.now());
		List<Map<String, Object>> results;
		try {
			results = QueryExecutionUtility.queryList(schedulerDb,
					"SELECT RUN_ID, STATUS, LAST_HEARTBEAT, LEASE_TOKEN, LEASE_EXPIRES_AT, ASYNC_ADMISSION, "
							+ "CASE WHEN LEASE_EXPIRES_AT IS NULL OR LEASE_EXPIRES_AT <= ? "
							+ "THEN TRUE ELSE FALSE END AS LEASE_EXPIRED "
							+ "FROM AUTOMATION_RUNS WHERE STATUS IN (?, ?)",
					statement -> {
						statement.setTimestamp(1, now);
						statement.setString(2, STATUS_SUBMITTED);
						statement.setString(3, STATUS_RUNNING);
					},
					result -> {
						Map<String, Object> row = new LinkedHashMap<>();
						row.put(RUN_ID, result.getString(RUN_ID));
						row.put(STATUS, result.getString(STATUS));
						row.put(LAST_HEARTBEAT, result.getTimestamp(LAST_HEARTBEAT));
						row.put(AutomationConstants.LEASE_TOKEN,
								result.getString(AutomationConstants.LEASE_TOKEN));
						row.put(AutomationConstants.LEASE_EXPIRES_AT,
								result.getTimestamp(AutomationConstants.LEASE_EXPIRES_AT));
						row.put("LEASE_EXPIRED", result.getBoolean("LEASE_EXPIRED"));
						row.put(AutomationConstants.ASYNC_ADMISSION,
								result.getObject(AutomationConstants.ASYNC_ADMISSION));
						return row;
					});
		} catch (Exception e) {
			classLogger.error("Failed to read active Automation leases for stale recovery", e);
			return;
		}
		if (results == null || results.isEmpty()) {
			return;
		}

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				for (Map<String, Object> row : results) {
					String runId = (String) row.get(RUN_ID);
					String currentStatus = (String) row.get(STATUS);

					Timestamp lastHeartbeat = toTimestampSafe(row.get(LAST_HEARTBEAT));
					boolean running = STATUS_RUNNING.equals(currentStatus);
					if (!running && Boolean.TRUE.equals(row.get(AutomationConstants.ASYNC_ADMISSION))) {
						continue;
					}
					boolean stale = running
							? Boolean.TRUE.equals(row.get("LEASE_EXPIRED"))
									|| lastHeartbeat == null || !lastHeartbeat.after(threshold)
							: lastHeartbeat == null || !lastHeartbeat.after(threshold);
					if (!stale) {
						classLogger.debug("Skipping automation run {} - heartbeat {} is newer than stale threshold {}",
								runId, lastHeartbeat, threshold);
						continue;
					}

					String sql = running ? MARK_EXPIRED_LEASE_INTERRUPTED : MARK_STALE_SUBMITTED_INTERRUPTED;
					try (PreparedStatement ps = conn.prepareStatement(sql)) {
						int index = 1;
						ps.setString(index++, STATUS_INTERRUPTED);
						ps.setTimestamp(index++, now);
						ps.setString(index++, running
								? "Execution lease expired; external effects may require reconciliation"
								: "Submitted run was abandoned before execution");
						ps.setString(index++, runId);
						ps.setString(index++, currentStatus);
						if (running) {
							String leaseToken = row.get(AutomationConstants.LEASE_TOKEN) == null ? null
									: String.valueOf(row.get(AutomationConstants.LEASE_TOKEN));
							schedulerDb.getQueryUtil().setNullableString(ps, index++, leaseToken);
							schedulerDb.getQueryUtil().setNullableString(ps, index++, leaseToken);
							ps.setTimestamp(index++, now);
							ps.setTimestamp(index++, threshold);
						} else {
							ps.setTimestamp(index++, threshold);
						}
						int updated = ps.executeUpdate();
						if (updated > 0) {
							if (running) {
								markUncommittedEffectsUnknown(conn, runId, null,
										"Run lease expired before effect publication completed");
							}
							classLogger.info("Marked stale automation run {} as INTERRUPTED", runId);
						} else {
							classLogger.debug("Automation run {} changed while stale recovery was in progress; "
									+ "leaving its status unchanged", runId);
						}
					}
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to mark stale automation runs", e);
		}
		recoverStaleDurableItems();
		reconcileDurableLoopCoordinators();
	}

	static int recoverStrandedDurableItemHandoffs() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return 0;
		}
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				Timestamp now = toTimestamp(Instant.now());
				List<String> runIds = new ArrayList<>();
				try (PreparedStatement select = conn.prepareStatement(
						"SELECT R.RUN_ID FROM AUTOMATION_RUNS R WHERE R.STATUS=? "
								+ "AND (R.LEASE_EXPIRES_AT IS NULL OR R.LEASE_EXPIRES_AT<=?) "
								+ "AND EXISTS (SELECT 1 FROM AUTOMATION_ITEMS I WHERE I.RUN_ID=R.RUN_ID AND I.STATUS=?) "
								+ "FOR UPDATE")) {
					select.setString(1, STATUS_RUNNING);
					select.setTimestamp(2, now);
					select.setString(3, AutomationConstants.ITEM_STATUS_PENDING);
					try (ResultSet rs = select.executeQuery()) {
						while (rs.next()) {
							runIds.add(rs.getString(1));
						}
					}
				}
				int recovered = 0;
				try (PreparedStatement update = conn.prepareStatement(
						"UPDATE AUTOMATION_RUNS SET STATUS=?,LAST_HEARTBEAT=?,LEASE_OWNER=NULL,LEASE_TOKEN=NULL,"
								+ "LEASE_EXPIRES_AT=NULL WHERE RUN_ID=? AND STATUS=? "
								+ "AND (LEASE_EXPIRES_AT IS NULL OR LEASE_EXPIRES_AT<=?)")) {
					for (String runId : runIds) {
						update.setString(1, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
						update.setTimestamp(2, now);
						update.setString(3, runId);
						update.setString(4, STATUS_RUNNING);
						update.setTimestamp(5, now);
						if (update.executeUpdate() == 1) {
							recovered++;
							classLogger.info("Recovered stranded durable Automation item handoff for run {}", runId);
						}
					}
				}
				return recovered;
			});
		} catch (Exception e) {
			classLogger.error("Failed to recover stranded durable Automation item handoffs", e);
			return 0;
		}
	}

	static void recoverStaleDurableItems() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return;
		}
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				Timestamp now = toTimestamp(Instant.now());
				try (PreparedStatement select = conn.prepareStatement(
						"SELECT ITEM_ID,RUN_ID,NODE_ID,ATTEMPT_ID,ATTEMPT_NUMBER,MAX_ATTEMPTS,ITEM_INDEX "
								+ "FROM AUTOMATION_ITEMS WHERE STATUS=? AND LEASE_EXPIRES_AT<=? FOR UPDATE")) {
					select.setString(1, AutomationConstants.ITEM_STATUS_RUNNING);
					select.setTimestamp(2, now);
					try (ResultSet rs = select.executeQuery()) {
						while (rs.next()) {
							String itemId = rs.getString(1);
							String attemptId = rs.getString(4);
							int attemptNumber = rs.getInt(5);
							int maxAttempts = rs.getInt(6);
							long itemIndex = rs.getLong(7);
							markUncommittedEffectsUnknown(conn, rs.getString(2), attemptId,
									"Item attempt lease expired before effect publication completed");
							boolean unknown = hasUnknownEffect(conn, itemId);
							String status = unknown ? AutomationConstants.ITEM_STATUS_BLOCKED
									: attemptNumber < maxAttempts ? AutomationConstants.ITEM_STATUS_RETRY
											: AutomationConstants.ITEM_STATUS_FAILED;
							try (PreparedStatement item = conn.prepareStatement(
									"UPDATE AUTOMATION_ITEMS SET STATUS=?,LEASE_OWNER=NULL,LEASE_TOKEN=NULL,"
											+ "LEASE_EXPIRES_AT=NULL,NEXT_ATTEMPT_AT=?,COMPLETED_AT=?,ERROR_MESSAGE=? "
											+ "WHERE ITEM_ID=? AND STATUS=?");
									PreparedStatement attempt = conn.prepareStatement(
											"UPDATE AUTOMATION_ITEM_ATTEMPTS SET STATUS=?,COMPLETED_AT=?,ERROR_MESSAGE=? "
													+ "WHERE ATTEMPT_ID=? AND STATUS=?")) {
								item.setString(1, status);
								item.setTimestamp(2, AutomationConstants.ITEM_STATUS_RETRY.equals(status) ? now : null);
								item.setTimestamp(3, AutomationConstants.ITEM_STATUS_RETRY.equals(status) ? null : now);
								schedulerDb.getQueryUtil().setNullableLargeText(item, 4,
										unknown ? "Effect outcome is UNKNOWN; reconciliation is required before retry"
												: "Item attempt lease expired");
								item.setString(5, itemId);
								item.setString(6, AutomationConstants.ITEM_STATUS_RUNNING);
								item.executeUpdate();
								attempt.setString(1, AutomationConstants.ATTEMPT_STATUS_INTERRUPTED);
								attempt.setTimestamp(2, now);
								schedulerDb.getQueryUtil().setNullableLargeText(attempt, 3,
										"Item attempt lease expired");
								attempt.setString(4, attemptId);
								attempt.setString(5, AutomationConstants.ATTEMPT_STATUS_RUNNING);
								attempt.executeUpdate();
							}
							try (PreparedStatement nodes = conn.prepareStatement(
									"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS=?,COMPLETED_AT=?,ERROR_MESSAGE=? "
											+ "WHERE RUN_ID=? AND PARENT_NODE_ID=? AND ITERATION_INDEX=? AND STATUS=?")) {
								nodes.setString(1, NODE_STATUS_FAILED);
								nodes.setTimestamp(2, now);
								schedulerDb.getQueryUtil().setNullableLargeText(nodes, 3,
										"Durable item attempt lease expired");
								nodes.setString(4, rs.getString(2));
								nodes.setString(5, rs.getString(3));
								nodes.setLong(6, itemIndex);
								nodes.setString(7, NODE_STATUS_RUNNING);
								nodes.executeUpdate();
							}
						}
					}
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to recover stale durable Automation items", e);
		}
	}

	// -- AUTOMATION_RUNS CRUD
	// --------------------------------------------------------

	/**
	 * Creates a submitted run together with its source snapshot and pending node
	 * rows in one transaction. Runs are independent; another active run for the
	 * same project does not block initialization.
	 *
	 * @throws IllegalStateException when the scheduler database is unavailable or
	 *                               history cannot be initialized
	 */
	public static void initializeRun(String runId, String projectId, String automationId, int definitionVersion,
			String definitionHash, String definitionSnapshot, Map<String, Object> inputs, String triggerType,
			String createdBy, List<Map<String, Object>> orderedNodes, Map<String, String> traceRoomIds,
			Map<String, String> nodeSources) {
		initializeRun(runId, projectId, automationId, definitionVersion, definitionHash, definitionSnapshot, null, null,
				inputs, triggerType, createdBy, null, orderedNodes, traceRoomIds, nodeSources);
	}

	/**
	 * Creates a submitted run with an immutable executor-selection contract.
	 */
	public static void initializeRun(String runId, String projectId, String automationId, int definitionVersion,
			String definitionHash, String definitionSnapshot, Integer executionContractVersion,
			String executionContractSnapshot, Map<String, Object> inputs, String triggerType, String createdBy,
			List<Map<String, Object>> orderedNodes, Map<String, String> traceRoomIds,
			Map<String, String> nodeSources) {
		initializeRun(runId, projectId, automationId, definitionVersion, definitionHash, definitionSnapshot,
				executionContractVersion, executionContractSnapshot, inputs, triggerType, createdBy, null,
				orderedNodes, traceRoomIds, nodeSources);
	}

	public static void initializeRun(String runId, String projectId, String automationId, int definitionVersion,
			String definitionHash, String definitionSnapshot, Integer executionContractVersion,
			String executionContractSnapshot, Map<String, Object> inputs, String triggerType, String createdBy,
			String principalAccess, List<Map<String, Object>> orderedNodes, Map<String, String> traceRoomIds,
			Map<String, String> nodeSources) {
		initializeRun(runId, projectId, automationId, definitionVersion, definitionHash, definitionSnapshot,
				executionContractVersion, executionContractSnapshot, inputs, triggerType, createdBy, principalAccess,
				false, orderedNodes, traceRoomIds, nodeSources);
	}

	public static void initializeRun(String runId, String projectId, String automationId, int definitionVersion,
			String definitionHash, String definitionSnapshot, Integer executionContractVersion,
			String executionContractSnapshot, Map<String, Object> inputs, String triggerType, String createdBy,
			String principalAccess, boolean asyncAdmission, List<Map<String, Object>> orderedNodes,
			Map<String, String> traceRoomIds, Map<String, String> nodeSources) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("initializing automation run history");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				enforceAdmissionLimits(conn, projectId);
				Timestamp now = toTimestamp(Instant.now());
				String inputSnapshot = AutomationRuntimeUtils.toBoundedRuntimeJson(inputs != null ? inputs : Map.of(),
						AutomationConstants.RUN_INPUTS_MAX_BYTES, "Automation run inputs");

				insertRun(conn, schedulerDb.getQueryUtil(), runId, projectId, automationId, definitionVersion,
						definitionHash, definitionSnapshot, executionContractVersion, executionContractSnapshot,
						inputSnapshot, triggerType, orderedNodes.size(), createdBy, principalAccess, asyncAdmission, now);
				insertAllRunNodeSources(conn, schedulerDb.getQueryUtil(), runId, nodeSources);
				insertAllNodeOutputs(conn, schedulerDb.getQueryUtil(), runId, orderedNodes, traceRoomIds);
				return null;
			});
		} catch (Exception e) {
			AutomationAdmissionException admission = findAdmissionException(e);
			if (admission != null) {
				throw admission;
			}
			classLogger.error("Failed to initialize automation run '{}' for project '{}'", runId, projectId, e);
			throw new IllegalStateException("Unable to initialize automation run history.", e);
		}
	}

	private static AutomationAdmissionException findAdmissionException(Throwable error) {
		Throwable current = error;
		while (current != null) {
			if (current instanceof AutomationAdmissionException admission) {
				return admission;
			}
			current = current.getCause();
		}
		return null;
	}

	private static void enforceAdmissionLimits(Connection conn, String projectId) throws SQLException {
		lockAdmission(conn);
		int globalLimit = positiveProperty("semoss.automation.admission.global",
				AutomationConstants.DEFAULT_GLOBAL_ADMISSION_LIMIT);
		int projectLimit = positiveProperty("semoss.automation.admission.project",
				AutomationConstants.DEFAULT_PROJECT_ADMISSION_LIMIT);
		int global = countActiveAdmissions(conn, null);
		if (global >= globalLimit) {
			throw new AutomationAdmissionException("Global Automation admission quota exceeded (" + globalLimit + ").");
		}
		int project = countActiveAdmissions(conn, projectId);
		if (project >= projectLimit) {
			throw new AutomationAdmissionException(
					"Automation admission quota exceeded for project '" + projectId + "' (" + projectLimit + ").");
		}
	}

	private static void ensureAdmissionLock(Connection conn) throws SQLException {
		try (PreparedStatement select = conn.prepareStatement(
				"SELECT LOCK_ID FROM AUTOMATION_ADMISSION_LOCK WHERE LOCK_ID = ?")) {
			select.setString(1, GLOBAL_ADMISSION_LOCK);
			try (ResultSet rs = select.executeQuery()) {
				if (rs.next()) {
					return;
				}
			}
		}
		Savepoint savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
		try (PreparedStatement insert = conn.prepareStatement(
				"INSERT INTO AUTOMATION_ADMISSION_LOCK (LOCK_ID) VALUES (?)")) {
			insert.setString(1, GLOBAL_ADMISSION_LOCK);
			insert.executeUpdate();
		} catch (SQLException e) {
			if (savepoint != null) {
				conn.rollback(savepoint);
			}
			try (PreparedStatement verify = conn.prepareStatement(
					"SELECT LOCK_ID FROM AUTOMATION_ADMISSION_LOCK WHERE LOCK_ID = ?")) {
				verify.setString(1, GLOBAL_ADMISSION_LOCK);
				try (ResultSet rs = verify.executeQuery()) {
					if (!rs.next()) {
						throw e;
					}
				}
			}
		}
	}

	private static void lockAdmission(Connection conn) throws SQLException {
		try (PreparedStatement lock = conn.prepareStatement(
				"SELECT LOCK_ID FROM AUTOMATION_ADMISSION_LOCK WHERE LOCK_ID = ? FOR UPDATE")) {
			lock.setString(1, GLOBAL_ADMISSION_LOCK);
			try (ResultSet rs = lock.executeQuery()) {
				if (!rs.next()) {
					throw new IllegalStateException("Automation admission lock is not initialized.");
				}
			}
		}
	}

	private static int countActiveAdmissions(Connection conn, String projectId) throws SQLException {
		String sql = "SELECT COUNT(*) FROM AUTOMATION_RUNS WHERE STATUS IN " + ACTIVE_ADMISSION_STATUSES
				+ (projectId == null ? "" : " AND PROJECT_ID = ?");
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			if (projectId != null) {
				ps.setString(1, projectId);
			}
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() ? rs.getInt(1) : 0;
			}
		}
	}

	private static int positiveProperty(String name, int defaultValue) {
		String configured = System.getProperty(name);
		if (configured == null || configured.isBlank()) {
			return defaultValue;
		}
		try {
			int value = Integer.parseInt(configured);
			return value > 0 ? value : defaultValue;
		} catch (NumberFormatException e) {
			classLogger.warn("Ignoring invalid positive integer property {}={}", name, configured);
			return defaultValue;
		}
	}

	public static final class AutomationAdmissionException extends IllegalStateException {
		private static final long serialVersionUID = 1L;

		private AutomationAdmissionException(String message) {
			super(message);
		}
	}

	record ClaimedRun(Map<String, Object> admission, String leaseToken) {
		String runId() {
			return String.valueOf(admission.get(RUN_ID));
		}
	}

	record CancellationResult(String status, boolean active) {
	}

	private record QueueClaimAttempt(boolean candidateFound, ClaimedRun claimedRun) {
	}

	record ItemClaim(String itemId, String runId, String nodeId, long itemIndex, String inputSnapshot,
			String referenceSnapshot, String attemptId, int attemptNumber, int maxAttempts, String leaseToken) {
	}

	record DurableItemAdmission(String projectId, String definitionSnapshot, String executionContractSnapshot,
			Integer executionContractVersion, String principalAccess) {
	}

	private record DurableItemSnapshot(String itemId, long itemIndex, String inputSnapshot,
			String referenceSnapshot, int maxAttempts) {
	}

	record EffectInvocation(String effectId, String operationKey) {
	}

	/**
	 * Atomically publishes an explicitly durable loop's immutable business-item
	 * queue and releases the owning run lease. Repeating the handoff with the same
	 * validated snapshots is harmless; a conflicting retry is rejected.
	 */
	static List<String> handoffToDurableItems(String runId, String loopNodeId, List<?> itemSnapshots,
			int maxAttempts) {
		if (maxAttempts < 1 || maxAttempts > AutomationConstants.DURABLE_ITEM_MAX_ATTEMPTS) {
			throw new IllegalArgumentException("Durable item maxAttempts is outside the supported range.");
		}
		List<DurableItemSnapshot> snapshots = durableItemSnapshots(runId, loopNodeId, itemSnapshots, maxAttempts);
		IRDBMSEngine schedulerDb = requireSchedulerDb("handing an automation run off to durable items");
		try {
			List<String> persistedItemIds = QueryExecutionUtility.write(schedulerDb, conn -> {
				String runStatus;
				String persistedLease;
				try (PreparedStatement run = conn.prepareStatement(
						"SELECT STATUS,LEASE_TOKEN FROM AUTOMATION_RUNS WHERE RUN_ID=? FOR UPDATE")) {
					run.setString(1, runId);
					try (ResultSet rs = run.executeQuery()) {
						if (!rs.next()) {
							throw new IllegalStateException("Automation run not found: " + runId);
						}
						runStatus = rs.getString(1);
						persistedLease = rs.getString(2);
					}
				}
				if (STATUS_RUNNING.equals(runStatus)) {
					String activeLease = requiredLease(runId);
					if (!Objects.equals(activeLease, persistedLease)) {
						throw new IllegalStateException("Automation run lease changed before durable-item handoff.");
					}
				} else if (!AutomationConstants.STATUS_WAITING_FOR_ITEMS.equals(runStatus)) {
					throw new IllegalStateException("Automation run is not eligible for durable-item handoff: "
							+ runStatus);
				}

				List<String> itemIds = new ArrayList<>(snapshots.size());
				try (PreparedStatement existing = conn.prepareStatement(
						"SELECT ITEM_ID,ITEM_INDEX,ITEM_INPUT_SNAPSHOT,ITEM_REFERENCE_SNAPSHOT,MAX_ATTEMPTS "
								+ "FROM AUTOMATION_ITEMS WHERE RUN_ID=? AND NODE_ID=? ORDER BY ITEM_INDEX");
						PreparedStatement insert = conn.prepareStatement(
								"INSERT INTO AUTOMATION_ITEMS (ITEM_ID,RUN_ID,NODE_ID,ITEM_INDEX,ITEM_INPUT_SNAPSHOT,"
										+ "ITEM_REFERENCE_SNAPSHOT,STATUS,ATTEMPT_NUMBER,MAX_ATTEMPTS,CREATED_AT,NEXT_ATTEMPT_AT) "
										+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
					existing.setString(1, runId);
					existing.setString(2, loopNodeId);
					try (ResultSet rs = existing.executeQuery()) {
						while (rs.next()) {
							int index = itemIds.size();
							if (index >= snapshots.size()) {
								throw durableItemSnapshotMismatch();
							}
							DurableItemSnapshot expected = snapshots.get(index);
							if (!expected.itemId().equals(rs.getString(1))
									|| expected.itemIndex() != rs.getLong(2)
									|| !Objects.equals(expected.inputSnapshot(), rs.getString(3))
									|| !Objects.equals(expected.referenceSnapshot(), rs.getString(4))
									|| expected.maxAttempts() != rs.getInt(5)) {
								throw durableItemSnapshotMismatch();
							}
							itemIds.add(rs.getString(1));
						}
					}
					if (!itemIds.isEmpty()) {
						if (itemIds.size() != snapshots.size()) {
							throw durableItemSnapshotMismatch();
						}
					} else if (AutomationConstants.STATUS_WAITING_FOR_ITEMS.equals(runStatus)
							&& !snapshots.isEmpty()) {
						throw durableItemSnapshotMismatch();
					} else {
						Timestamp now = toTimestamp(Instant.now());
						for (DurableItemSnapshot snapshot : snapshots) {
							int parameter = 1;
							insert.setString(parameter++, snapshot.itemId());
							insert.setString(parameter++, runId);
							insert.setString(parameter++, loopNodeId);
							insert.setLong(parameter++, snapshot.itemIndex());
							schedulerDb.getQueryUtil().setNullableLargeText(insert, parameter++,
									snapshot.inputSnapshot());
							schedulerDb.getQueryUtil().setNullableLargeText(insert, parameter++,
									snapshot.referenceSnapshot());
							insert.setString(parameter++, AutomationConstants.ITEM_STATUS_PENDING);
							insert.setInt(parameter++, 0);
							insert.setInt(parameter++, snapshot.maxAttempts());
							insert.setTimestamp(parameter++, now);
							insert.setTimestamp(parameter++, now);
							insert.addBatch();
							itemIds.add(snapshot.itemId());
						}
						if (!snapshots.isEmpty()) {
							insert.executeBatch();
						}
					}
				}
				if (STATUS_RUNNING.equals(runStatus)) {
					try (PreparedStatement ps = conn.prepareStatement(
							"UPDATE AUTOMATION_RUNS SET STATUS=?,LAST_HEARTBEAT=?,LEASE_OWNER=NULL,LEASE_TOKEN=NULL,"
									+ "LEASE_EXPIRES_AT=NULL WHERE RUN_ID=? AND STATUS=? AND LEASE_TOKEN=?")) {
						ps.setString(1, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
						ps.setTimestamp(2, toTimestamp(Instant.now()));
						ps.setString(3, runId);
						ps.setString(4, STATUS_RUNNING);
						ps.setString(5, persistedLease);
						requireSingleRow(ps.executeUpdate(), "park the durable-item run", runId, loopNodeId);
					}
				}
				return itemIds;
			});
			CLAIMED_LEASES.get().remove(runId);
			ACTIVE_RUN_LEASES.remove(runId);
			return persistedItemIds;
		} catch (Exception e) {
			throw new IllegalStateException("Unable to hand the Automation run off to durable items.", e);
		}
	}

	private static List<DurableItemSnapshot> durableItemSnapshots(String runId, String loopNodeId,
			List<?> itemSnapshots, int maxAttempts) {
		List<DurableItemSnapshot> snapshots = new ArrayList<>(itemSnapshots.size());
		for (int index = 0; index < itemSnapshots.size(); index++) {
			String itemId = stableItemId(runId, loopNodeId, index);
			String input = AutomationRuntimeUtils.toBoundedRuntimeJson(itemSnapshots.get(index),
					AutomationConstants.DURABLE_ITEM_SNAPSHOT_MAX_BYTES, "Durable Automation item input");
			String reference = AutomationRuntimeUtils.toBoundedRuntimeJson(
					Map.of("loopNodeId", loopNodeId, "itemIndex", index, "total", itemSnapshots.size()),
					AutomationConstants.DURABLE_ITEM_REFERENCE_MAX_BYTES,
					"Durable Automation item reference");
			snapshots.add(new DurableItemSnapshot(itemId, index, input, reference, maxAttempts));
		}
		return snapshots;
	}

	private static IllegalStateException durableItemSnapshotMismatch() {
		return new IllegalStateException(
				"Durable item history does not match the immutable loop input snapshot.");
	}

	private static String stableItemId(String runId, String loopNodeId, long itemIndex) {
		return UUID.nameUUIDFromBytes((runId + "\n" + loopNodeId + "\n" + itemIndex)
				.getBytes(StandardCharsets.UTF_8)).toString();
	}

	static DurableItemAdmission getDurableItemAdmission(ItemClaim claim) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("loading a durable automation item admission");
		Connection conn = null;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(
					"SELECT PROJECT_ID,DEFINITION_SNAPSHOT,EXECUTION_CONTRACT_SNAPSHOT,"
							+ "EXECUTION_CONTRACT_VERSION,PRINCIPAL_ACCESS FROM AUTOMATION_RUNS "
							+ "WHERE RUN_ID=? AND STATUS=?")) {
				ps.setString(1, claim.runId());
				ps.setString(2, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
				try (ResultSet rs = ps.executeQuery()) {
					if (!rs.next()) {
						throw new IllegalStateException(
								"Durable Automation item no longer belongs to a parked run.");
					}
					int version = rs.getInt(4);
					boolean versionMissing = rs.wasNull();
					return new DurableItemAdmission(rs.getString(1), rs.getString(2), rs.getString(3),
							versionMissing ? null : version, rs.getString(5));
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to load the durable Automation item admission.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	/**
	 * Atomically finalizes a parked terminal durable loop. Locking the run row and
	 * requiring WAITING_FOR_ITEMS makes competing coordinators harmless.
	 */
	static boolean coordinateDurableLoop(String runId, String loopNodeId, String loopOutputVar) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("coordinating a durable automation loop");
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				boolean cancelRequested;
				try (PreparedStatement lock = conn.prepareStatement(
						"SELECT CANCEL_REQUESTED FROM AUTOMATION_RUNS WHERE RUN_ID=? AND STATUS=? FOR UPDATE")) {
					lock.setString(1, runId);
					lock.setString(2, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
					try (ResultSet rs = lock.executeQuery()) {
						if (!rs.next()) {
							return false;
						}
						cancelRequested = rs.getBoolean(1);
					}
				}

				Map<String, Long> counts = new LinkedHashMap<>();
				try (PreparedStatement ps = conn.prepareStatement(
						"SELECT STATUS,COUNT(*) FROM AUTOMATION_ITEMS WHERE RUN_ID=? AND NODE_ID=? GROUP BY STATUS")) {
					ps.setString(1, runId);
					ps.setString(2, loopNodeId);
					try (ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							counts.put(rs.getString(1), rs.getLong(2));
						}
					}
				}
				long active = counts.getOrDefault(AutomationConstants.ITEM_STATUS_PENDING, 0L)
						+ counts.getOrDefault(AutomationConstants.ITEM_STATUS_RETRY, 0L)
						+ counts.getOrDefault(AutomationConstants.ITEM_STATUS_RUNNING, 0L);
				if (active > 0) {
					return false;
				}
				long blocked = counts.getOrDefault(AutomationConstants.ITEM_STATUS_BLOCKED, 0L);
				long failed = counts.getOrDefault(AutomationConstants.ITEM_STATUS_FAILED, 0L);
				long cancelled = counts.getOrDefault(AutomationConstants.ITEM_STATUS_CANCELLED, 0L);
				long succeeded = counts.getOrDefault(AutomationConstants.ITEM_STATUS_SUCCESS, 0L);
				long total = counts.values().stream().mapToLong(Long::longValue).sum();
				String runStatus;
				String nodeStatus;
				String error = null;
				if (cancelRequested || cancelled > 0) {
					runStatus = AutomationConstants.STATUS_CANCELLED;
					nodeStatus = NODE_STATUS_FAILED;
					error = "Durable Automation items were cancelled.";
				} else if (blocked > 0) {
					runStatus = AutomationConstants.STATUS_BLOCKED;
					nodeStatus = NODE_STATUS_FAILED;
					error = "One or more durable Automation items are BLOCKED by an UNKNOWN or committed effect.";
				} else if (failed > 0) {
					runStatus = AutomationConstants.STATUS_FAILED;
					nodeStatus = NODE_STATUS_FAILED;
					error = "One or more durable Automation items exhausted their attempts.";
				} else {
					runStatus = AutomationConstants.STATUS_SUCCESS;
					nodeStatus = NODE_STATUS_SUCCESS;
				}
				Map<String, Object> value = new LinkedHashMap<>();
				value.put("processed", succeeded);
				value.put("iterations", total);
				value.put("itemStatuses", counts);
				String output = AutomationRuntimeUtils.toBoundedRuntimeJson(value,
						AutomationConstants.NODE_OUTPUT_MAX_BYTES, "Durable Automation loop output");
				String preview = AutomationRuntimeUtils.generatePreview(output);
				try (PreparedStatement node = conn.prepareStatement(
						"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS=?,COMPLETED_AT=?,OUTPUT_VAR_NAME=?,"
								+ "OUTPUT_VALUE=?,OUTPUT_PREVIEW=?,ERROR_MESSAGE=? "
								+ "WHERE RUN_ID=? AND NODE_ID=? AND STATUS=?");
						PreparedStatement run = conn.prepareStatement(
								"UPDATE AUTOMATION_RUNS SET STATUS=?,COMPLETED_AT=?,FAILED_NODE_ID=?,"
										+ "ERROR_MESSAGE=?,LEASE_OWNER=NULL,LEASE_TOKEN=NULL,LEASE_EXPIRES_AT=NULL "
										+ "WHERE RUN_ID=? AND STATUS=?")) {
					Timestamp now = toTimestamp(Instant.now());
					node.setString(1, nodeStatus);
					node.setTimestamp(2, now);
					schedulerDb.getQueryUtil().setNullableString(node, 3, loopOutputVar);
					schedulerDb.getQueryUtil().setNullableLargeText(node, 4,
							NODE_STATUS_SUCCESS.equals(nodeStatus) ? output : null);
					schedulerDb.getQueryUtil().setNullableString(node, 5,
							NODE_STATUS_SUCCESS.equals(nodeStatus) ? preview : null);
					schedulerDb.getQueryUtil().setNullableLargeText(node, 6, error);
					node.setString(7, runId);
					node.setString(8, loopNodeId);
					node.setString(9, NODE_STATUS_RUNNING);
					requireSingleRow(node.executeUpdate(), "finalize the durable loop node", runId, loopNodeId);
					run.setString(1, runStatus);
					run.setTimestamp(2, now);
					schedulerDb.getQueryUtil().setNullableString(run, 3,
							NODE_STATUS_SUCCESS.equals(nodeStatus) ? null : loopNodeId);
					schedulerDb.getQueryUtil().setNullableLargeText(run, 4, error);
					run.setString(5, runId);
					run.setString(6, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
					requireSingleRow(run.executeUpdate(), "finalize the durable-item run", runId, loopNodeId);
				}
				return true;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to coordinate the durable Automation loop.", e);
		}
	}

	static void reconcileDurableLoopCoordinators() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return;
		}
		Connection conn = null;
		List<Map<String, String>> candidates = new ArrayList<>();
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(
					"SELECT R.RUN_ID,R.DEFINITION_SNAPSHOT,I.NODE_ID FROM AUTOMATION_RUNS R "
							+ "JOIN AUTOMATION_ITEMS I ON I.RUN_ID=R.RUN_ID WHERE R.STATUS=? "
							+ "GROUP BY R.RUN_ID,R.DEFINITION_SNAPSHOT,I.NODE_ID")) {
				ps.setString(1, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						candidates.add(Map.of(RUN_ID, rs.getString(1), DEFINITION_SNAPSHOT, rs.getString(2),
								NODE_ID, rs.getString(3)));
					}
				}
			}
		} catch (SQLException e) {
			classLogger.warn("Unable to load durable Automation coordinator candidates", e);
			return;
		} finally {
			closeConnection(schedulerDb, conn);
		}
		for (Map<String, String> candidate : candidates) {
			try {
				prerna.reactor.automation.definition.AutomationDefinitionValidator.ValidatedDefinition definition =
						prerna.reactor.automation.definition.AutomationDefinitionValidator
								.parseAndValidate(candidate.get(DEFINITION_SNAPSHOT));
				String nodeId = candidate.get(NODE_ID);
				String outputVar = AutomationRuntime.nodesForRun(definition).stream()
						.filter(node -> nodeId.equals(node.get(NODE_FIELD_ID)))
						.map(node -> (String) node.get(AutomationConstants.NODE_FIELD_OUTPUT_VAR))
						.findFirst()
						.orElse(null);
				coordinateDurableLoop(candidate.get(RUN_ID), nodeId, outputVar);
			} catch (Exception e) {
				classLogger.warn("Unable to reconcile durable Automation loop coordinator for run '{}'",
						candidate.get(RUN_ID), e);
			}
		}
	}

	/**
	 * Claims the oldest eligible item for one loop and creates a new durable
	 * attempt. UNKNOWN effects exclude an item from blind retry.
	 */
	static ItemClaim claimOldestDurableItem(String runId, String loopNodeId) {
		return claimOldestDurableItem(runId, loopNodeId, false);
	}

	/**
	 * Claims the oldest eligible durable item across all runs parked for item
	 * execution. The scheduler database row lock and conditional status transition
	 * make concurrent hosts receive distinct attempts.
	 */
	static ItemClaim claimOldestDurableItem() {
		return claimOldestDurableItem(null, null, true);
	}

	private static ItemClaim claimOldestDurableItem(String runId, String loopNodeId, boolean sharedQueue) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("claiming a durable automation item");
		for (int attempt = 0; attempt < QUEUE_CLAIM_RETRIES; attempt++) {
			try {
				ItemClaim claim = QueryExecutionUtility.write(schedulerDb, conn -> {
				if (!sharedQueue) {
					fenceActiveLease(conn, runId);
				}
				Timestamp now = toTimestamp(Instant.now());
				String select = "SELECT I.ITEM_ID,I.ITEM_INDEX,I.ITEM_INPUT_SNAPSHOT,I.ITEM_REFERENCE_SNAPSHOT,"
						+ "I.ATTEMPT_NUMBER,I.MAX_ATTEMPTS FROM AUTOMATION_ITEMS I "
						+ (sharedQueue
								? "JOIN AUTOMATION_RUNS R ON R.RUN_ID=I.RUN_ID WHERE R.STATUS=? "
										+ "AND (R.CANCEL_REQUESTED IS NULL OR R.CANCEL_REQUESTED=?) "
								: "WHERE I.RUN_ID=? AND I.NODE_ID=? ")
						+ "AND I.STATUS IN (?,?) "
						+ "AND (I.NEXT_ATTEMPT_AT IS NULL OR I.NEXT_ATTEMPT_AT<=?) "
						+ "AND I.ATTEMPT_NUMBER<I.MAX_ATTEMPTS "
						+ "AND NOT EXISTS (SELECT 1 FROM AUTOMATION_EFFECTS E WHERE E.ITEM_ID=I.ITEM_ID "
						+ "AND E.EFFECT_STATE=?) "
						+ (sharedQueue ? "ORDER BY I.CREATED_AT,I.ITEM_INDEX,I.ITEM_ID "
								: "ORDER BY I.ITEM_INDEX,I.CREATED_AT ")
						+ "FOR UPDATE";
				try (PreparedStatement candidate = conn.prepareStatement(select)) {
					int p = 1;
					if (sharedQueue) {
						candidate.setString(p++, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
						candidate.setBoolean(p++, false);
					} else {
						candidate.setString(p++, runId);
						candidate.setString(p++, loopNodeId);
					}
					candidate.setString(p++, AutomationConstants.ITEM_STATUS_PENDING);
					candidate.setString(p++, AutomationConstants.ITEM_STATUS_RETRY);
					candidate.setTimestamp(p++, now);
					candidate.setString(p++, AutomationConstants.EFFECT_STATE_UNKNOWN);
					candidate.setMaxRows(1);
					try (ResultSet rs = candidate.executeQuery()) {
						if (!rs.next()) {
							return null;
						}
						String itemId = rs.getString(1);
						String claimedRunId = sharedQueue ? itemRunId(conn, itemId) : runId;
						String claimedLoopNodeId = sharedQueue ? itemNodeId(conn, itemId) : loopNodeId;
						long itemIndex = rs.getLong(2);
						String input = rs.getString(3);
						String reference = rs.getString(4);
						int attemptNumber = rs.getInt(5) + 1;
						int maxAttempts = rs.getInt(6);
						String attemptId = UUID.randomUUID().toString();
						String leaseToken = UUID.randomUUID().toString();
						try (PreparedStatement update = conn.prepareStatement(
								"UPDATE AUTOMATION_ITEMS SET STATUS=?,ATTEMPT_ID=?,ATTEMPT_NUMBER=?,LEASE_OWNER=?,"
										+ "LEASE_TOKEN=?,LEASE_EXPIRES_AT=?,STARTED_AT=?,COMPLETED_AT=NULL,ERROR_MESSAGE=NULL "
										+ "WHERE ITEM_ID=? AND STATUS IN (?,?)");
								PreparedStatement attemptInsert = conn.prepareStatement(
										"INSERT INTO AUTOMATION_ITEM_ATTEMPTS (ATTEMPT_ID,ITEM_ID,RUN_ID,ATTEMPT_NUMBER,"
												+ "STATUS,LEASE_OWNER,LEASE_TOKEN,STARTED_AT) VALUES (?,?,?,?,?,?,?,?)")) {
							p = 1;
							update.setString(p++, AutomationConstants.ITEM_STATUS_RUNNING);
							update.setString(p++, attemptId);
							update.setInt(p++, attemptNumber);
							update.setString(p++, LEASE_OWNER_ID);
							update.setString(p++, leaseToken);
							update.setTimestamp(p++, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
							update.setTimestamp(p++, now);
							update.setString(p++, itemId);
							update.setString(p++, AutomationConstants.ITEM_STATUS_PENDING);
							update.setString(p++, AutomationConstants.ITEM_STATUS_RETRY);
							if (update.executeUpdate() != 1) {
								return null;
							}
							p = 1;
							attemptInsert.setString(p++, attemptId);
							attemptInsert.setString(p++, itemId);
							attemptInsert.setString(p++, claimedRunId);
							attemptInsert.setInt(p++, attemptNumber);
							attemptInsert.setString(p++, AutomationConstants.ATTEMPT_STATUS_RUNNING);
							attemptInsert.setString(p++, LEASE_OWNER_ID);
							attemptInsert.setString(p++, leaseToken);
							attemptInsert.setTimestamp(p++, now);
							attemptInsert.executeUpdate();
						}
						return new ItemClaim(itemId, claimedRunId, claimedLoopNodeId, itemIndex, input, reference, attemptId,
								attemptNumber, maxAttempts, leaseToken);
					}
				}
				});
				if (claim != null || !sharedQueue) {
					return claim;
				}
			} catch (Exception e) {
				if (sharedQueue && isRetryableClaimConflict(e)) {
					continue;
				}
				throw new IllegalStateException("Unable to claim a durable Automation item.", e);
			}
		}
		return null;
	}

	private static String itemRunId(Connection conn, String itemId) throws SQLException {
		return itemIdentity(conn, itemId, RUN_ID);
	}

	private static String itemNodeId(Connection conn, String itemId) throws SQLException {
		return itemIdentity(conn, itemId, NODE_ID);
	}

	private static String itemIdentity(Connection conn, String itemId, String column) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT " + column + " FROM AUTOMATION_ITEMS WHERE ITEM_ID=?")) {
			ps.setString(1, itemId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					throw new IllegalStateException("Claimed durable Automation item disappeared.");
				}
				return rs.getString(1);
			}
		}
	}

	static void activateItemClaim(ItemClaim claim) {
		CLAIMED_ITEM.set(claim);
	}

	static void clearItemClaim(ItemClaim claim) {
		ItemClaim active = CLAIMED_ITEM.get();
		if (active != null && active.attemptId().equals(claim.attemptId())) {
			CLAIMED_ITEM.remove();
		}
	}

	static void completeDurableItem(ItemClaim claim) {
		updateDurableItemTerminal(claim, true, null);
	}

	static String failDurableItem(ItemClaim claim, String error) {
		return updateDurableItemTerminal(claim, false, error);
	}

	private static String updateDurableItemTerminal(ItemClaim claim, boolean success, String error) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("publishing a durable automation item attempt");
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, claim.runId());
				fenceItemLease(conn, claim);
				Timestamp now = toTimestamp(Instant.now());
				boolean unknown = hasUnknownEffect(conn, claim.itemId());
				boolean committedEffect = hasEffectState(conn, claim.itemId(),
						AutomationConstants.EFFECT_STATE_COMMITTED);
				String itemStatus;
				String attemptStatus;
				String itemError = error;
				Timestamp nextAttempt = null;
				if (success) {
					itemStatus = AutomationConstants.ITEM_STATUS_SUCCESS;
					attemptStatus = AutomationConstants.ATTEMPT_STATUS_SUCCESS;
				} else if (unknown) {
					itemStatus = AutomationConstants.ITEM_STATUS_BLOCKED;
					attemptStatus = AutomationConstants.ATTEMPT_STATUS_INTERRUPTED;
				} else if (committedEffect) {
					itemStatus = AutomationConstants.ITEM_STATUS_BLOCKED;
					attemptStatus = AutomationConstants.ATTEMPT_STATUS_FAILED;
					itemError = (error == null ? "" : error)
							+ " A prior effect was committed; automatic replay is blocked.";
				} else if (claim.attemptNumber() < claim.maxAttempts()) {
					itemStatus = AutomationConstants.ITEM_STATUS_RETRY;
					attemptStatus = AutomationConstants.ATTEMPT_STATUS_FAILED;
					nextAttempt = now;
				} else {
					itemStatus = AutomationConstants.ITEM_STATUS_FAILED;
					attemptStatus = AutomationConstants.ATTEMPT_STATUS_FAILED;
				}
				try (PreparedStatement item = conn.prepareStatement(
						"UPDATE AUTOMATION_ITEMS SET STATUS=?,LEASE_OWNER=NULL,LEASE_TOKEN=NULL,LEASE_EXPIRES_AT=NULL,"
								+ "NEXT_ATTEMPT_AT=?,COMPLETED_AT=?,ERROR_MESSAGE=? WHERE ITEM_ID=? AND ATTEMPT_ID=? "
								+ "AND LEASE_TOKEN=?");
						PreparedStatement attempt = conn.prepareStatement(
								"UPDATE AUTOMATION_ITEM_ATTEMPTS SET STATUS=?,COMPLETED_AT=?,ERROR_MESSAGE=? "
										+ "WHERE ATTEMPT_ID=? AND LEASE_TOKEN=?")) {
					int p = 1;
					item.setString(p++, itemStatus);
					item.setTimestamp(p++, nextAttempt);
					item.setTimestamp(p++, AutomationConstants.ITEM_STATUS_RETRY.equals(itemStatus) ? null : now);
					schedulerDb.getQueryUtil().setNullableLargeText(item, p++, itemError);
					item.setString(p++, claim.itemId());
					item.setString(p++, claim.attemptId());
					item.setString(p++, claim.leaseToken());
					requireSingleRow(item.executeUpdate(), "publish the durable item", claim.runId(), claim.nodeId());
					p = 1;
					attempt.setString(p++, attemptStatus);
					attempt.setTimestamp(p++, now);
					schedulerDb.getQueryUtil().setNullableLargeText(attempt, p++, itemError);
					attempt.setString(p++, claim.attemptId());
					attempt.setString(p++, claim.leaseToken());
					requireSingleRow(attempt.executeUpdate(), "publish the durable item attempt", claim.runId(),
							claim.nodeId());
				}
				return itemStatus;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to publish the durable Automation item attempt.", e);
		}
	}

	private static void fenceItemLease(Connection conn, ItemClaim claim) throws SQLException {
		try (PreparedStatement fence = conn.prepareStatement(
				"UPDATE AUTOMATION_ITEMS SET LEASE_TOKEN=LEASE_TOKEN WHERE ITEM_ID=? AND STATUS=? "
						+ "AND ATTEMPT_ID=? AND LEASE_OWNER=? AND LEASE_TOKEN=? AND LEASE_EXPIRES_AT>?")) {
			fence.setString(1, claim.itemId());
			fence.setString(2, AutomationConstants.ITEM_STATUS_RUNNING);
			fence.setString(3, claim.attemptId());
			fence.setString(4, LEASE_OWNER_ID);
			fence.setString(5, claim.leaseToken());
			fence.setTimestamp(6, toTimestamp(Instant.now()));
			requireSingleRow(fence.executeUpdate(), "fence the durable item lease", claim.runId(), claim.nodeId());
		}
	}

	private static boolean hasUnknownEffect(Connection conn, String itemId) throws SQLException {
		return hasEffectState(conn, itemId, AutomationConstants.EFFECT_STATE_UNKNOWN);
	}

	private static boolean hasEffectState(Connection conn, String itemId, String effectState) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT 1 FROM AUTOMATION_EFFECTS WHERE ITEM_ID=? AND EFFECT_STATE=?")) {
			ps.setString(1, itemId);
			ps.setString(2, effectState);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next();
			}
		}
	}

	private static void markUncommittedEffectsUnknown(Connection conn, String runId, String attemptId, String error)
			throws SQLException {
		String sql = "UPDATE AUTOMATION_EFFECTS SET EFFECT_STATE=?,ERROR_MESSAGE=? WHERE RUN_ID=? "
				+ (attemptId == null ? "" : "AND ATTEMPT_ID=? ")
				+ "AND EFFECT_STATE IN (?,?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			int p = 1;
			ps.setString(p++, AutomationConstants.EFFECT_STATE_UNKNOWN);
			ps.setString(p++, error);
			ps.setString(p++, runId);
			if (attemptId != null) {
				ps.setString(p++, attemptId);
			}
			ps.setString(p++, AutomationConstants.EFFECT_STATE_PREPARED);
			ps.setString(p, AutomationConstants.EFFECT_STATE_ACCEPTED);
			ps.executeUpdate();
		}
	}

	/** Persists PREPARED before invoking a direct effectful adapter. */
	static EffectInvocation prepareEffect(String runId, String nodeId) {
		return prepareEffect(runId, nodeId, nodeId);
	}

	static EffectInvocation prepareEffect(String runId, String nodeId, String operationNodeId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("preparing an automation effect");
		ItemClaim item = claimedItem(runId);
		String effectId = UUID.randomUUID().toString();
		try {
			String operationKey = QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				if (item != null) {
					fenceItemLease(conn, item);
				}
				String stableOperationKey = item == null ? UUID.randomUUID().toString()
						: UUID.nameUUIDFromBytes((item.itemId() + "\n" + operationNodeId)
								.getBytes(StandardCharsets.UTF_8)).toString();
				try (PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO AUTOMATION_EFFECTS (EFFECT_ID,RUN_ID,NODE_ID,ITEM_ID,ATTEMPT_ID,EFFECT_STATE,"
								+ "OPERATION_KEY,PREPARED_AT) VALUES (?,?,?,?,?,?,?,?)")) {
					int p = 1;
					ps.setString(p++, effectId);
					ps.setString(p++, runId);
					ps.setString(p++, nodeId);
					schedulerDb.getQueryUtil().setNullableString(ps, p++, item == null ? null : item.itemId());
					schedulerDb.getQueryUtil().setNullableString(ps, p++, item == null ? null : item.attemptId());
					ps.setString(p++, AutomationConstants.EFFECT_STATE_PREPARED);
					ps.setString(p++, stableOperationKey);
					ps.setTimestamp(p++, toTimestamp(Instant.now()));
					ps.executeUpdate();
				}
				return stableOperationKey;
			});
			return new EffectInvocation(effectId, operationKey);
		} catch (Exception e) {
			throw new IllegalStateException("Unable to prepare the Automation effect.", e);
		}
	}

	static void markEffectAccepted(String effectId, String idempotencyKey, String receiptMetadata) {
		String bounded = receiptMetadata == null ? null
				: AutomationRuntimeUtils.toBoundedRuntimeJson(
						AutomationRuntimeUtils.GSON.fromJson(receiptMetadata, Object.class),
						AutomationConstants.EFFECT_RECEIPT_METADATA_MAX_BYTES, "Automation effect receipt metadata");
		String receiptHash = bounded == null ? null : sha256(bounded.getBytes(StandardCharsets.UTF_8));
		updateEffectState(effectId, AutomationConstants.EFFECT_STATE_ACCEPTED, idempotencyKey, receiptHash, bounded,
				null);
	}

	static void markEffectUnknown(String effectId, String error) {
		if (effectId == null) {
			return;
		}
		IRDBMSEngine schedulerDb = requireSchedulerDb("marking an automation effect unknown");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(
						"UPDATE AUTOMATION_EFFECTS SET EFFECT_STATE=?,ERROR_MESSAGE=? WHERE EFFECT_ID=? "
								+ "AND EFFECT_STATE IN (?,?)")) {
					ps.setString(1, AutomationConstants.EFFECT_STATE_UNKNOWN);
					schedulerDb.getQueryUtil().setNullableLargeText(ps, 2, error);
					ps.setString(3, effectId);
					ps.setString(4, AutomationConstants.EFFECT_STATE_PREPARED);
					ps.setString(5, AutomationConstants.EFFECT_STATE_ACCEPTED);
					ps.executeUpdate();
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to mark Automation effect '{}' UNKNOWN", effectId, e);
		}
	}

	private static void updateEffectState(String effectId, String state, String idempotencyKey, String receiptHash,
			String receiptMetadata, String error) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("updating an automation effect");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(
						"UPDATE AUTOMATION_EFFECTS SET EFFECT_STATE=?,IDEMPOTENCY_KEY=?,RECEIPT_HASH=?,"
								+ "RECEIPT_METADATA=?,ACCEPTED_AT=?,ERROR_MESSAGE=? WHERE EFFECT_ID=? "
								+ "AND EFFECT_STATE IN (?,?)")) {
					int p = 1;
					ps.setString(p++, state);
					schedulerDb.getQueryUtil().setNullableString(ps, p++, idempotencyKey);
					schedulerDb.getQueryUtil().setNullableString(ps, p++, receiptHash);
					schedulerDb.getQueryUtil().setNullableLargeText(ps, p++, receiptMetadata);
					ps.setTimestamp(p++, AutomationConstants.EFFECT_STATE_ACCEPTED.equals(state)
							? toTimestamp(Instant.now()) : null);
					schedulerDb.getQueryUtil().setNullableLargeText(ps, p++, error);
					ps.setString(p++, effectId);
					ps.setString(p++, AutomationConstants.EFFECT_STATE_PREPARED);
					ps.setString(p++, AutomationConstants.EFFECT_STATE_ACCEPTED);
					requireSingleRow(ps.executeUpdate(), "update the automation effect", null, null);
				}
				return null;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to update the Automation effect.", e);
		}
	}

	/**
	 * Atomically transitions one submitted run to running.
	 *
	 * @param runId run to claim
	 * @return {@code true} when this caller claimed the run; {@code false} when it
	 *         was already claimed or reached another state
	 */
	public static boolean claimRun(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("claiming the submitted automation run");
		Timestamp now = toTimestamp(Instant.now());
		String attemptId = UUID.randomUUID().toString();
		String leaseToken = UUID.randomUUID().toString();

		try {
			boolean claimed = QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(CLAIM_RUN)) {
					int index = 1;
					ps.setString(index++, STATUS_RUNNING);
					ps.setTimestamp(index++, now);
					ps.setTimestamp(index++, now);
					ps.setString(index++, attemptId);
					ps.setString(index++, LEASE_OWNER_ID);
					ps.setString(index++, leaseToken);
					ps.setTimestamp(index++, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
					ps.setString(index++, runId);
					ps.setString(index++, STATUS_SUBMITTED);
					return ps.executeUpdate() == 1;
				}
			});
			if (claimed) {
				CLAIMED_LEASES.get().put(runId, leaseToken);
				ACTIVE_RUN_LEASES.put(runId, leaseToken);
			}
			return claimed;
		} catch (Exception e) {
			classLogger.error("Failed to claim submitted automation run '{}'", runId, e);
			throw new IllegalStateException("Unable to claim the submitted automation run.", e);
		}
	}

	static ClaimedRun claimOldestSubmittedRun() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return null;
		}
		for (int attempt = 0; attempt < QUEUE_CLAIM_RETRIES; attempt++) {
			Timestamp claimedAt = toTimestamp(Instant.now());
			String attemptId = UUID.randomUUID().toString();
			String leaseToken = UUID.randomUUID().toString();
			try {
				QueueClaimAttempt claimAttempt = QueryExecutionUtility.write(schedulerDb, conn -> {
					Map<String, Object> admission = selectOldestSubmittedRun(conn);
					if (admission == null) {
						return new QueueClaimAttempt(false, null);
					}
					try (PreparedStatement ps = conn.prepareStatement(CLAIM_RUN)) {
						int index = 1;
						ps.setString(index++, STATUS_RUNNING);
						ps.setTimestamp(index++, claimedAt);
						ps.setTimestamp(index++, claimedAt);
						ps.setString(index++, attemptId);
						ps.setString(index++, LEASE_OWNER_ID);
						ps.setString(index++, leaseToken);
						ps.setTimestamp(index++, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
						ps.setString(index++, String.valueOf(admission.get(RUN_ID)));
						ps.setString(index++, STATUS_SUBMITTED);
						ClaimedRun claimed = ps.executeUpdate() == 1
								? new ClaimedRun(admission, leaseToken)
								: null;
						return new QueueClaimAttempt(true, claimed);
					}
				});
				if (!claimAttempt.candidateFound()) {
					return null;
				}
				if (claimAttempt.claimedRun() != null) {
					return claimAttempt.claimedRun();
				}
			} catch (Exception e) {
				if (isRetryableClaimConflict(e)) {
					continue;
				}
				throw new IllegalStateException("Unable to claim the oldest Automation admission.", e);
			}
		}
		classLogger.debug("Automation queue claim conflicted {} times; deferring to the next poll",
				QUEUE_CLAIM_RETRIES);
		return null;
	}

	private static boolean isRetryableClaimConflict(Throwable error) {
		Throwable current = error;
		while (current != null) {
			if (current instanceof SQLTransientException) {
				return true;
			}
			if (current instanceof SQLException sqlException
					&& sqlException.getSQLState() != null
					&& sqlException.getSQLState().startsWith("40")) {
				return true;
			}
			current = current.getCause();
		}
		return false;
	}

	private static Map<String, Object> selectOldestSubmittedRun(Connection conn) throws SQLException {
		String sql = "SELECT RUN_ID, PROJECT_ID, DEFINITION_SNAPSHOT, PRINCIPAL_ACCESS, STARTED_AT "
				+ "FROM AUTOMATION_RUNS WHERE STATUS = ? AND ASYNC_ADMISSION = ? "
				+ "AND (CANCEL_REQUESTED IS NULL OR CANCEL_REQUESTED = ?) ORDER BY STARTED_AT ASC, RUN_ID ASC";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, STATUS_SUBMITTED);
			ps.setBoolean(2, true);
			ps.setBoolean(3, false);
			ps.setMaxRows(1);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				Map<String, Object> row = new LinkedHashMap<>();
				row.put(RUN_ID, rs.getString(RUN_ID));
				row.put(PROJECT_ID, rs.getString(PROJECT_ID));
				row.put(DEFINITION_SNAPSHOT, rs.getString(DEFINITION_SNAPSHOT));
				row.put(AutomationConstants.PRINCIPAL_ACCESS, rs.getString(AutomationConstants.PRINCIPAL_ACCESS));
				row.put(STARTED_AT, rs.getTimestamp(STARTED_AT));
				return row;
			}
		}
	}

	static Map<String, Object> queueMetrics() {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return Map.of("submitted", 0L, "running", 0L);
		}
		Connection conn = null;
		long submitted = 0;
		long running = 0;
		try {
			conn = schedulerDb.getConnection();
			try (PreparedStatement ps = conn.prepareStatement(
					"SELECT STATUS, COUNT(*) AS STATUS_COUNT FROM AUTOMATION_RUNS "
							+ "WHERE STATUS IN (?, ?) GROUP BY STATUS")) {
				ps.setString(1, STATUS_SUBMITTED);
				ps.setString(2, STATUS_RUNNING);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						if (STATUS_SUBMITTED.equals(rs.getString(STATUS))) {
							submitted = rs.getLong("STATUS_COUNT");
						} else if (STATUS_RUNNING.equals(rs.getString(STATUS))) {
							running = rs.getLong("STATUS_COUNT");
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to collect Automation queue metrics.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
		return Map.of("submitted", submitted, "running", running);
	}

	static Map<String, Object> operationalMetrics(String projectId) {
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Must provide a project id for Automation operational metrics.");
		}
		IRDBMSEngine schedulerDb = requireSchedulerDb("collecting Automation operational metrics");
		Connection conn = null;
		Instant collectedAt = Instant.now();
		try {
			conn = schedulerDb.getConnection();
			Map<String, Long> runCounts = collectGroupedCounts(conn,
					"SELECT STATUS, COUNT(*) AS METRIC_COUNT FROM AUTOMATION_RUNS "
							+ "WHERE PROJECT_ID = ? GROUP BY STATUS",
					projectId);
			Map<String, Long> itemCounts = collectGroupedCounts(conn,
					"SELECT I.STATUS, COUNT(*) AS METRIC_COUNT FROM AUTOMATION_ITEMS I "
							+ "INNER JOIN AUTOMATION_RUNS R ON R.RUN_ID = I.RUN_ID "
							+ "WHERE R.PROJECT_ID = ? GROUP BY I.STATUS",
					projectId);
			Map<String, Long> effectCounts = collectGroupedCounts(conn,
					"SELECT E.EFFECT_STATE, COUNT(*) AS METRIC_COUNT FROM AUTOMATION_EFFECTS E "
							+ "INNER JOIN AUTOMATION_RUNS R ON R.RUN_ID = E.RUN_ID "
							+ "WHERE R.PROJECT_ID = ? GROUP BY E.EFFECT_STATE",
					projectId);
			LeaseMetrics runLeases = collectLeaseMetrics(conn,
					"SELECT "
							+ "SUM(CASE WHEN LEASE_TOKEN IS NOT NULL AND LEASE_EXPIRES_AT > ? THEN 1 ELSE 0 END) "
							+ "AS FRESH_COUNT, "
							+ "SUM(CASE WHEN LEASE_TOKEN IS NOT NULL AND LEASE_EXPIRES_AT <= ? THEN 1 ELSE 0 END) "
							+ "AS EXPIRED_COUNT FROM AUTOMATION_RUNS WHERE PROJECT_ID = ?",
					projectId, collectedAt);
			LeaseMetrics itemLeases = collectLeaseMetrics(conn,
					"SELECT "
							+ "SUM(CASE WHEN I.LEASE_TOKEN IS NOT NULL AND I.LEASE_EXPIRES_AT > ? THEN 1 ELSE 0 END) "
							+ "AS FRESH_COUNT, "
							+ "SUM(CASE WHEN I.LEASE_TOKEN IS NOT NULL AND I.LEASE_EXPIRES_AT <= ? THEN 1 ELSE 0 END) "
							+ "AS EXPIRED_COUNT FROM AUTOMATION_ITEMS I "
							+ "INNER JOIN AUTOMATION_RUNS R ON R.RUN_ID = I.RUN_ID WHERE R.PROJECT_ID = ?",
					projectId, collectedAt);
			Map<String, PayloadMetrics> payloadMetrics = collectPayloadMetrics(conn, projectId);

			Map<String, Object> metrics = new LinkedHashMap<>();
			metrics.put("projectId", projectId);
			metrics.put("collectedAt", collectedAt.toString());
			metrics.put("runs", runMetrics(runCounts, oldestSubmittedAt(conn, projectId), collectedAt));
			metrics.put("leases", leaseMetrics(runLeases, itemLeases));
			metrics.put("durableItems", durableItemMetrics(itemCounts));
			metrics.put("effects", effectMetrics(effectCounts));
			metrics.put("retention", retentionMetrics(payloadMetrics));
			metrics.put("limits", configuredOperationalLimits());
			return metrics;
		} catch (SQLException e) {
			throw new IllegalStateException(
					"Unable to collect Automation operational metrics for project '" + projectId + "'.", e);
		} finally {
			closeConnection(schedulerDb, conn);
		}
	}

	private static Map<String, Long> collectGroupedCounts(Connection conn, String sql, String projectId)
			throws SQLException {
		Map<String, Long> counts = new HashMap<>();
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, projectId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					String status = rs.getString(1);
					counts.put(status, rs.getLong("METRIC_COUNT"));
				}
			}
		}
		return counts;
	}

	private static LeaseMetrics collectLeaseMetrics(Connection conn, String sql, String projectId, Instant collectedAt)
			throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			Timestamp now = toTimestamp(collectedAt);
			ps.setTimestamp(1, now);
			ps.setTimestamp(2, now);
			ps.setString(3, projectId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return new LeaseMetrics(0, 0);
				}
				return new LeaseMetrics(rs.getLong("FRESH_COUNT"), rs.getLong("EXPIRED_COUNT"));
			}
		}
	}

	private static Timestamp oldestSubmittedAt(Connection conn, String projectId) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT MIN(STARTED_AT) AS OLDEST_SUBMITTED_AT FROM AUTOMATION_RUNS "
						+ "WHERE PROJECT_ID = ? AND STATUS = ?")) {
			ps.setString(1, projectId);
			ps.setString(2, STATUS_SUBMITTED);
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() ? rs.getTimestamp("OLDEST_SUBMITTED_AT") : null;
			}
		}
	}

	private static Map<String, PayloadMetrics> collectPayloadMetrics(Connection conn, String projectId)
			throws SQLException {
		Map<String, PayloadMetrics> metrics = new HashMap<>();
		try (PreparedStatement ps = conn.prepareStatement(
				"SELECT CAPTURE_STATE, COUNT(*) AS PAYLOAD_COUNT, COALESCE(SUM(BYTE_COUNT), 0) AS PAYLOAD_BYTES "
						+ "FROM AUTOMATION_SAVED_RESULTS WHERE PROJECT_ID = ? GROUP BY CAPTURE_STATE")) {
			ps.setString(1, projectId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					metrics.put(rs.getString(AutomationConstants.CAPTURE_STATE),
							new PayloadMetrics(rs.getLong("PAYLOAD_COUNT"), rs.getLong("PAYLOAD_BYTES")));
				}
			}
		}
		return metrics;
	}

	private static Map<String, Object> runMetrics(Map<String, Long> counts, Timestamp oldestSubmitted,
			Instant collectedAt) {
		Map<String, Object> metrics = new LinkedHashMap<>();
		metrics.put("submitted", count(counts, STATUS_SUBMITTED));
		metrics.put("running", count(counts, STATUS_RUNNING));
		metrics.put("waiting", count(counts, AutomationConstants.STATUS_WAITING_FOR_INPUT)
				+ count(counts, AutomationConstants.STATUS_WAITING_FOR_ITEMS));
		metrics.put("interrupted", count(counts, STATUS_INTERRUPTED));
		metrics.put("cancelled", count(counts, AutomationConstants.STATUS_CANCELLED));
		metrics.put("failed", count(counts, AutomationConstants.STATUS_FAILED));
		metrics.put("success", count(counts, AutomationConstants.STATUS_SUCCESS));
		metrics.put("blocked", count(counts, AutomationConstants.STATUS_BLOCKED));
		metrics.put("unknown", unknownCount(counts, Set.of(STATUS_SUBMITTED, STATUS_RUNNING,
				AutomationConstants.STATUS_WAITING_FOR_INPUT, AutomationConstants.STATUS_WAITING_FOR_ITEMS,
				STATUS_INTERRUPTED, AutomationConstants.STATUS_CANCELLED, AutomationConstants.STATUS_FAILED,
				AutomationConstants.STATUS_SUCCESS, AutomationConstants.STATUS_BLOCKED)));
		long oldestQueueAgeMs = oldestSubmitted == null ? 0
				: Math.max(0, collectedAt.toEpochMilli() - oldestSubmitted.getTime());
		metrics.put("oldestQueueAgeMs", oldestQueueAgeMs);
		return metrics;
	}

	private static Map<String, Object> leaseMetrics(LeaseMetrics runLeases, LeaseMetrics itemLeases) {
		Map<String, Object> metrics = new LinkedHashMap<>();
		metrics.put("fresh", runLeases.fresh() + itemLeases.fresh());
		metrics.put("expired", runLeases.expired() + itemLeases.expired());
		metrics.put("runFresh", runLeases.fresh());
		metrics.put("runExpired", runLeases.expired());
		metrics.put("itemFresh", itemLeases.fresh());
		metrics.put("itemExpired", itemLeases.expired());
		return metrics;
	}

	private static Map<String, Object> durableItemMetrics(Map<String, Long> counts) {
		Map<String, Object> metrics = new LinkedHashMap<>();
		metrics.put("pending", count(counts, AutomationConstants.ITEM_STATUS_PENDING));
		metrics.put("running", count(counts, AutomationConstants.ITEM_STATUS_RUNNING));
		metrics.put("retry", count(counts, AutomationConstants.ITEM_STATUS_RETRY));
		metrics.put("success", count(counts, AutomationConstants.ITEM_STATUS_SUCCESS));
		metrics.put("failed", count(counts, AutomationConstants.ITEM_STATUS_FAILED));
		metrics.put("cancelled", count(counts, AutomationConstants.ITEM_STATUS_CANCELLED));
		metrics.put("blocked", count(counts, AutomationConstants.ITEM_STATUS_BLOCKED));
		metrics.put("unknown", unknownCount(counts, Set.of(AutomationConstants.ITEM_STATUS_PENDING,
				AutomationConstants.ITEM_STATUS_RUNNING, AutomationConstants.ITEM_STATUS_RETRY,
				AutomationConstants.ITEM_STATUS_SUCCESS, AutomationConstants.ITEM_STATUS_FAILED,
				AutomationConstants.ITEM_STATUS_CANCELLED, AutomationConstants.ITEM_STATUS_BLOCKED)));
		return metrics;
	}

	private static Map<String, Object> effectMetrics(Map<String, Long> counts) {
		Map<String, Object> metrics = new LinkedHashMap<>();
		metrics.put("prepared", count(counts, AutomationConstants.EFFECT_STATE_PREPARED));
		metrics.put("accepted", count(counts, AutomationConstants.EFFECT_STATE_ACCEPTED));
		metrics.put("committed", count(counts, AutomationConstants.EFFECT_STATE_COMMITTED));
		metrics.put("unknown", count(counts, AutomationConstants.EFFECT_STATE_UNKNOWN)
				+ unknownCount(counts, Set.of(AutomationConstants.EFFECT_STATE_PREPARED,
						AutomationConstants.EFFECT_STATE_ACCEPTED, AutomationConstants.EFFECT_STATE_COMMITTED,
						AutomationConstants.EFFECT_STATE_UNKNOWN)));
		return metrics;
	}

	private static Map<String, Object> retentionMetrics(Map<String, PayloadMetrics> payloadMetrics) {
		PayloadMetrics retained = payloadMetrics.getOrDefault(AutomationConstants.CAPTURE_STATE_RETAINED,
				new PayloadMetrics(0, 0));
		PayloadMetrics expired = payloadMetrics.getOrDefault(AutomationConstants.CAPTURE_STATE_EXPIRED,
				new PayloadMetrics(0, 0));
		Map<String, Object> metrics = new LinkedHashMap<>();
		metrics.put("retainedPayloads", retained.count());
		metrics.put("retainedBytes", retained.bytes());
		metrics.put("expiredPayloads", expired.count());
		metrics.put("expiredBytes", expired.bytes());
		return metrics;
	}

	private static Map<String, Object> configuredOperationalLimits() {
		Map<String, Object> limits = new LinkedHashMap<>();
		limits.put("globalAdmissionQuota", positiveProperty("semoss.automation.admission.global",
				AutomationConstants.DEFAULT_GLOBAL_ADMISSION_LIMIT));
		limits.put("projectAdmissionQuota", positiveProperty("semoss.automation.admission.project",
				AutomationConstants.DEFAULT_PROJECT_ADMISSION_LIMIT));
		limits.put("runWorkerConcurrency", positiveProperty("semoss.automation.worker.concurrency",
				AutomationConstants.DEFAULT_WORKER_CONCURRENCY));
		limits.put("itemWorkerConcurrency", positiveProperty("semoss.automation.itemWorker.concurrency",
				AutomationConstants.DEFAULT_ITEM_WORKER_CONCURRENCY));
		limits.put("retentionDays", AutomationConstants.SAVED_RESULT_RETENTION_DAYS);
		limits.put("pageDefaultRows", AutomationConstants.SAVED_RESULT_PAGE_DEFAULT_ROWS);
		limits.put("pageMaxRows", AutomationConstants.SAVED_RESULT_PAGE_MAX_ROWS);
		limits.put("pageMaxBytes", AutomationConstants.SAVED_RESULT_PAGE_MAX_BYTES);
		limits.put("openMaxRows", AutomationConstants.SAVED_RESULT_OPEN_MAX_ROWS);
		limits.put("openMaxBytes", AutomationConstants.SAVED_RESULT_OPEN_MAX_BYTES);
		return limits;
	}

	private static long count(Map<String, Long> counts, String status) {
		return counts.getOrDefault(status, 0L);
	}

	private static long unknownCount(Map<String, Long> counts, Set<String> knownStatuses) {
		return counts.entrySet().stream()
				.filter(entry -> entry.getKey() == null || !knownStatuses.contains(entry.getKey()))
				.mapToLong(Map.Entry::getValue)
				.sum();
	}

	private record LeaseMetrics(long fresh, long expired) {
	}

	private record PayloadMetrics(long count, long bytes) {
	}

	static CancellationResult cancelQueuedOrRunningRun(String runId, String projectId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("cancelling the queued or running automation run");
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(CANCEL_SUBMITTED_RUN)) {
					ps.setString(1, AutomationConstants.STATUS_CANCELLED);
					ps.setBoolean(2, true);
					ps.setTimestamp(3, toTimestamp(Instant.now()));
					ps.setString(4, "Cancelled before worker claim");
					ps.setString(5, runId);
					ps.setString(6, projectId);
					ps.setString(7, STATUS_SUBMITTED);
					if (ps.executeUpdate() == 1) {
						cancelDurableItems(conn, schedulerDb.getQueryUtil(), runId);
						return new CancellationResult(AutomationConstants.STATUS_CANCELLED, false);
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(CANCEL_RUNNING_RUN)) {
					ps.setBoolean(1, true);
					ps.setString(2, runId);
					ps.setString(3, projectId);
					ps.setString(4, STATUS_RUNNING);
					if (ps.executeUpdate() == 1) {
						cancelDurableItems(conn, schedulerDb.getQueryUtil(), runId);
						return new CancellationResult(STATUS_RUNNING, true);
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(CANCEL_WAITING_ITEMS_RUN)) {
					ps.setString(1, AutomationConstants.STATUS_CANCELLED);
					ps.setBoolean(2, true);
					ps.setTimestamp(3, toTimestamp(Instant.now()));
					ps.setString(4, "Cancelled while durable items were queued or active");
					ps.setString(5, runId);
					ps.setString(6, projectId);
					ps.setString(7, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
					if (ps.executeUpdate() == 1) {
						cancelDurableItems(conn, schedulerDb.getQueryUtil(), runId);
						return new CancellationResult(AutomationConstants.STATUS_CANCELLED, true);
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(
						"SELECT STATUS FROM AUTOMATION_RUNS WHERE RUN_ID = ? AND PROJECT_ID = ?")) {
					ps.setString(1, runId);
					ps.setString(2, projectId);
					try (ResultSet rs = ps.executeQuery()) {
						return new CancellationResult(rs.next() ? rs.getString(STATUS) : null, false);
					}
				}

			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to cancel the queued or running Automation run.", e);
		}
	}

	private static void cancelDurableItems(Connection conn, AbstractSqlQueryUtil queryUtil, String runId)
			throws SQLException {
		Timestamp now = toTimestamp(Instant.now());
		try (PreparedStatement effects = conn.prepareStatement(
				"UPDATE AUTOMATION_EFFECTS SET EFFECT_STATE=?,ERROR_MESSAGE=? WHERE RUN_ID=? "
						+ "AND ATTEMPT_ID IN (SELECT ATTEMPT_ID FROM AUTOMATION_ITEM_ATTEMPTS "
						+ "WHERE RUN_ID=? AND STATUS=?) AND EFFECT_STATE IN (?,?)");
				PreparedStatement attempts = conn.prepareStatement(
				"UPDATE AUTOMATION_ITEM_ATTEMPTS SET STATUS=?,COMPLETED_AT=?,ERROR_MESSAGE=? WHERE RUN_ID=? "
						+ "AND STATUS=?");
				PreparedStatement items = conn.prepareStatement(
						"UPDATE AUTOMATION_ITEMS SET STATUS=?,COMPLETED_AT=?,LEASE_OWNER=NULL,LEASE_TOKEN=NULL,"
								+ "LEASE_EXPIRES_AT=NULL,ERROR_MESSAGE=? WHERE RUN_ID=? AND STATUS IN (?,?,?)");
				PreparedStatement loop = conn.prepareStatement(
						"UPDATE AUTOMATION_NODE_OUTPUTS SET STATUS=?,COMPLETED_AT=?,ERROR_MESSAGE=? "
								+ "WHERE RUN_ID=? AND STATUS=? AND (NODE_ID IN "
								+ "(SELECT DISTINCT NODE_ID FROM AUTOMATION_ITEMS WHERE RUN_ID=?) OR PARENT_NODE_ID IN "
								+ "(SELECT DISTINCT NODE_ID FROM AUTOMATION_ITEMS WHERE RUN_ID=?))")) {
			effects.setString(1, AutomationConstants.EFFECT_STATE_UNKNOWN);
			queryUtil.setNullableLargeText(effects, 2, "Cancelled with owning Automation run");
			effects.setString(3, runId);
			effects.setString(4, runId);
			effects.setString(5, AutomationConstants.ATTEMPT_STATUS_RUNNING);
			effects.setString(6, AutomationConstants.EFFECT_STATE_PREPARED);
			effects.setString(7, AutomationConstants.EFFECT_STATE_ACCEPTED);
			effects.executeUpdate();
			attempts.setString(1, AutomationConstants.ATTEMPT_STATUS_CANCELLED);
			attempts.setTimestamp(2, now);
			queryUtil.setNullableLargeText(attempts, 3, "Cancelled with owning Automation run");
			attempts.setString(4, runId);
			attempts.setString(5, AutomationConstants.ATTEMPT_STATUS_RUNNING);
			attempts.executeUpdate();
			items.setString(1, AutomationConstants.ITEM_STATUS_CANCELLED);
			items.setTimestamp(2, now);
			queryUtil.setNullableLargeText(items, 3, "Cancelled with owning Automation run");
			items.setString(4, runId);
			items.setString(5, AutomationConstants.ITEM_STATUS_PENDING);
			items.setString(6, AutomationConstants.ITEM_STATUS_RETRY);
			items.setString(7, AutomationConstants.ITEM_STATUS_RUNNING);
			items.executeUpdate();
			loop.setString(1, NODE_STATUS_FAILED);
			loop.setTimestamp(2, now);
			queryUtil.setNullableLargeText(loop, 3, "Cancelled with owning Automation run");
			loop.setString(4, runId);
			loop.setString(5, NODE_STATUS_RUNNING);
			loop.setString(6, runId);
			loop.setString(7, runId);
			loop.executeUpdate();
		}
	}

	static boolean activateClaimedLease(ClaimedRun claimedRun) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("activating the claimed automation lease");
		try {
			boolean active = QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(FENCE_ACTIVE_LEASE)) {
					ps.setString(1, claimedRun.runId());
					ps.setString(2, STATUS_RUNNING);
					ps.setString(3, claimedRun.leaseToken());
					return ps.executeUpdate() == 1;
				}
			});
			if (active) {
				CLAIMED_LEASES.get().put(claimedRun.runId(), claimedRun.leaseToken());
				ACTIVE_RUN_LEASES.put(claimedRun.runId(), claimedRun.leaseToken());
			}
			return active;
		} catch (Exception e) {
			throw new IllegalStateException("Unable to activate the claimed Automation lease.", e);
		}
	}

	static void clearClaimedLease(ClaimedRun claimedRun) {
		CLAIMED_LEASES.get().remove(claimedRun.runId(), claimedRun.leaseToken());
		ACTIVE_RUN_LEASES.remove(claimedRun.runId(), claimedRun.leaseToken());
	}

	static void reconcileRejectedClaim(ClaimedRun claimedRun) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("releasing the rejected automation claim");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				Timestamp now = toTimestamp(Instant.now());
				try (PreparedStatement ps = conn.prepareStatement(CANCEL_CLAIMED_RUN)) {
					ps.setString(1, AutomationConstants.STATUS_CANCELLED);
					ps.setTimestamp(2, now);
					ps.setString(3, "Cancelled before worker execution");
					ps.setString(4, claimedRun.runId());
					ps.setString(5, STATUS_RUNNING);
					ps.setString(6, claimedRun.leaseToken());
					ps.setBoolean(7, true);
					if (ps.executeUpdate() == 1) {
						return null;
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(RELEASE_CLAIMED_RUN)) {
					Timestamp submittedAt = (Timestamp) claimedRun.admission().get(STARTED_AT);
					ps.setString(1, STATUS_SUBMITTED);
					ps.setTimestamp(2, submittedAt);
					ps.setTimestamp(3, now);
					ps.setString(4, claimedRun.runId());
					ps.setString(5, STATUS_RUNNING);
					ps.setString(6, claimedRun.leaseToken());
					ps.setBoolean(7, false);
					ps.executeUpdate();
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to reconcile rejected Automation claim '{}'", claimedRun.runId(), e);
		}
	}

	static void failClaimedRun(ClaimedRun claimedRun, String message) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("failing the claimed automation run");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(CANCEL_CLAIMED_RUN)) {
					ps.setString(1, AutomationConstants.STATUS_CANCELLED);
					ps.setTimestamp(2, toTimestamp(Instant.now()));
					ps.setString(3, "Cancelled before worker execution");
					ps.setString(4, claimedRun.runId());
					ps.setString(5, STATUS_RUNNING);
					ps.setString(6, claimedRun.leaseToken());
					ps.setBoolean(7, true);
					if (ps.executeUpdate() == 1) {
						return null;
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(FAIL_CLAIMED_RUN)) {
					ps.setString(1, AutomationConstants.STATUS_FAILED);
					ps.setTimestamp(2, toTimestamp(Instant.now()));
					ps.setString(3, message);
					ps.setString(4, claimedRun.runId());
					ps.setString(5, STATUS_RUNNING);
					ps.setString(6, claimedRun.leaseToken());
					ps.setBoolean(7, false);
					ps.executeUpdate();
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to fail claimed Automation run '{}'", claimedRun.runId(), e);
		}
	}

	/**
	 * Persists an agent input boundary and releases the Automation run from its
	 * executing worker. Agent action arguments remain authoritative in the
	 * model-inference agent tables.
	 *
	 * @param runId         Automation run identifier
	 * @param projectId     owning Automation project
	 * @param nodeId        waiting agent node
	 * @param outputVar     node output variable
	 * @param outputValue   bounded interim output, usually {@code null}
	 * @param outputPreview bounded display preview
	 * @param agentRunId    durable agent run identifier
	 * @param roomId        durable agent room identifier
	 * @param resumeNodeId  next control node, or {@code null} when this is the
	 *                      final graph node
	 * @param createdBy     principal that initiated the Automation run
	 * @param expiresAt     approval deadline retained for policy and cleanup
	 *                      processing
	 * @param durationMs    elapsed execution time before the input boundary
	 * @return persisted wait identifier
	 */
	public static String persistAgentWait(String runId, String projectId, String nodeId, String outputVar,
			String outputValue, String outputPreview, String agentRunId, String roomId, String resumeNodeId,
			String createdBy, Instant expiresAt, long durationMs) {
		return persistAgentWait(runId, projectId, nodeId, outputVar, outputValue, outputPreview, agentRunId, roomId,
				resumeNodeId, createdBy, expiresAt, durationMs, null);
	}

	public static String persistAgentWait(String runId, String projectId, String nodeId, String outputVar,
			String outputValue, String outputPreview, String agentRunId, String roomId, String resumeNodeId,
			String createdBy, Instant expiresAt, long durationMs, String effectId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting the automation agent wait");
		ItemClaim itemClaim = claimedItem(runId);
		String waitId = UUID.randomUUID().toString();
		Timestamp now = toTimestamp(Instant.now());
		try {
			String persistedWaitId = QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				if (itemClaim != null) {
					fenceItemLease(conn, itemClaim);
				}
				try (PreparedStatement ps = conn.prepareStatement(MARK_NODE_WAITING)) {
					int index = 1;
					ps.setString(index++, AutomationConstants.NODE_STATUS_WAITING_FOR_INPUT);
					ps.setLong(index++, durationMs);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, outputVar);
					schedulerDb.getQueryUtil().setNullableLargeText(ps, index++, outputValue);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, outputPreview);
					ps.setString(index++, agentRunId);
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					ps.setString(index++, NODE_STATUS_RUNNING);
					requireSingleRow(ps.executeUpdate(), "mark the agent node waiting", runId, nodeId);
				}
				try (PreparedStatement ps = conn.prepareStatement(MARK_RUN_WAITING)) {
					ps.setString(1, AutomationConstants.STATUS_WAITING_FOR_INPUT);
					ps.setTimestamp(2, now);
					ps.setString(3, runId);
					ps.setString(4, projectId);
					ps.setString(5, STATUS_RUNNING);
					ps.setString(6, requiredLease(runId));
					requireSingleRow(ps.executeUpdate(), "mark the automation run waiting", runId, nodeId);
				}
				QueryExecutionUtility.executeUpdate(conn, INSERT_RUN_WAIT, ps -> {
					int index = 1;
					ps.setString(index++, waitId);
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					ps.setString(index++, AutomationConstants.WAIT_TYPE_AGENT_ACTION);
					ps.setString(index++, agentRunId);
					ps.setString(index++, roomId);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, resumeNodeId);
					ps.setString(index++, AutomationConstants.WAIT_STATUS_PENDING);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, createdBy);
					ps.setTimestamp(index++, now);
					ps.setTimestamp(index, toTimestamp(expiresAt));
				});
				if (effectId != null) {
					try (PreparedStatement effect = conn.prepareStatement(
							"UPDATE AUTOMATION_EFFECTS SET EFFECT_STATE=?,COMMITTED_AT=? WHERE EFFECT_ID=? "
									+ "AND RUN_ID=? AND NODE_ID=? AND EFFECT_STATE IN (?,?)")) {
						effect.setString(1, AutomationConstants.EFFECT_STATE_COMMITTED);
						effect.setTimestamp(2, now);
						effect.setString(3, effectId);
						effect.setString(4, runId);
						effect.setString(5, nodeId);
						effect.setString(6, AutomationConstants.EFFECT_STATE_PREPARED);
						effect.setString(7, AutomationConstants.EFFECT_STATE_ACCEPTED);
						requireSingleRow(effect.executeUpdate(), "commit the waiting agent effect", runId, nodeId);
					}
				}

				return waitId;
			});
			CLAIMED_LEASES.get().remove(runId);
			ACTIVE_RUN_LEASES.remove(runId);
			return persistedWaitId;
		} catch (Exception e) {
			classLogger.error("Failed to persist agent wait for run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the automation agent wait.", e);
		}
	}

	/**
	 * Returns the newest pending or claimed input boundary for a run.
	 *
	 * @param runId Automation run identifier
	 * @return active wait row, or {@code null}
	 */
	public static Map<String, Object> getActiveWait(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return null;
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : new String[] { AutomationConstants.WAIT_ID, RUN_ID, NODE_ID, AutomationConstants.WAIT_TYPE,
				AGENT_RUN_ID, ROOM_ID, AutomationConstants.RESUME_NODE_ID, STATUS, CREATED_BY, STARTED_AT,
				AutomationConstants.EXPIRES_AT }) {
			qs.addSelector(new QueryColumnSelector(TABLE_RUN_WAITS + "__" + column, column));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUN_WAITS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		OrQueryFilter active = new OrQueryFilter();
		active.addFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUN_WAITS + "__" + STATUS, "==",
				AutomationConstants.WAIT_STATUS_PENDING, PixelDataType.CONST_STRING));
		active.addFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUN_WAITS + "__" + STATUS, "==",
				AutomationConstants.WAIT_STATUS_RESUMING, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(active);
		qs.addOrderBy(TABLE_RUN_WAITS + "__" + STARTED_AT,
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.DESC.toString());
		qs.setLimit(1);
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return rows == null || rows.isEmpty() ? null : rows.get(0);
	}

	/**
	 * Atomically claims one waiting run and its input boundary for continuation.
	 *
	 * @param runId     Automation run identifier
	 * @param projectId owning Automation project
	 * @return claimed wait row, or {@code null} when another caller already claimed
	 *         it
	 */
	public static Map<String, Object> claimWaitingRun(String runId, String projectId) {
		Map<String, Object> wait = getActiveWait(runId);
		if (wait == null || !AutomationConstants.WAIT_STATUS_PENDING.equals(wait.get(STATUS))) {
			return null;
		}
		IRDBMSEngine schedulerDb = requireSchedulerDb("claiming the waiting automation run");
		String leaseToken = UUID.randomUUID().toString();
		try {
			Map<String, Object> claimedWait = QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(CLAIM_WAITING_RUN)) {
					Timestamp now = toTimestamp(Instant.now());
					ps.setString(1, STATUS_RUNNING);
					ps.setTimestamp(2, now);
					ps.setString(3, UUID.randomUUID().toString());
					ps.setString(4, LEASE_OWNER_ID);
					ps.setString(5, leaseToken);
					ps.setTimestamp(6, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
					ps.setString(7, runId);
					ps.setString(8, projectId);
					ps.setString(9, AutomationConstants.STATUS_WAITING_FOR_INPUT);
					if (ps.executeUpdate() != 1) {
						return null;
					}
				}
				try (PreparedStatement ps = conn.prepareStatement(CLAIM_RUN_WAIT)) {
					ps.setString(1, AutomationConstants.WAIT_STATUS_RESUMING);
					ps.setString(2, String.valueOf(wait.get(AutomationConstants.WAIT_ID)));
					ps.setString(3, runId);
					ps.setString(4, AutomationConstants.WAIT_STATUS_PENDING);
					requireSingleRow(ps.executeUpdate(), "claim the automation wait", runId,
							String.valueOf(wait.get(NODE_ID)));
				}

				wait.put(STATUS, AutomationConstants.WAIT_STATUS_RESUMING);
				return wait;
			});
			if (claimedWait != null) {
				CLAIMED_LEASES.get().put(runId, leaseToken);
			}
			return claimedWait;
		} catch (Exception e) {
			classLogger.error("Failed to claim waiting automation run '{}'", runId, e);
			throw new IllegalStateException("Unable to claim the waiting automation run.", e);
		}
	}

	/**
	 * Marks a claimed input boundary resolved after its agent node reaches a
	 * terminal state.
	 *
	 * @param runId      Automation run identifier
	 * @param waitId     wait identifier
	 * @param resolvedBy project editor who reconciled the completed agent run
	 */
	public static void resolveWait(String runId, String waitId, String resolvedBy) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("resolving the automation wait");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				try (PreparedStatement ps = conn.prepareStatement(RESOLVE_RUN_WAIT)) {
					ps.setString(1, AutomationConstants.WAIT_STATUS_RESOLVED);
					ps.setTimestamp(2, toTimestamp(Instant.now()));
					schedulerDb.getQueryUtil().setNullableString(ps, 3, resolvedBy);
					ps.setString(4, waitId);
					ps.setString(5, runId);
					ps.setString(6, AutomationConstants.WAIT_STATUS_RESUMING);
					requireSingleRow(ps.executeUpdate(), "resolve the automation wait", runId, null);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to resolve wait '{}' for run '{}'", waitId, runId, e);
			throw new IllegalStateException("Unable to resolve the automation wait.", e);
		}
	}

	private static void insertAllRunNodeSources(Connection conn, AbstractSqlQueryUtil queryUtil, String runId,
			Map<String, String> nodeSources) throws SQLException, UnsupportedEncodingException {
		if (nodeSources == null || nodeSources.isEmpty()) {
			return;
		}
		try (PreparedStatement ps = conn.prepareStatement(INSERT_RUN_NODE_SOURCE)) {
			for (Map.Entry<String, String> entry : nodeSources.entrySet()) {
				int index = 1;
				ps.setString(index++, runId);
				ps.setString(index++, entry.getKey());
				ps.setString(index++, AutomationDefinitionService.calculateSourceHash(entry.getValue()));
				queryUtil.setNullableLargeText(ps, index, entry.getValue());
				ps.addBatch();
			}
			ps.executeBatch();
		}
	}

	private static void insertRun(Connection conn, AbstractSqlQueryUtil queryUtil, String runId, String projectId,
			String automationId, int definitionVersion, String definitionHash, String definitionSnapshot,
			Integer executionContractVersion, String executionContractSnapshot, String inputSnapshot,
			String triggerType, int totalNodes, String createdBy, String principalAccess, boolean asyncAdmission,
			Timestamp now) throws SQLException, UnsupportedEncodingException {
		try (PreparedStatement ps = conn.prepareStatement(INSERT_RUN)) {
			int index = 1;
			ps.setString(index++, runId);
			ps.setString(index++, projectId);
			ps.setString(index++, automationId);
			ps.setInt(index++, definitionVersion);
			ps.setString(index++, definitionHash);
			queryUtil.setNullableLargeText(ps, index++, definitionSnapshot);
			if (executionContractVersion == null) {
				ps.setNull(index++, java.sql.Types.INTEGER);
			} else {
				ps.setInt(index++, executionContractVersion);
			}
			queryUtil.setNullableLargeText(ps, index++, executionContractSnapshot);
			queryUtil.setNullableLargeText(ps, index++, inputSnapshot);
			ps.setString(index++, STATUS_SUBMITTED);
			ps.setString(index++, triggerType);
			ps.setTimestamp(index++, now);
			ps.setTimestamp(index++, now);
			ps.setInt(index++, totalNodes);
			ps.setString(index++, createdBy);
			queryUtil.setNullableString(ps, index++, principalAccess);
			ps.setBoolean(index++, asyncAdmission);
			ps.executeUpdate();
		}
	}

	private static void insertAllNodeOutputs(Connection conn, AbstractSqlQueryUtil queryUtil, String runId,
			List<Map<String, Object>> orderedNodes, Map<String, String> traceRoomIds) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(INSERT_NODE_OUTPUT)) {
			for (int i = 0; i < orderedNodes.size(); i++) {
				Map<String, Object> node = orderedNodes.get(i);
				int index = 1;
				ps.setString(index++, runId);
				ps.setString(index++, (String) node.get(NODE_FIELD_ID));
				ps.setString(index++, (String) node.get(NODE_FIELD_LABEL));
				ps.setInt(index++, i);
				ps.setString(index++, NODE_STATUS_PENDING);
				queryUtil.setNullableString(ps, index++,
						traceRoomIds == null ? null : traceRoomIds.get(node.get(NODE_FIELD_ID)));
				queryUtil.setNullableString(ps, index++, AutomationRuntime.configuredAgentWorkspaceId(node));
				ps.addBatch();
			}
			ps.executeBatch();
		}
	}

	/**
	 * Materializes one loop iteration into the existing node-history table. The
	 * runtime row receives its own ID while SOURCE_NODE_ID keeps the stable graph
	 * identity, so repeated executions never overwrite each other and clients do
	 * not have to parse synthetic identifiers.
	 *
	 * @param runId         owning run
	 * @param loopNodeId    canonical parent loop node
	 * @param iterationIndex zero-based iteration
	 * @param orderedNodes  body nodes in deterministic execution-history order
	 * @param traceRoomIds  optional conversational room IDs keyed by source node
	 * @return runtime node IDs keyed by canonical body node ID
	 */
	public static Map<String, String> insertLoopIterationNodes(String runId, String loopNodeId, int iterationIndex,
			List<Map<String, Object>> orderedNodes, Map<String, String> traceRoomIds) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("initialize an automation loop iteration");
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				Map<String, String> executionNodeIds = new LinkedHashMap<>();
				try (PreparedStatement total = conn.prepareStatement(INCREMENT_RUN_TOTAL_NODES);
						PreparedStatement output = conn.prepareStatement(INSERT_LOOP_NODE_OUTPUT)) {
					total.setInt(1, orderedNodes.size());
					total.setString(2, runId);
					requireSingleRow(total.executeUpdate(), "extend the loop run node count", runId, loopNodeId);
					for (int index = 0; index < orderedNodes.size(); index++) {
						Map<String, Object> node = orderedNodes.get(index);
						String sourceNodeId = (String) node.get(NODE_FIELD_ID);
						String executionNodeId = UUID.randomUUID().toString();
						executionNodeIds.put(sourceNodeId, executionNodeId);
						int parameter = 1;
						output.setString(parameter++, runId);
						output.setString(parameter++, executionNodeId);
						output.setString(parameter++, (String) node.get(NODE_FIELD_LABEL));
						output.setInt(parameter++, LOOP_EXECUTION_ORDER_OFFSET
								+ iterationIndex * Math.max(1, orderedNodes.size()) + index);
						output.setString(parameter++, NODE_STATUS_PENDING);
						schedulerDb.getQueryUtil().setNullableString(output, parameter++,
								traceRoomIds == null ? null : traceRoomIds.get(sourceNodeId));
						schedulerDb.getQueryUtil().setNullableString(output, parameter++,
								AutomationRuntime.configuredAgentWorkspaceId(node));
						output.setString(parameter++, loopNodeId);
						output.setInt(parameter++, iterationIndex);
						output.setString(parameter++, sourceNodeId);
						output.addBatch();
					}
					output.executeBatch();
				}
				return executionNodeIds;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to initialize the automation loop iteration.", e);
		}
	}

	/**
	 * Sets the cluster-safe cancellation flag on a run. Called by
	 * {@code CancelAutomationRunReactor} regardless of which pod receives the
	 * cancel request. Unlike the in-memory run registry, this flag is visible to
	 * whichever pod is executing the run via {@link #isCancelRequested(String)}.
	 */
	public static void setCancelRequested(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting the automation cancellation request");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(SET_CANCEL_REQUESTED)) {
					ps.setBoolean(1, true);
					ps.setString(2, runId);
					requireSingleRow(ps.executeUpdate(), "set the cancellation flag", runId, null);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to set cancel-requested flag for run '{}'", runId, e);
			throw new IllegalStateException("Unable to persist the automation cancellation request.", e);
		}
	}

	/**
	 * Checks the cluster-safe cancellation flag for a run. Polled by the executing
	 * pod's between-node cancellation check in addition to the local in-memory
	 * flag, so a cancel request landing on a different pod than the one executing
	 * the run is still honored.
	 */
	public static boolean isCancelRequested(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return false;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + CANCEL_REQUESTED, CANCEL_REQUESTED));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUNS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results == null || results.isEmpty()) {
			return false;
		}
		Object flag = results.get(0).get(CANCEL_REQUESTED);
		if (flag instanceof Boolean) {
			return (Boolean) flag;
		}
		return flag != null && Boolean.parseBoolean(flag.toString());
	}

	/**
	 * Persists a terminal status for a running Automation run.
	 */
	public static void completeRun(String runId, String projectId, String status, String failedNodeId,
			String errorMessage) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("completing the automation run");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(UPDATE_RUN_STATUS)) {
					int index = 1;
					ps.setString(index++, status);
					ps.setTimestamp(index++, toTimestamp(Instant.now()));
					schedulerDb.getQueryUtil().setNullableString(ps, index++, failedNodeId);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, errorMessage);
					ps.setString(index++, runId);
					ps.setString(index++, projectId);
					ps.setString(index++, STATUS_RUNNING);
					ps.setString(index++, requiredLease(runId));
					requireSingleRow(ps.executeUpdate(), "update the terminal run status", runId, null);
				}
				return null;
			});
			CLAIMED_LEASES.get().remove(runId);
			ACTIVE_RUN_LEASES.remove(runId);
		} catch (Exception e) {
			classLogger.error("Failed to complete run '{}' for project '{}'", runId, projectId, e);
			throw new IllegalStateException("Unable to persist the completed automation run.", e);
		}
	}

	/**
	 * Persists the human-readable outcome summary for a completed run. Called after
	 * the run finishes, separately from {@link #completeRun} because the summary is
	 * built by {@link AutomationRunExecutionService} after node results are
	 * assembled.
	 */
	public static boolean updateRunSummary(String runId, String resultSummary) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return false;
		}

		try {
			QueryExecutionUtility.executeUpdate(schedulerDb, UPDATE_RUN_SUMMARY, ps -> {
				schedulerDb.getQueryUtil().setNullableString(ps, 1, resultSummary);
				ps.setString(2, runId);
			});
			return true;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update run summary for '{}'", runId, e);
			return false;
		}
	}

	/**
	 * Updates the heartbeat timestamp and completed node count for a running
	 * automation.
	 */
	public static boolean updateHeartbeat(String runId, int completedNodes) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return false;
		}

		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(UPDATE_HEARTBEAT)) {
					int index = 1;
					Timestamp now = toTimestamp(Instant.now());
					ps.setTimestamp(index++, now);
					ps.setInt(index++, completedNodes);
					ps.setTimestamp(index++, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
					ps.setString(index++, runId);
					ps.setString(index++, STATUS_RUNNING);
					ps.setString(index++, requiredLease(runId));
					return ps.executeUpdate() == 1;
				}
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update heartbeat for run '{}'", runId, e);
			return false;
		}
	}

	/**
	 * Updates only the heartbeat timestamp for a running automation. Used when the
	 * node count hasn't changed but liveness needs to be signaled.
	 */
	public static boolean touchHeartbeat(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return false;
		}

		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(TOUCH_HEARTBEAT)) {
					Timestamp now = toTimestamp(Instant.now());
					ps.setTimestamp(1, now);
					ps.setTimestamp(2, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
					ps.setString(3, runId);
					ps.setString(4, STATUS_RUNNING);
					ps.setString(5, requiredLease(runId));
					return ps.executeUpdate() == 1;
				}
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to touch heartbeat for run '{}'", runId, e);
			return false;
		}
	}

	static boolean heartbeatDurableItem(ItemClaim claim) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return false;
		}
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				try (PreparedStatement ps = conn.prepareStatement(
						"UPDATE AUTOMATION_ITEMS SET LEASE_EXPIRES_AT=? WHERE ITEM_ID=? AND STATUS=? "
								+ "AND ATTEMPT_ID=? AND LEASE_OWNER=? AND LEASE_TOKEN=?")) {
					ps.setTimestamp(1, toTimestamp(Instant.now().plusSeconds(LEASE_SECONDS)));
					ps.setString(2, claim.itemId());
					ps.setString(3, AutomationConstants.ITEM_STATUS_RUNNING);
					ps.setString(4, claim.attemptId());
					ps.setString(5, LEASE_OWNER_ID);
					ps.setString(6, claim.leaseToken());
					return ps.executeUpdate() == 1;
				}
			});
		} catch (Exception e) {
			classLogger.warn("Failed to heartbeat durable Automation item '{}'", claim.itemId(), e);
			return false;
		}
	}

	static void incrementDurableItemCompletedNodes(ItemClaim claim) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("updating durable item run progress");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceItemLease(conn, claim);
				try (PreparedStatement ps = conn.prepareStatement(
						"UPDATE AUTOMATION_RUNS SET COMPLETED_NODES=COMPLETED_NODES+1 "
								+ "WHERE RUN_ID=? AND STATUS=?")) {
					ps.setString(1, claim.runId());
					ps.setString(2, AutomationConstants.STATUS_WAITING_FOR_ITEMS);
					requireSingleRow(ps.executeUpdate(), "update durable item run progress", claim.runId(),
							claim.nodeId());
				}
				return null;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to update durable Automation item progress.", e);
		}
	}

	/**
	 * Lists automation runs for a project, newest first.
	 *
	 * @param projectId the project to query
	 * @param limit     max number of runs to return
	 * @return list of run summary maps
	 */
	public static List<Map<String, Object>> getRunsForProject(String projectId, int limit) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return new ArrayList<>();
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RUN_ID, RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + PROJECT_ID, PROJECT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AUTOMATION_ID, AUTOMATION_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_VERSION, DEFINITION_VERSION));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_HASH, DEFINITION_HASH));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STATUS, STATUS));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TRIGGER_TYPE, TRIGGER_TYPE));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STARTED_AT, STARTED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.ATTEMPT_ID,
				AutomationConstants.ATTEMPT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.ATTEMPT_NUMBER,
				AutomationConstants.ATTEMPT_NUMBER));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_AT, COMPLETED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + FAILED_NODE_ID, FAILED_NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TOTAL_NODES, TOTAL_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_NODES, COMPLETED_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + CREATED_BY, CREATED_BY));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.PRINCIPAL_ACCESS,
				AutomationConstants.PRINCIPAL_ACCESS));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RESULT_SUMMARY_COL, RESULT_SUMMARY_COL));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUNS + "__" + PROJECT_ID, "==", projectId,
				PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_RUNS + "__" + STARTED_AT, QueryColumnOrderBySelector.ORDER_BY_DIRECTION.DESC.toString());
		qs.setLimit(limit);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return results != null ? results : new ArrayList<>();
	}

	/**
	 * Gets a single run detail by run ID (includes error message).
	 */
	public static Map<String, Object> getRunDetail(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return null;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RUN_ID, RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + PROJECT_ID, PROJECT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AUTOMATION_ID, AUTOMATION_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_VERSION, DEFINITION_VERSION));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_HASH, DEFINITION_HASH));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + DEFINITION_SNAPSHOT, DEFINITION_SNAPSHOT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + EXECUTION_CONTRACT_VERSION,
				EXECUTION_CONTRACT_VERSION));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STATUS, STATUS));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TRIGGER_TYPE, TRIGGER_TYPE));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + STARTED_AT, STARTED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.ATTEMPT_ID,
				AutomationConstants.ATTEMPT_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.ATTEMPT_NUMBER,
				AutomationConstants.ATTEMPT_NUMBER));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.LEASE_OWNER,
				AutomationConstants.LEASE_OWNER));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + AutomationConstants.LEASE_EXPIRES_AT,
				AutomationConstants.LEASE_EXPIRES_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_AT, COMPLETED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + FAILED_NODE_ID, FAILED_NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + ERROR_MESSAGE, ERROR_MESSAGE));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + TOTAL_NODES, TOTAL_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + COMPLETED_NODES, COMPLETED_NODES));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + CREATED_BY, CREATED_BY));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + RESULT_SUMMARY_COL, RESULT_SUMMARY_COL));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUNS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results != null && !results.isEmpty()) {
			return results.get(0);
		}
		return null;
	}

	public static List<Map<String, Object>> getDurableItemsForRun(String runId) {
		return selectRunRows(TABLE_ITEMS, runId, AutomationConstants.DURABLE_ITEM_DETAIL_MAX_ROWS,
				List.of(AutomationConstants.ITEM_ID, RUN_ID, NODE_ID, AutomationConstants.ITEM_INDEX,
						STATUS, AutomationConstants.ATTEMPT_ID, AutomationConstants.ATTEMPT_NUMBER,
						AutomationConstants.MAX_ATTEMPTS, AutomationConstants.LEASE_OWNER,
						AutomationConstants.LEASE_EXPIRES_AT, AutomationConstants.NEXT_ATTEMPT_AT,
						AutomationConstants.CREATED_AT, STARTED_AT, COMPLETED_AT, ERROR_MESSAGE),
				AutomationConstants.ITEM_INDEX);
	}

	public static List<Map<String, Object>> getEffectsForRun(String runId) {
		return selectRunRows(TABLE_EFFECTS, runId, AutomationConstants.DURABLE_ITEM_DETAIL_MAX_ROWS,
				List.of(AutomationConstants.EFFECT_ID, RUN_ID, NODE_ID, AutomationConstants.ITEM_ID,
						AutomationConstants.ATTEMPT_ID, AutomationConstants.EFFECT_STATE,
						AutomationConstants.OPERATION_KEY, AutomationConstants.IDEMPOTENCY_KEY,
						AutomationConstants.RECEIPT_HASH,
						AutomationConstants.PREPARED_AT, AutomationConstants.ACCEPTED_AT,
						AutomationConstants.COMMITTED_AT, ERROR_MESSAGE),
				AutomationConstants.PREPARED_AT);
	}

	private static List<Map<String, Object>> selectRunRows(String table, String runId, int limit,
			List<String> columns, String orderColumn) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return List.of();
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : columns) {
			qs.addSelector(new QueryColumnSelector(table + "__" + column, column));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.addOrderBy(table + "__" + orderColumn, QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());
		qs.setLimit(limit);
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return rows == null ? List.of() : rows;
	}

	/**
	 * Loads the immutable executor-selection contract captured at submission.
	 * Legacy rows return a record whose two values are both {@code null}.
	 */
	static PersistedExecutionContract getRunExecutionContract(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("loading the automation run execution contract");
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + EXECUTION_CONTRACT_VERSION,
				EXECUTION_CONTRACT_VERSION));
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + EXECUTION_CONTRACT_SNAPSHOT,
				EXECUTION_CONTRACT_SNAPSHOT));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUNS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results == null || results.isEmpty()) {
			throw new IllegalStateException("Automation run '" + runId + "' does not exist.");
		}
		Map<String, Object> row = results.get(0);
		Object rawVersion = row.get(EXECUTION_CONTRACT_VERSION);
		Integer version = rawVersion instanceof Number number ? number.intValue() : null;
		Object rawSnapshot = row.get(EXECUTION_CONTRACT_SNAPSHOT);
		return new PersistedExecutionContract(version, rawSnapshot == null ? null : rawSnapshot.toString());
	}

	record PersistedExecutionContract(Integer version, String snapshot) {
	}

	/**
	 * Loads the immutable effective trigger inputs captured when a run was
	 * submitted. Runtime-owned scope metadata is deliberately created at execution
	 * time and is never stored in this column.
	 *
	 * @param runId run whose input snapshot is loaded
	 * @return mutable input map for the run-local execution scope
	 */
	public static Map<String, Object> getRunInputs(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("loading the automation run input snapshot");

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUNS + "__" + INPUT_SNAPSHOT, INPUT_SNAPSHOT));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUNS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.setLimit(1);

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		if (results == null || results.isEmpty()) {
			throw new IllegalStateException("Automation run '" + runId + "' does not exist.");
		}
		Object value = results.get(0).get(INPUT_SNAPSHOT);
		if (value == null || value.toString().isBlank()) {
			return new LinkedHashMap<>();
		}
		try {
			Map<String, Object> inputs = AutomationRuntimeUtils.GSON.fromJson(value.toString(),
					AutomationRuntimeUtils.MAP_TYPE);
			AutomationRuntimeUtils.toBoundedRuntimeJson(inputs, AutomationConstants.RUN_INPUTS_MAX_BYTES,
					"Persisted automation run inputs");
			return new LinkedHashMap<>(inputs);
		} catch (Exception e) {
			throw new IllegalStateException("Automation run '" + runId + "' has an invalid input snapshot.", e);
		}
	}

	/**
	 * Loads the immutable Python source snapshot captured when a run was created.
	 * Source hashes are checked before any source is returned for execution.
	 */
	public static Map<String, String> getRunNodeSources(String runId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("loading the automation run source snapshot");

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_RUN_SOURCES + "__" + NODE_ID, NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_RUN_SOURCES + "__" + SOURCE_HASH, SOURCE_HASH));
		qs.addSelector(new QueryColumnSelector(TABLE_RUN_SOURCES + "__" + SOURCE_CODE, SOURCE_CODE));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUN_SOURCES + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));

		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		Map<String, String> sources = new LinkedHashMap<>();
		if (rows == null) {
			return sources;
		}

		for (Map<String, Object> row : rows) {
			String nodeId = String.valueOf(row.get(NODE_ID));
			Object sourceValue = row.get(SOURCE_CODE);
			String source = sourceValue == null ? null : sourceValue.toString();
			String expectedHash = String.valueOf(row.get(SOURCE_HASH));
			if (source == null || !expectedHash.equals(AutomationDefinitionService.calculateSourceHash(source))) {
				throw new IllegalStateException("Automation run source snapshot is invalid for node '" + nodeId + "'.");
			}
			sources.put(nodeId, source);
		}
		return sources;
	}

	static Map<String, String> getRunTraceRoomIds(String runId) {
		Map<String, String> traceRoomIds = new LinkedHashMap<>();
		for (Map<String, Object> output : getNodeOutputsForRun(runId)) {
			Object roomId = output.get(ROOM_ID);
			Object sourceNodeId = output.get(AutomationConstants.SOURCE_NODE_ID);
			String nodeId = sourceNodeId == null ? String.valueOf(output.get(NODE_ID)) : String.valueOf(sourceNodeId);
			if (roomId != null && !String.valueOf(roomId).isBlank()) {
				traceRoomIds.putIfAbsent(nodeId, String.valueOf(roomId));
			}
		}
		return traceRoomIds;
	}

	/** Returns payload-free retained-result references for a run. */
	public static List<Map<String, Object>> getSavedResultReferencesForRun(String runId) {
		SelectQueryStruct qs = savedResultMetadataQuery(false);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_SAVED_RESULTS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_SAVED_RESULTS + "__" + AutomationConstants.CAPTURE_STATE, "==",
				AutomationConstants.CAPTURE_STATE_RETAINED, PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_SAVED_RESULTS + "__" + AutomationConstants.CREATED_AT,
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());
		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(
				requireSchedulerDb("loading automation saved-result references"), qs);
		return results != null ? results : new ArrayList<>();
	}

	/** Returns one retained saved result, including its small value or table schema payload. */
	public static Map<String, Object> getSavedResult(String resultId) {
		SelectQueryStruct qs = savedResultMetadataQuery(true);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_SAVED_RESULTS + "__" + AutomationConstants.RESULT_ID, "==", resultId,
				PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_SAVED_RESULTS + "__" + AutomationConstants.CAPTURE_STATE, "==",
				AutomationConstants.CAPTURE_STATE_RETAINED, PixelDataType.CONST_STRING));
		qs.setLimit(1);
		List<Map<String, Object>> results =
				QueryExecutionUtility.flushRsToMap(requireSchedulerDb("loading an automation saved result"), qs);
		return results == null || results.isEmpty() ? null : results.get(0);
	}

	/** Returns ordered chunk metadata without loading chunk payloads. */
	public static List<Map<String, Object>> getSavedResultChunkMetadata(String resultId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		addChunkSelectors(qs, false);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RESULT_CHUNKS + "__" + AutomationConstants.RESULT_ID, "==", resultId,
				PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_RESULT_CHUNKS + "__" + AutomationConstants.CHUNK_INDEX,
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());
		List<Map<String, Object>> results =
				QueryExecutionUtility.flushRsToMap(requireSchedulerDb("loading saved-result chunk metadata"), qs);
		return results != null ? results : new ArrayList<>();
	}

	/** Loads one bounded result chunk. */
	public static Map<String, Object> getSavedResultChunk(String resultId, int chunkIndex) {
		SelectQueryStruct qs = new SelectQueryStruct();
		addChunkSelectors(qs, true);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RESULT_CHUNKS + "__" + AutomationConstants.RESULT_ID, "==", resultId,
				PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				TABLE_RESULT_CHUNKS + "__" + AutomationConstants.CHUNK_INDEX, "==", chunkIndex,
				PixelDataType.CONST_INT));
		qs.setLimit(1);
		List<Map<String, Object>> results =
				QueryExecutionUtility.flushRsToMap(requireSchedulerDb("loading a saved-result chunk"), qs);
		return results == null || results.isEmpty() ? null : results.get(0);
	}

	/**
	 * Expires a bounded batch of retained payloads while preserving their metadata
	 * and integrity summary.
	 */
	public static int expireSavedResults(Instant now, int limit) {
		if (limit <= 0 || limit > 10_000) {
			throw new IllegalArgumentException("Saved-result expiration limit must be between 1 and 10000.");
		}

		IRDBMSEngine schedulerDb = requireSchedulerDb("expiring automation saved results");
		List<Map<String, Object>> candidates;
		try {
			candidates = QueryExecutionUtility.write(schedulerDb, conn -> {
				List<Map<String, Object>> rows = new ArrayList<>();
				String sql = "SELECT S.RESULT_ID, S.RUN_ID, R.STATUS FROM AUTOMATION_SAVED_RESULTS S "
						+ "JOIN AUTOMATION_RUNS R ON R.RUN_ID = S.RUN_ID "
						+ "WHERE S.CAPTURE_STATE = ? AND S.RETAIN_UNTIL < ? ORDER BY S.RETAIN_UNTIL";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.setString(1, AutomationConstants.CAPTURE_STATE_RETAINED);
					ps.setTimestamp(2, toTimestamp(now));
					ps.setMaxRows(limit);
					try (java.sql.ResultSet result = ps.executeQuery()) {
						while (result.next()) {
							rows.add(Map.of(AutomationConstants.RESULT_ID, result.getString(1), RUN_ID,
									result.getString(2), STATUS, result.getString(3)));
						}
					}
				}
				return rows;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to find expired automation saved results.", e);
		}
		if (candidates == null || candidates.isEmpty()) {
			return 0;
		}
		List<String> resultIds = new ArrayList<>();
		for (Map<String, Object> candidate : candidates) {
			Object runStatus = candidate.get(STATUS);
			if (STATUS_RUNNING.equals(runStatus) || STATUS_SUBMITTED.equals(runStatus)
					|| AutomationConstants.STATUS_WAITING_FOR_INPUT.equals(runStatus)) {
				continue;
			}
			resultIds.add(String.valueOf(candidate.get(AutomationConstants.RESULT_ID)));
		}
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				int expired = 0;
				for (String resultId : resultIds) {
					try (PreparedStatement delete = conn.prepareStatement(DELETE_RESULT_CHUNKS)) {
						delete.setString(1, resultId);
						delete.executeUpdate();
					}
					try (PreparedStatement update = conn.prepareStatement(EXPIRE_SAVED_RESULT)) {
						update.setString(1, AutomationConstants.CAPTURE_STATE_EXPIRED);
						update.setString(2, resultId);
						update.setString(3, AutomationConstants.CAPTURE_STATE_RETAINED);
						expired += update.executeUpdate();
					}
				}
				return expired;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to expire automation saved results.", e);
		}
	}

	/**
	 * Redacts bounded item input and receipt metadata after the same retention
	 * window as saved results while preserving business identity, attempts, states,
	 * operation keys, and receipt hashes.
	 */
	public static int expireDurableItemPayloads(Instant now, int limit) {
		if (limit <= 0 || limit > 10_000) {
			throw new IllegalArgumentException("Durable-item expiration limit must be between 1 and 10000.");
		}
		IRDBMSEngine schedulerDb = requireSchedulerDb("expiring durable automation item payloads");
		Timestamp cutoff = toTimestamp(now.minusSeconds(
				AutomationConstants.SAVED_RESULT_RETENTION_DAYS * 24L * 60 * 60));
		try {
			return QueryExecutionUtility.write(schedulerDb, conn -> {
				List<String> itemIds = new ArrayList<>();
				try (PreparedStatement select = conn.prepareStatement(
						"SELECT I.ITEM_ID FROM AUTOMATION_ITEMS I JOIN AUTOMATION_RUNS R ON R.RUN_ID=I.RUN_ID "
								+ "WHERE R.COMPLETED_AT<? AND R.STATUS NOT IN (?,?,?) "
								+ "AND I.ITEM_INPUT_SNAPSHOT<>'null' ORDER BY R.COMPLETED_AT")) {
					select.setTimestamp(1, cutoff);
					select.setString(2, STATUS_RUNNING);
					select.setString(3, STATUS_SUBMITTED);
					select.setString(4, AutomationConstants.STATUS_WAITING_FOR_INPUT);
					select.setMaxRows(limit);
					try (ResultSet rs = select.executeQuery()) {
						while (rs.next()) {
							itemIds.add(rs.getString(1));
						}
					}
				}
				List<String> effectIds = new ArrayList<>();
				try (PreparedStatement select = conn.prepareStatement(
						"SELECT E.EFFECT_ID FROM AUTOMATION_EFFECTS E "
								+ "JOIN AUTOMATION_RUNS R ON R.RUN_ID=E.RUN_ID "
								+ "WHERE R.STATUS NOT IN (?,?,?) AND E.RECEIPT_METADATA IS NOT NULL "
								+ "AND COALESCE(R.COMPLETED_AT,E.COMMITTED_AT,E.ACCEPTED_AT,E.PREPARED_AT)<? "
								+ "ORDER BY COALESCE(R.COMPLETED_AT,E.COMMITTED_AT,E.ACCEPTED_AT,E.PREPARED_AT)")) {
					select.setString(1, STATUS_RUNNING);
					select.setString(2, STATUS_SUBMITTED);
					select.setString(3, AutomationConstants.STATUS_WAITING_FOR_INPUT);
					select.setTimestamp(4, cutoff);
					select.setMaxRows(limit);
					try (ResultSet rs = select.executeQuery()) {
						while (rs.next()) {
							effectIds.add(rs.getString(1));
						}
					}
				}
				int expired = 0;
				try (PreparedStatement item = conn.prepareStatement(
						"UPDATE AUTOMATION_ITEMS SET ITEM_INPUT_SNAPSHOT='null' WHERE ITEM_ID=?");
						PreparedStatement effects = conn.prepareStatement(
								"UPDATE AUTOMATION_EFFECTS SET RECEIPT_METADATA=NULL WHERE ITEM_ID=?");
						PreparedStatement effect = conn.prepareStatement(
								"UPDATE AUTOMATION_EFFECTS SET RECEIPT_METADATA=NULL WHERE EFFECT_ID=? "
										+ "AND RECEIPT_METADATA IS NOT NULL")) {
					for (String itemId : itemIds) {
						item.setString(1, itemId);
						expired += item.executeUpdate();
						effects.setString(1, itemId);
						effects.executeUpdate();
					}
					for (String effectId : effectIds) {
						effect.setString(1, effectId);
						expired += effect.executeUpdate();
					}
				}
				return expired;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to expire durable Automation item payloads.", e);
		}
	}

	private static SelectQueryStruct savedResultMetadataQuery(boolean includePayload) {
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : new String[] { AutomationConstants.RESULT_ID, PROJECT_ID, RUN_ID, NODE_ID,
				AutomationConstants.PARENT_NODE_ID, AutomationConstants.ITERATION_INDEX,
				AutomationConstants.RESULT_KIND, AutomationConstants.FORMAT_VERSION, AutomationConstants.ROW_COUNT,
				AutomationConstants.BYTE_COUNT, AutomationConstants.CHECKSUM, AutomationConstants.CREATED_AT,
				AutomationConstants.RETAIN_UNTIL, AutomationConstants.CAPTURE_STATE,
				AutomationConstants.RESULT_ROLE, AutomationConstants.LOGICAL_NAME }) {
			qs.addSelector(new QueryColumnSelector(TABLE_SAVED_RESULTS + "__" + column, column));
		}
		if (includePayload) {
			qs.addSelector(new QueryColumnSelector(
					TABLE_SAVED_RESULTS + "__" + AutomationConstants.PAYLOAD_VALUE, AutomationConstants.PAYLOAD_VALUE));
		}
		return qs;
	}

	private static void addChunkSelectors(SelectQueryStruct qs, boolean includePayload) {
		for (String column : new String[] { AutomationConstants.RESULT_ID, AutomationConstants.CHUNK_INDEX,
				AutomationConstants.FIRST_ROW_INDEX, AutomationConstants.CHUNK_ROW_COUNT,
				AutomationConstants.BYTE_COUNT, AutomationConstants.CHECKSUM }) {
			qs.addSelector(new QueryColumnSelector(TABLE_RESULT_CHUNKS + "__" + column, column));
		}
		if (includePayload) {
			qs.addSelector(new QueryColumnSelector(
					TABLE_RESULT_CHUNKS + "__" + AutomationConstants.CHUNK_VALUE, AutomationConstants.CHUNK_VALUE));
		}
	}

	// -- AUTOMATION_NODE_OUTPUTS CRUD
	// ------------------------------------------------

	/**
	 * Marks a node as RUNNING (before pixel execution starts).
	 */
	public static void markNodeRunning(String runId, String nodeId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("marking the automation node as running");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_STATUS)) {
					int index = 1;
					ps.setString(index++, NODE_STATUS_RUNNING);
					ps.setTimestamp(index++, toTimestamp(Instant.now()));
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					requireSingleRow(ps.executeUpdate(), "mark the node as running", runId, nodeId);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to mark node running for run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to mark the automation node as running.", e);
		}
	}

	/**
	 * Marks all nodes that did not start because the run reached a terminal state
	 * as skipped.
	 */
	public static void skipPendingNodes(String runId, String reason) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting skipped automation nodes");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				try (PreparedStatement ps = conn.prepareStatement(SKIP_PENDING_NODE_OUTPUTS)) {
					ps.setString(1, NODE_STATUS_SKIPPED);
					schedulerDb.getQueryUtil().setNullableString(ps, 2, reason);
					ps.setString(3, runId);
					ps.setString(4, NODE_STATUS_PENDING);
					ps.executeUpdate();
				}
				return null;
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to skip pending nodes for run '{}'", runId, e);
			throw new IllegalStateException("Unable to persist skipped automation nodes.", e);
		}
	}

	/**
	 * Marks only the unselected nodes in one materialized loop iteration skipped.
	 *
	 * @param runId  owning run
	 * @param nodeIds runtime node IDs that remain pending
	 * @param reason persisted skip explanation
	 */
	public static void skipPendingNodes(String runId, List<String> nodeIds, String reason) {
		if (nodeIds == null || nodeIds.isEmpty()) {
			return;
		}
		IRDBMSEngine schedulerDb = requireSchedulerDb("persist skipped automation loop nodes");
		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				try (PreparedStatement ps = conn.prepareStatement(SKIP_PENDING_NODE_OUTPUT)) {
					for (String nodeId : nodeIds) {
						ps.setString(1, NODE_STATUS_SKIPPED);
						schedulerDb.getQueryUtil().setNullableString(ps, 2, reason);
						ps.setString(3, runId);
						ps.setString(4, nodeId);
						ps.setString(5, NODE_STATUS_PENDING);
						ps.addBatch();
					}
					ps.executeBatch();
				}
				return null;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to persist skipped automation loop nodes.", e);
		}
	}

	/**
	 * Updates a node output after successful execution.
	 */
	public static void updateNodeSuccess(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String outputVar, String outputValue, String outputPreview, String modelMessageId, String agentRunId) {
		updateNodeSuccess(runId, nodeId, startedAt, durationMs, outputVar, null, outputValue, outputPreview,
				modelMessageId, agentRunId);
	}

	/** Updates a node output and its internal storage kind after successful execution. */
	public static void updateNodeSuccess(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String outputVar, String outputKind, String outputValue, String outputPreview, String modelMessageId,
			String agentRunId) {
		updateNodeSuccess(runId, nodeId, startedAt, durationMs, outputVar, outputKind, outputValue, outputPreview,
				modelMessageId, agentRunId, null);
	}

	/** Publishes a captured table and node success atomically. */
	public static void updateNodeSuccess(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String outputVar, String outputKind, String outputValue, String outputPreview, String modelMessageId,
			String agentRunId, AutomationTableCaptureService.TableCapture tableCapture) {
		updateNodeSuccess(runId, nodeId, startedAt, durationMs, outputVar, outputKind, outputValue, outputPreview,
				modelMessageId, agentRunId, tableCapture, null);
	}

	/** Publishes node success and the accepted effect commit in one transaction. */
	public static void updateNodeSuccess(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String outputVar, String outputKind, String outputValue, String outputPreview, String modelMessageId,
			String agentRunId, AutomationTableCaptureService.TableCapture tableCapture, String effectId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting the successful automation node result");
		ItemClaim itemClaim = claimedItem(runId);

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				if (itemClaim != null) {
					fenceItemLease(conn, itemClaim);
				}
				AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
				if (tableCapture != null) {
					insertSavedTable(conn, queryUtil, runId, nodeId, outputVar, tableCapture);
				} else if (AutomationConstants.OUTPUT_KIND_FILE.equals(outputKind) && outputValue != null) {
					insertSavedFile(conn, queryUtil, runId, nodeId, outputVar, outputValue);
				} else if (!AutomationConstants.OUTPUT_KIND_TABLE.equals(outputKind) && outputValue != null) {
					insertSavedValue(conn, queryUtil, runId, nodeId, outputVar, outputValue);
				}

				try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_SUCCESS)) {
					int index = 1;
					ps.setString(index++, NODE_STATUS_SUCCESS);
					ps.setTimestamp(index++, startedAt);
					ps.setTimestamp(index++, toTimestamp(Instant.now()));
					ps.setLong(index++, durationMs);
					ps.setString(index++, outputVar);
					queryUtil.setNullableString(ps, index++, outputKind);
					// Handle CLOB for potentially large output values
					queryUtil.setNullableLargeText(ps, index++, outputValue);
					ps.setString(index++, outputPreview);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, modelMessageId);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, agentRunId);
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					requireSingleRow(ps.executeUpdate(), "persist the successful node result", runId, nodeId);
				}
				if (effectId != null) {
					try (PreparedStatement effect = conn.prepareStatement(
							"UPDATE AUTOMATION_EFFECTS SET EFFECT_STATE=?,COMMITTED_AT=? WHERE EFFECT_ID=? "
									+ "AND RUN_ID=? AND NODE_ID=? AND EFFECT_STATE IN (?,?)")) {
						effect.setString(1, AutomationConstants.EFFECT_STATE_COMMITTED);
						effect.setTimestamp(2, toTimestamp(Instant.now()));
						effect.setString(3, effectId);
						effect.setString(4, runId);
						effect.setString(5, nodeId);
						effect.setString(6, AutomationConstants.EFFECT_STATE_PREPARED);
						effect.setString(7, AutomationConstants.EFFECT_STATE_ACCEPTED);
						requireSingleRow(effect.executeUpdate(), "commit the automation effect", runId, nodeId);
					}
				}

				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to update node success for run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the successful automation node result.", e);
		}
	}

	private static void insertSavedTable(Connection conn, AbstractSqlQueryUtil queryUtil, String runId, String nodeId,
			String outputVar, AutomationTableCaptureService.TableCapture capture) throws SQLException {
		String resultId = UUID.randomUUID().toString();
		insertSavedResultMetadata(conn, queryUtil, resultId, runId, nodeId, outputVar,
				AutomationConstants.RESULT_KIND_TABLE, capture.rowCount(), capture.byteCount(), capture.checksum(),
				capture.schema());
		try (PreparedStatement ps = conn.prepareStatement(INSERT_RESULT_CHUNK)) {
			for (AutomationTableCaptureService.TableChunk chunk : capture.chunks()) {
				int index = 1;
				ps.setString(index++, resultId);
				ps.setInt(index++, chunk.index());
				ps.setLong(index++, chunk.firstRow());
				ps.setLong(index++, chunk.rowCount());
				ps.setLong(index++, chunk.byteCount());
				ps.setString(index++, chunk.checksum());
				queryUtil.setNullableLargeText(ps, index++, chunk.payload());
				ps.addBatch();
			}
			if (!capture.chunks().isEmpty()) {
				int[] counts = ps.executeBatch();
				if (counts.length != capture.chunks().size()) {
					throw new SQLException("Not all saved-result chunks were persisted.");
				}
			}
		}
	}

	private static void insertSavedValue(Connection conn, AbstractSqlQueryUtil queryUtil, String runId, String nodeId,
			String outputVar, String outputValue) throws SQLException {
		byte[] payload = outputValue.getBytes(StandardCharsets.UTF_8);
		insertSavedResultMetadata(conn, queryUtil, UUID.randomUUID().toString(), runId, nodeId, outputVar,
				AutomationConstants.RESULT_KIND_VALUE, null, payload.length, sha256(payload), outputValue);
	}

	private static void insertSavedFile(Connection conn, AbstractSqlQueryUtil queryUtil, String runId, String nodeId,
			String outputVar, String outputValue) throws SQLException {
		byte[] payload = outputValue.getBytes(StandardCharsets.UTF_8);
		insertSavedResultMetadata(conn, queryUtil, UUID.randomUUID().toString(), runId, nodeId, outputVar,
				AutomationConstants.RESULT_KIND_FILE, null, payload.length, sha256(payload), outputValue);
	}

	private static void insertSavedResultMetadata(Connection conn, AbstractSqlQueryUtil queryUtil, String resultId,
			String runId, String nodeId, String outputVar, String resultKind, Long rowCount, long byteCount,
			String checksum, String payload) throws SQLException {
		Timestamp created = toTimestamp(Instant.now());
		try (PreparedStatement ps = conn.prepareStatement(INSERT_SAVED_VALUE)) {
			int index = 1;
			ps.setString(index++, resultId);
			ps.setString(index++, resultKind);
			ps.setInt(index++, AutomationConstants.SAVED_RESULT_FORMAT_VERSION);
			if (rowCount == null) {
				ps.setNull(index++, Types.BIGINT);
			} else {
				ps.setLong(index++, rowCount);
			}
			ps.setLong(index++, byteCount);
			ps.setString(index++, checksum);
			ps.setTimestamp(index++, created);
			ps.setTimestamp(index++, toTimestamp(created.toInstant()
					.plusSeconds(AutomationConstants.SAVED_RESULT_RETENTION_DAYS * 24L * 60 * 60)));
			ps.setString(index++, AutomationConstants.CAPTURE_STATE_RETAINED);
			ps.setString(index++, AutomationConstants.RESULT_ROLE_OUTPUT);
			queryUtil.setNullableString(ps, index++, outputVar);
			queryUtil.setNullableLargeText(ps, index++, payload);
			ps.setString(index++, runId);
			ps.setString(index++, nodeId);
			requireSingleRow(ps.executeUpdate(), "persist the saved automation value", runId, nodeId);
		}
	}

	private static String sha256(byte[] value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable.", e);
		}
	}

	private static String requiredLease(String runId) {
		String lease = CLAIMED_LEASES.get().get(runId);
		if (lease == null) {
			lease = ACTIVE_RUN_LEASES.get(runId);
		}
		if (lease == null) {
			throw new IllegalStateException("Automation run '" + runId + "' is not leased by this worker.");
		}
		return lease;
	}

	private static void fenceActiveLease(Connection conn, String runId) throws SQLException {
		ItemClaim itemClaim = currentItemClaim(runId);
		if (itemClaim != null) {
			fenceItemLease(conn, itemClaim);
			return;
		}
		try (PreparedStatement ps = conn.prepareStatement(FENCE_ACTIVE_LEASE)) {
			ps.setString(1, runId);
			ps.setString(2, STATUS_RUNNING);
			ps.setString(3, requiredLease(runId));
			requireSingleRow(ps.executeUpdate(), "fence the active run lease", runId, null);
		}
	}

	private static ItemClaim currentItemClaim(String runId) {
		ItemClaim itemClaim = claimedItem(runId);
		boolean hasRunLease = CLAIMED_LEASES.get().containsKey(runId) || ACTIVE_RUN_LEASES.containsKey(runId);
		return itemClaim != null && !hasRunLease ? itemClaim : null;
	}

	private static ItemClaim claimedItem(String runId) {
		ItemClaim itemClaim = CLAIMED_ITEM.get();
		return itemClaim != null && itemClaim.runId().equals(runId) ? itemClaim : null;
	}

	/**
	 * Persists the durable agent-run ID as soon as an asynchronous child is
	 * submitted, while the automation node remains RUNNING. This makes active agent
	 * nodes inspectable before terminal output exists.
	 */
	public static void updateNodeAgentRunTrace(String runId, String nodeId, String agentRunId) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting the automation agent run trace");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_AGENT_RUN_TRACE)) {
					schedulerDb.getQueryUtil().setNullableString(ps, 1, agentRunId);
					ps.setString(2, runId);
					ps.setString(3, nodeId);
					requireSingleRow(ps.executeUpdate(), "persist the agent run trace", runId, nodeId);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to persist agent run trace for run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the automation agent run trace.", e);
		}
	}

	/**
	 * Updates a node output after failed execution.
	 */
	public static void updateNodeFailed(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String errorMessage) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting the failed automation node result");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_FAILED)) {
					int index = 1;
					ps.setString(index++, NODE_STATUS_FAILED);
					ps.setTimestamp(index++, startedAt);
					ps.setTimestamp(index++, toTimestamp(Instant.now()));
					ps.setLong(index++, durationMs);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, errorMessage);
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					requireSingleRow(ps.executeUpdate(), "persist the failed node result", runId, nodeId);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to update node failed for run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the failed automation node result.", e);
		}
	}

	/**
	 * Persists a failed durable agent result without discarding the run reference
	 * or terminal output. This keeps Agent Activity reachable from failed,
	 * cancelled, and timed-out automation nodes.
	 */
	public static void updateNodeFailedWithResult(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String outputVar, String outputValue, String outputPreview, String agentRunId, String errorMessage) {
		updateNodeFailedWithResult(runId, nodeId, startedAt, durationMs, outputVar, null, outputValue, outputPreview,
				agentRunId, errorMessage);
	}

	/** Persists a failed durable result and its internal storage kind. */
	public static void updateNodeFailedWithResult(String runId, String nodeId, Timestamp startedAt, long durationMs,
			String outputVar, String outputKind, String outputValue, String outputPreview, String agentRunId,
			String errorMessage) {
		IRDBMSEngine schedulerDb = requireSchedulerDb("persisting the failed automation agent result");

		try {
			QueryExecutionUtility.write(schedulerDb, conn -> {
				fenceActiveLease(conn, runId);
				AbstractSqlQueryUtil queryUtil = schedulerDb.getQueryUtil();
				try (PreparedStatement ps = conn.prepareStatement(UPDATE_NODE_OUTPUT_FAILED_WITH_RESULT)) {
					int index = 1;
					ps.setString(index++, NODE_STATUS_FAILED);
					ps.setTimestamp(index++, startedAt);
					ps.setTimestamp(index++, toTimestamp(Instant.now()));
					ps.setLong(index++, durationMs);
					ps.setString(index++, outputVar);
					queryUtil.setNullableString(ps, index++, outputKind);
					queryUtil.setNullableLargeText(ps, index++, outputValue);
					ps.setString(index++, outputPreview);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, agentRunId);
					schedulerDb.getQueryUtil().setNullableString(ps, index++, errorMessage);
					ps.setString(index++, runId);
					ps.setString(index++, nodeId);
					requireSingleRow(ps.executeUpdate(), "persist the failed agent node result", runId, nodeId);
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to update agent node failure for run '{}', node '{}'", runId, nodeId, e);
			throw new IllegalStateException("Unable to persist the failed automation agent result.", e);
		}
	}

	/**
	 * Gets all node outputs for a run, ordered by execution order.
	 */
	public static List<Map<String, Object>> getNodeOutputsForRun(String runId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return new ArrayList<>();
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + RUN_ID, RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + NODE_ID, NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + NODE_LABEL, NODE_LABEL));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + EXECUTION_ORDER, EXECUTION_ORDER));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + STATUS, STATUS));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + STARTED_AT, STARTED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + COMPLETED_AT, COMPLETED_AT));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + DURATION_MS, DURATION_MS));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_VAR_NAME, OUTPUT_VAR_NAME));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_KIND, OUTPUT_KIND));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_VALUE, OUTPUT_VALUE));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + OUTPUT_PREVIEW, OUTPUT_PREVIEW));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + ROOM_ID, ROOM_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + WORKSPACE_ID, WORKSPACE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + MODEL_MESSAGE_ID, MODEL_MESSAGE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + AGENT_RUN_ID, AGENT_RUN_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + AutomationConstants.PARENT_NODE_ID,
				AutomationConstants.PARENT_NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + AutomationConstants.ITERATION_INDEX,
				AutomationConstants.ITERATION_INDEX));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + AutomationConstants.SOURCE_NODE_ID,
				AutomationConstants.SOURCE_NODE_ID));
		qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + ERROR_MESSAGE, ERROR_MESSAGE));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_NODE_OUTPUTS + "__" + RUN_ID, "==", runId,
				PixelDataType.CONST_STRING));
		qs.addOrderBy(TABLE_NODE_OUTPUTS + "__" + EXECUTION_ORDER,
				QueryColumnOrderBySelector.ORDER_BY_DIRECTION.ASC.toString());

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(schedulerDb, qs);
		return results != null ? results : new ArrayList<>();
	}

	/**
	 * Confirms that a durable agent run was explicitly persisted as the trace for
	 * one exact Automation project run and node. This is intentionally an existence
	 * check rather than an agent-run lookup: callers must first establish the
	 * requesting user's project permission.
	 *
	 * @param projectId       Automation project identifier
	 * @param automationRunId Automation run identifier
	 * @param nodeId          Automation node identifier
	 * @param agentRunId      durable agent run identifier
	 * @return {@code true} only when all four identifiers describe one persisted
	 *         trace row
	 */
	public static boolean hasAgentRunTrace(String projectId, String automationRunId, String nodeId, String agentRunId) {
		IRDBMSEngine schedulerDb = getSchedulerDb();
		if (schedulerDb == null) {
			return false;
		}

		try {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(TABLE_NODE_OUTPUTS + "__" + RUN_ID, RUN_ID));
			qs.addRelation(TABLE_NODE_OUTPUTS + "__" + RUN_ID, TABLE_RUNS + "__" + RUN_ID, "inner.join");
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_RUNS + "__" + PROJECT_ID, "==", projectId,
					PixelDataType.CONST_STRING));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_NODE_OUTPUTS + "__" + RUN_ID, "==",
					automationRunId, PixelDataType.CONST_STRING));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_NODE_OUTPUTS + "__" + NODE_ID, "==", nodeId,
					PixelDataType.CONST_STRING));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TABLE_NODE_OUTPUTS + "__" + AGENT_RUN_ID, "==",
					agentRunId, PixelDataType.CONST_STRING));
			qs.setLimit(1L);
			return !QueryExecutionUtility.flushRsToMap(schedulerDb, qs).isEmpty();
		} catch (Exception e) {
			throw new IllegalStateException("Unable to verify the Automation agent-run trace.", e);
		}
	}

	// -- Result Assembly
	// -----------------------------------------------------------

	/**
	 * Builds a list of per-node result maps from the raw DB output rows returned by
	 * {@link #getNodeOutputsForRun(String)}. The shape matches what
	 * {@link GetAutomationRunReactor} and {@link TriggerAutomationReactor} return
	 * to callers.
	 *
	 * <p>
	 * Each entry contains: nodeId, nodeLabel, status, durationMs, a bounded display
	 * preview, an optional small outputValue, errorMessage, and an optional trace
	 * map. Large values remain server-owned and are not copied into run-detail
	 * responses.
	 *
	 * @param nodeOutputs ordered rows from {@link #getNodeOutputsForRun(String)}
	 * @return mutable list of node result maps (empty when {@code nodeOutputs} is
	 *         null)
	 */
	public static List<Map<String, Object>> buildNodeResults(List<Map<String, Object>> nodeOutputs) {
		List<Map<String, Object>> nodeResults = new ArrayList<>();
		Map<String, Map<String, Object>> parentResults = new LinkedHashMap<>();
		Map<String, Map<Integer, List<Map<String, Object>>>> loopIterations = new LinkedHashMap<>();
		if (nodeOutputs == null) {
			return nodeResults;
		}
		for (Map<String, Object> output : nodeOutputs) {
			Map<String, Object> nodeResult = new LinkedHashMap<>();
			Object parentNodeId = output.get(AutomationConstants.PARENT_NODE_ID);
			Object sourceNodeId = output.get(AutomationConstants.SOURCE_NODE_ID);
			nodeResult.put(AutomationConstants.NODE_ID,
					sourceNodeId == null ? output.get(AutomationConstants.NODE_ID) : sourceNodeId);
			nodeResult.put(AutomationConstants.NODE_LABEL, output.get(AutomationConstants.NODE_LABEL));
			nodeResult.put(AutomationConstants.STATUS, output.get(AutomationConstants.STATUS));
			nodeResult.put(AutomationConstants.DURATION_MS, output.get(AutomationConstants.DURATION_MS));
			nodeResult.put(AutomationConstants.OUTPUT_KIND, output.get(AutomationConstants.OUTPUT_KIND));
			String outputForDisplay = (String) output.get(AutomationConstants.OUTPUT_PREVIEW);
			String outputValue = (String) output.get(AutomationConstants.OUTPUT_VALUE);
			if ((outputForDisplay == null || outputForDisplay.isBlank()) && outputValue != null
					&& outputValue.length() <= AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH) {
				outputForDisplay = outputValue;
			}
			nodeResult.put(AutomationConstants.OUTPUT_PREVIEW, outputForDisplay);
			if (outputValue != null && outputValue.length() <= AutomationConstants.OUTPUT_PREVIEW_MAX_LENGTH) {
				nodeResult.put(AutomationConstants.OUTPUT_VALUE, outputValue);
			}
			nodeResult.put(AutomationConstants.ERROR_MESSAGE, output.get(AutomationConstants.ERROR_MESSAGE));
			Map<String, Object> trace = new LinkedHashMap<>();
			putIfPresent(trace, AutomationConstants.TRACE_AUTOMATION_RUN_ID, output.get(AutomationConstants.RUN_ID));
			putIfPresent(trace, AutomationConstants.TRACE_NODE_ID, output.get(AutomationConstants.NODE_ID));
			putIfPresent(trace, AutomationConstants.TRACE_ROOM_ID, output.get(AutomationConstants.ROOM_ID));
			putIfPresent(trace, AutomationConstants.TRACE_WORKSPACE_ID, output.get(AutomationConstants.WORKSPACE_ID));
			putIfPresent(trace, AutomationConstants.TRACE_MODEL_MESSAGE_ID,
					output.get(AutomationConstants.MODEL_MESSAGE_ID));
			putIfPresent(trace, AutomationConstants.TRACE_AGENT_RUN_ID, output.get(AutomationConstants.AGENT_RUN_ID));
			if (AutomationConstants.NODE_STATUS_WAITING_FOR_INPUT.equals(output.get(AutomationConstants.STATUS))) {
				trace.put(AutomationConstants.TRACE_AGENT_STATUS, "INPUT_REQUIRED");
			}
			if (!trace.isEmpty()) {
				nodeResult.put(AutomationConstants.RESULT_TRACE, trace);
			}
			if (parentNodeId == null) {
				nodeResults.add(nodeResult);
				parentResults.put(String.valueOf(output.get(AutomationConstants.NODE_ID)), nodeResult);
			} else {
				Object iterationValue = output.get(AutomationConstants.ITERATION_INDEX);
				int iteration = iterationValue instanceof Number number ? number.intValue()
						: Integer.parseInt(String.valueOf(iterationValue));
				loopIterations.computeIfAbsent(String.valueOf(parentNodeId), ignored -> new LinkedHashMap<>())
						.computeIfAbsent(iteration, ignored -> new ArrayList<>()).add(nodeResult);
			}
		}
		for (Map.Entry<String, Map<Integer, List<Map<String, Object>>>> parent : loopIterations.entrySet()) {
			Map<String, Object> parentResult = parentResults.get(parent.getKey());
			if (parentResult == null) {
				continue;
			}
			List<Map<String, Object>> iterations = new ArrayList<>();
			for (Map.Entry<Integer, List<Map<String, Object>>> iteration : parent.getValue().entrySet()) {
				iterations.add(Map.of("index", iteration.getKey(), "nodeResults", iteration.getValue()));
			}
			parentResult.put("iterations", iterations);
		}
		return nodeResults;
	}

	// -- Table Creation
	// ------------------------------------------------------------

	private static void putIfPresent(Map<String, Object> target, String key, Object value) {
		if (value != null && !value.toString().isBlank()) {
			target.put(key, value);
		}
	}

	// -- Helpers
	// -------------------------------------------------------------------

	private static IRDBMSEngine getSchedulerDb() {
		try {
			return SystemEngineRegistry.getSchedulerDb();
		} catch (Exception e) {
			classLogger.warn("Could not obtain scheduler DB: {}", e.getMessage());
			return null;
		}
	}

	private static IRDBMSEngine requireSchedulerDb(String operation) {
		try {
			SystemEngineRegistry.requireDatabase(Constants.SCHEDULER_DB, operation);
		} catch (IllegalArgumentException e) {
			// Automation persistence exposes unavailable infrastructure as an invalid
			// state.
			throw new IllegalStateException(e.getMessage(), e);
		}
		return SystemEngineRegistry.getSchedulerDb();
	}

	private static void closeConnection(IRDBMSEngine engine, Connection conn) {
		ConnectionUtils.closeAllConnectionsIfPooling(engine, conn);
	}

	private static void requireSingleRow(int updatedRows, String operation, String runId, String nodeId) {
		if (updatedRows == 1) {
			return;
		}
		String nodeContext = nodeId == null ? "" : ", node '" + nodeId + "'";
		throw new IllegalStateException("Unable to " + operation + " for run '" + runId + "'" + nodeContext
				+ ": expected one row but updated " + updatedRows + ".");
	}

	private static Timestamp toTimestamp(Instant instant) {
		return Utility.getSqlTimestampUTC(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
	}

	/**
	 * Best-effort conversion of a value read from the result set into a
	 * {@link Timestamp}. Handles {@link Timestamp}, any {@link Date}, and parseable
	 * timestamp strings. Returns null when the value is null or cannot be
	 * interpreted.
	 */
	private static Timestamp toTimestampSafe(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof SemossDate semossDate && semossDate.getLocalDateTime() != null) {
			return Timestamp.valueOf(semossDate.getLocalDateTime());
		}
		if (value instanceof Timestamp) {
			return (Timestamp) value;
		}
		if (value instanceof Date) {
			return new Timestamp(((Date) value).getTime());
		}
		try {
			return Timestamp.valueOf(value.toString().trim());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	private static void addPrimaryKeyIfNotExists(Connection conn, AbstractSqlQueryUtil queryUtil, String tableName,
			String database, String schema, String pkName, String[] columns) {
		try {
			if (queryUtil.tableConstraintExists(conn, pkName, tableName, database, schema)) {
				return;
			}
			if (queryUtil.allowIfExistsAddConstraint()) {
				String colList = String.join(", ", columns);
				String sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT IF NOT EXISTS " + pkName + " PRIMARY KEY ("
						+ colList + ")";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.execute();
				}
			} else {
				String colList = String.join(", ", columns);
				String sql = "ALTER TABLE " + tableName + " ADD CONSTRAINT " + pkName + " PRIMARY KEY (" + colList
						+ ")";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.execute();
				}
			}
		} catch (Exception e) {
			classLogger.debug("Primary key {} may already exist on {}: {}", pkName, tableName, e.getMessage());
		}
	}

	private static void createIndexIfNotExists(Connection conn, AbstractSqlQueryUtil queryUtil, boolean allowIfExists,
			String indexName, String tableName, String[] columns) {
		try {
			List<String> colList = Arrays.asList(columns);
			String sql;
			if (allowIfExists && queryUtil.allowIfExistsIndexSyntax()) {
				sql = queryUtil.createIndexIfNotExists(indexName, tableName, colList);
			} else {
				sql = queryUtil.createIndex(indexName, tableName, colList);
			}
			if (sql != null && !sql.isEmpty()) {
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					ps.execute();
				}
			}
		} catch (Exception e) {
			classLogger.debug("Index {} may already exist on {}: {}", indexName, tableName, e.getMessage());
		}
	}

}
