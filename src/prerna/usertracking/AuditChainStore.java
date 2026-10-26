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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import com.google.gson.GsonBuilder;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;

/**
 * Database-owned append order. Legacy rows remain outside the version-two
 * chain.
 */
public final class AuditChainStore {

	static final String TABLE = "USER_AUDIT_CHAIN_STATE";
	public static final String SEQUENCE = "AUDIT_SEQUENCE";

	private AuditChainStore() {
	}

	/** Run after the audit schema migration, before accepting audit writes. */
	public static void initialize(IRDBMSEngine engine) throws Exception {
		if (!QueryExecutionUtility.read(engine,
				c -> engine.getQueryUtil().tableExists(c, TABLE, engine.getDatabase(), engine.getSchema()))) {
			try {
				QueryExecutionUtility.write(engine, c -> {
					try (var statement = c.createStatement()) {
						statement.execute(engine.getQueryUtil().createTableWithCustomConstraints(TABLE,
								new String[] { "CHAIN_ID", "LAST_SEQUENCE", "LAST_HASH" },
								new String[] { "VARCHAR(50)", "BIGINT", "VARCHAR(100)" },
								new Object[] { "PRIMARY KEY", "NOT NULL", null }));
					}
					return null;
				});
			} catch (Exception failure) {
				// Another instance can win initialization; never ignore other DDL failures.
				if (!QueryExecutionUtility.read(engine,
						c -> engine.getQueryUtil().tableExists(c, TABLE, engine.getDatabase(), engine.getSchema()))) {
					throw failure;
				}
			}
		}
		if (QueryExecutionUtility.queryOne(engine, "SELECT LAST_SEQUENCE FROM " + TABLE + " WHERE CHAIN_ID='GLOBAL'",
				QueryExecutionUtility.NO_PARAMETERS, r -> r.getLong(1)) == null) {
			Long existing = QueryExecutionUtility.queryOne(engine, "SELECT MAX(AUDIT_SEQUENCE) FROM USER_AUDIT_EVENTS",
					QueryExecutionUtility.NO_PARAMETERS, r -> r.getObject(1) == null ? null : r.getLong(1));
			if (existing != null) {
				if (QueryExecutionUtility.queryOne(engine,
						"SELECT LAST_SEQUENCE FROM " + TABLE + " WHERE CHAIN_ID='GLOBAL'",
						QueryExecutionUtility.NO_PARAMETERS, r -> r.getLong(1)) == null) {
					throw new IllegalStateException(
							"Audit chain head is missing; restore it from a trusted backup before accepting writes");
				}
				return;
			}
			try {
				QueryExecutionUtility.executeUpdate(engine,
						"INSERT INTO " + TABLE + " (CHAIN_ID,LAST_SEQUENCE,LAST_HASH) VALUES ('GLOBAL',0,NULL)",
						QueryExecutionUtility.NO_PARAMETERS);
			} catch (Exception failure) {
				if (QueryExecutionUtility.queryOne(engine,
						"SELECT LAST_SEQUENCE FROM " + TABLE + " WHERE CHAIN_ID='GLOBAL'",
						QueryExecutionUtility.NO_PARAMETERS, r -> r.getLong(1)) == null) {
					throw failure;
				}
			}
		}
	}

	/**
	 * The UPDATE acquires a cross-process write lock, held until the caller
	 * commits.
	 */
	static Head advance(Connection connection) throws Exception {
		int updated = QueryExecutionUtility.executeUpdate(connection,
				"UPDATE " + TABLE + " SET LAST_SEQUENCE=LAST_SEQUENCE+1 WHERE CHAIN_ID='GLOBAL'",
				QueryExecutionUtility.NO_PARAMETERS);
		if (updated != 1) {
			throw new IllegalStateException("Audit chain is not initialized");
		}
		return QueryExecutionUtility.queryOne(connection,
				"SELECT LAST_SEQUENCE,LAST_HASH FROM " + TABLE + " WHERE CHAIN_ID='GLOBAL'",
				QueryExecutionUtility.NO_PARAMETERS, r -> new Head(r.getLong(1), r.getString(2)));
	}

	public static long snapshot(IRDBMSEngine engine) throws Exception {
		Long sequence = QueryExecutionUtility.queryOne(engine,
				"SELECT LAST_SEQUENCE FROM " + TABLE + " WHERE CHAIN_ID='GLOBAL'", QueryExecutionUtility.NO_PARAMETERS,
				r -> r.getLong(1));
		if (sequence == null) {
			throw new IllegalStateException("Audit chain is not initialized");
		}
		return sequence;
	}

	static String hashPersistedRow(Connection connection, Head head) throws Exception {
		return QueryExecutionUtility.queryOne(connection,
				"SELECT " + String.join(",", UserAuditTrailUtils.COLUMNS)
						+ " FROM USER_AUDIT_EVENTS WHERE AUDIT_SEQUENCE=?",
				p -> p.setLong(1, head.sequence()), r -> hashRow(r, head));
	}

	private static String hashRow(ResultSet row, Head head) throws Exception {
		List<Object> canonical = new ArrayList<>();
		canonical.add("audit-v2");
		canonical.add(head.sequence());
		canonical.add(head.hash());
		for (String column : UserAuditTrailUtils.COLUMNS) {
			if (column.startsWith("HASH_")) {
				continue;
			}
			canonical.add(column);
			Object value = switch (column) {
			case "EVENT_TIME", "EVENT_OCCURRED_TIME" ->
				row.getTimestamp(column) == null ? null : row.getTimestamp(column).toString();
			case "ACTOR_IS_ADMIN" -> row.getObject(column) == null ? null : row.getBoolean(column);
			case "HTTP_STATUS" -> row.getObject(column) == null ? null : row.getInt(column);
			default -> row.getString(column);
			};
			canonical.add(value);
		}
		String json = new GsonBuilder().serializeNulls().disableHtmlEscaping().create().toJson(canonical);
		return "sha256:v2:" + HexFormat.of()
				.formatHex(MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8)));
	}

	/**
	 * Verify the committed v2 prefix; unsequenced legacy rows are intentionally not
	 * certified.
	 */
	public static long verify(IRDBMSEngine engine) throws Exception {
		return QueryExecutionUtility.read(engine, c -> {
			Head head = QueryExecutionUtility.queryOne(c,
					"SELECT LAST_SEQUENCE,LAST_HASH FROM " + TABLE + " WHERE CHAIN_ID='GLOBAL'",
					QueryExecutionUtility.NO_PARAMETERS, r -> new Head(r.getLong(1), r.getString(2)));
			if (head == null) {
				throw new IllegalStateException("Audit chain is not initialized");
			}
			long expected = 1;
			String previous = null;
			try (PreparedStatement ps = c.prepareStatement("SELECT " + String.join(",", UserAuditTrailUtils.COLUMNS)
					+ ",AUDIT_SEQUENCE FROM USER_AUDIT_EVENTS WHERE AUDIT_SEQUENCE<=? ORDER BY AUDIT_SEQUENCE")) {
				ps.setLong(1, head.sequence());
				try (ResultSet rows = ps.executeQuery()) {
					while (rows.next()) {
						if (rows.getLong(SEQUENCE) != expected
								|| !java.util.Objects.equals(previous, rows.getString("HASH_PREVIOUS"))
								|| !java.util.Objects.equals(hashRow(rows, new Head(expected, previous)),
										rows.getString("HASH_CURRENT"))) {
							throw new IllegalStateException("Audit integrity check failed at sequence " + expected);
						}
						previous = rows.getString("HASH_CURRENT");
						expected++;
					}
				}
			}
			if (expected - 1 != head.sequence() || !java.util.Objects.equals(previous, head.hash())) {
				throw new IllegalStateException("Audit integrity check failed at the chain head");
			}
			return head.sequence();
		});
	}

	record Head(long sequence, String hash) {
	}
}
