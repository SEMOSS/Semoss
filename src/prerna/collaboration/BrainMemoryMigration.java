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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.collaboration.BrainMemoryUtils.Memory;
import prerna.collaboration.BrainMemoryUtils.Ref;
import prerna.collaboration.BrainMemoryUtils.Source;

/**
 * Moves topic notes (BRAIN_TOPIC_NOTE, KIND note) and thread facts (WORK_THREAD_FACT) into Brain memory, linked to
 * their topic or thread. Each row is inserted and its source deleted in one transaction under an id made from the
 * source row, so a crash or a second boot neither loses nor duplicates anything.
 */
final class BrainMemoryMigration {

	private static final Logger classLogger = LogManager.getLogger(BrainMemoryMigration.class);

	static final String NOTE_KIND = "note";
	private static final String CONFIRMED = "confirmed";

	private BrainMemoryMigration() {
	}

	static void run() {
		int notes = moveNotes();
		int facts = moveFacts();
		if (notes + facts > 0) {
			classLogger.info("Moved {} topic notes and {} thread facts into Brain memory", notes, facts);
		}
	}

	private static int moveNotes() {
		List<Map<String, Object>> rows = CollaborationDbUtils.query("SELECT OWNER_ID, OWNER_TYPE, NOTE_ID, TOPIC_ID, "
				+ "TEXT, STATE, ORIGIN, SOURCE_REF, CREATED_AT, UPDATED_AT FROM BRAIN_TOPIC_NOTE WHERE KIND = ?",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("ownerId", rs.getString("OWNER_ID"));
					row.put("ownerType", rs.getString("OWNER_TYPE"));
					row.put("id", rs.getString("NOTE_ID"));
					row.put("refId", rs.getString("TOPIC_ID"));
					row.put("text", CollaborationDbUtils.getString(rs, "TEXT"));
					row.put("state", rs.getString("STATE"));
					row.put("origin", rs.getString("ORIGIN"));
					row.put("sourceRef", rs.getString("SOURCE_REF"));
					row.put("createdAt", rs.getTimestamp("CREATED_AT"));
					row.put("updatedAt", rs.getTimestamp("UPDATED_AT"));
					return row;
				}, NOTE_KIND);
		int moved = 0;
		for (Map<String, Object> row : rows) {
			Memory memory = fromNote(row);
			if (move(row, memory, "DELETE FROM BRAIN_TOPIC_NOTE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND NOTE_ID = ?")) {
				moved++;
			}
		}
		return moved;
	}

	private static int moveFacts() {
		List<Map<String, Object>> rows = CollaborationDbUtils.query("SELECT OWNER_ID, OWNER_TYPE, FACT_ID, THREAD_ID, "
				+ "TEXT, FROM_LABEL, STATUS, SOURCE_PERSON_ID, CREATED_AT, UPDATED_AT FROM WORK_THREAD_FACT", rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("ownerId", rs.getString("OWNER_ID"));
					row.put("ownerType", rs.getString("OWNER_TYPE"));
					row.put("id", rs.getString("FACT_ID"));
					row.put("refId", rs.getString("THREAD_ID"));
					row.put("text", CollaborationDbUtils.getString(rs, "TEXT"));
					row.put("state", rs.getString("STATUS"));
					row.put("label", rs.getString("FROM_LABEL"));
					row.put("personId", rs.getString("SOURCE_PERSON_ID"));
					row.put("createdAt", rs.getTimestamp("CREATED_AT"));
					row.put("updatedAt", rs.getTimestamp("UPDATED_AT"));
					return row;
				});
		int moved = 0;
		for (Map<String, Object> row : rows) {
			Memory memory = fromFact(row);
			if (move(row, memory, "DELETE FROM WORK_THREAD_FACT WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND FACT_ID = ?")) {
				moved++;
			}
		}
		return moved;
	}

	// one source row: insert unless a run before this one already did, then drop the source
	private static boolean move(Map<String, Object> row, Memory memory, String deleteSource) {
		String ownerId = (String) row.get("ownerId");
		String ownerType = (String) row.get("ownerType");
		try {
			CollaborationDbUtils.inTransaction(conn -> {
				if (memory != null && CollaborationDbUtils.query(conn,
						"SELECT 1 FROM BRAIN_MEMORY WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MEMORY_ID = ?",
						rs -> Boolean.TRUE, ownerId, ownerType, memory.id()).isEmpty()) {
					BrainMemoryUtils.insert(conn, ownerId, ownerType, memory);
				}
				CollaborationDbUtils.update(conn, deleteSource, ownerId, ownerType, row.get("id"));
			});
			return memory != null;
		} catch (RuntimeException e) {
			classLogger.warn("Could not move {} into Brain memory; it stays where it is", row.get("id"), e);
			return false;
		}
	}

	/** A topic note as a fact about its topic; null for an empty note, which is just dropped. */
	static Memory fromNote(Map<String, Object> row) {
		String text = clean((String) row.get("text"));
		if (text == null) {
			return null;
		}
		boolean confirmed = CONFIRMED.equals(row.get("state"));
		String origin = row.get("origin") == null ? BrainProfileUtils.YOU : (String) row.get("origin");
		return memory(row, BrainMemoryUtils.FROM_TOPIC_NOTE, text, confirmed, origin, new Ref(BrainMemoryUtils.TOPIC,
				(String) row.get("refId")), new Source(BrainMemoryUtils.FROM_TOPIC_NOTE, null, null,
						(String) row.get("id"), null, (String) row.get("sourceRef")));
	}

	/** A thread fact as a fact about its thread, keeping who said it. */
	static Memory fromFact(Map<String, Object> row) {
		String text = clean((String) row.get("text"));
		if (text == null) {
			return null;
		}
		boolean confirmed = CONFIRMED.equals(row.get("state"));
		return memory(row, BrainMemoryUtils.FROM_THREAD_FACT, text, confirmed, BrainProfileUtils.YOU,
				new Ref(BrainMemoryUtils.THREAD, (String) row.get("refId")),
				new Source(BrainMemoryUtils.FROM_THREAD_FACT, (String) row.get("refId"), null, (String) row.get("id"),
						(String) row.get("personId"), (String) row.get("label")));
	}

	// confirmed rows stay in use; drafts become suggestions for the owner to accept
	private static Memory memory(Map<String, Object> row, String sourceKind, String text, boolean confirmed,
			String origin, Ref ref, Source source) {
		Timestamp created = (Timestamp) row.get("createdAt");
		Timestamp updated = row.get("updatedAt") == null ? created : (Timestamp) row.get("updatedAt");
		String id = BrainMemoryUtils.stableId((String) row.get("ownerId"), (String) row.get("ownerType"), "memory",
				sourceKind, (String) row.get("id"));
		return new Memory(id, BrainMemoryUtils.FACT, text,
				confirmed ? BrainMemoryUtils.ACTIVE : BrainMemoryUtils.SUGGESTED, origin, confirmed, false, null,
				null, source, created, updated, confirmed ? updated : null, ref.id() == null ? List.of() : List.of(ref));
	}

	// one line, cut to the memory limit; an old note never fails the move for being long
	private static String clean(String text) {
		if (text == null || text.isBlank()) {
			return null;
		}
		String line = text.replaceAll("\\s+", " ").trim();
		return line.length() > BrainMemoryUtils.MAX_CHARS ? line.substring(0, BrainMemoryUtils.MAX_CHARS).trim()
				: line;
	}
}
