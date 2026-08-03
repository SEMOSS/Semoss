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
package prerna.util.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;

public class PGVectorQueryUtil extends PostgresQueryUtil {

	private static final Logger classLogger = LogManager.getLogger(PGVectorQueryUtil.class);

	/** Name of the generated column used for PostgreSQL full-text search. */
	public static final String CONTENT_TSV_COLUMN = "content_tsv";

	private static final String HNSW_INDEX_SUFFIX = "_hnsw_idx";
	private static final String GIN_INDEX_SUFFIX = "_fts_idx";

	public PGVectorQueryUtil() {
		super();
	}

	public PGVectorQueryUtil(String connectionUrl, String username, String password) {
		super();
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		try (Statement stmt = con.createStatement()) {
			stmt.execute(addVectorExtension());
		} catch(SQLException e) {
			classLogger.error("Failed to ensure pgvector extension exists using SQL '{}'", addVectorExtension(), e);
		}
	}

	/**
	 * Returns SQL that enables the pgvector extension in the current database if it
	 * does not already exist.
	 *
	 * @return CREATE EXTENSION SQL string
	 */
	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}

	/**
	 * Returns SQL that creates the embeddings table if it does not already exist.
	 * The table stores the embedding vector alongside source metadata and the raw
	 * text content used to produce it.
	 *
	 * @param table embeddings table name
	 * @return CREATE TABLE SQL string
	 */
	public String createEmbeddingsTable(String table) {
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "
				+ "EMBEDDING VECTOR, "
				+ "SOURCE TEXT, "
				+ "MODALITY TEXT, "
				+ "DIVIDER TEXT, "
				+ "PART TEXT, "
				+ "TOKENS INTEGER, "
				+ "CONTENT TEXT "
				+ ");";
	}
	
	/**
	 * Returns SQL that creates the embeddings metadata table if it does not already
	 * exist. Each row stores one key-value attribute for a source document, typed
	 * across string, integer, numeric, boolean, date, and timestamp columns.
	 *
	 * @param table metadata table name
	 * @return CREATE TABLE SQL string
	 */
	public String createEmbeddingsMetadataTable(String table) {
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "
				+ "SOURCE TEXT, "
				+ "ATTRIBUTE TEXT, "
				+ "STR_VALUE TEXT, "
				+ "INT_VALUE INTEGER, "
				+ "NUM_VALUE NUMERIC(18,4), "
				+ "BOOL_VALUE BOOLEAN, "
				+ "DATE_VAL DATE, "
				+ "TIMESTAMP_VAL TIMESTAMP "
				+ ");";
	}

	/**
	 * Returns SQL that adds a plain {@code tsvector} column for full-text search if
	 * it does not already exist. Rows already in the table will have a {@code NULL}
	 * value until {@link #backfillFtsColumn} is called. The column is language-neutral;
	 * the text search configuration is applied at insert time and at query time.
	 *
	 * @param table embeddings table name
	 * @return ALTER TABLE SQL string
	 */
	public String addFtsColumn(String table) {
		return "ALTER TABLE " + table + " ADD COLUMN IF NOT EXISTS " + CONTENT_TSV_COLUMN + " tsvector;";
	}

	/**
	 * Returns SQL that populates the full-text search column for all rows where it
	 * is currently {@code NULL}. Safe to run repeatedly; already-populated rows are
	 * not touched.
	 *
	 * @param table    embeddings table name
	 * @param language PostgreSQL text search configuration (e.g. {@code "english"},
	 *                 {@code "simple"})
	 * @return UPDATE SQL string
	 */
	public String backfillFtsColumn(String table, String language) {
		return "UPDATE " + table + " SET " + CONTENT_TSV_COLUMN
				+ " = to_tsvector('" + language + "', COALESCE(CONTENT, ''))"
				+ " WHERE " + CONTENT_TSV_COLUMN + " IS NULL;";
	}

	/**
	 * Returns SQL that creates a GIN index on the full-text search column if it
	 * does not already exist.
	 *
	 * @param table embeddings table name
	 * @return CREATE INDEX SQL string
	 */
	public String createFtsIndex(String table) {
		return "CREATE INDEX IF NOT EXISTS " + table + GIN_INDEX_SUFFIX
				+ " ON " + table + " USING GIN(" + CONTENT_TSV_COLUMN + ");";
	}

	/**
	 * Returns SQL that creates an HNSW approximate-nearest-neighbour index on the
	 * embedding column if it does not already exist. Building the index is
	 * synchronous; on large tables the first startup after deploying this change
	 * will block briefly.
	 *
	 * @param table     embeddings table name
	 * @param indexOps  pgvector operator class — {@code "vector_cosine_ops"} for
	 *                  cosine similarity or {@code "vector_l2_ops"} for Euclidean
	 * @return CREATE INDEX SQL string
	 */
	public String createEmbeddingHnswIndex(String table, String indexOps) {
		return "CREATE INDEX IF NOT EXISTS " + table + HNSW_INDEX_SUFFIX
				+ " ON " + table + " USING hnsw (EMBEDDING " + indexOps + ");";
	}

	public void createOWL(PGVectorDatabaseEngine engine, String embeddingsTable, String metadataTable) {
		try(WriteOWLEngine writer = engine.getOWLEngineFactory().getWriteOWL()) {
			writer.createEmptyOWLFile();
			
			writer.addConcept(embeddingsTable);
			writer.addProp(embeddingsTable, "ID", "IDENTITY");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.SOURCE, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.MODALITY, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.DIVIDER, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.PART, "TEXT");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.TOKENS, "INTEGER");
			writer.addProp(embeddingsTable, VectorDatabaseCSVTable.CONTENT, "TEXT");
			writer.addProp(embeddingsTable, "EMBEDDING", "VECTOR");

			writer.addConcept(metadataTable);
			writer.addProp(metadataTable, "ID", "IDENTITY");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.SOURCE, "TEXT");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.ATTRIBUTE, "TEXT");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.STR_VALUE, "TEXT");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.INT_VALUE, "INTEGER");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.NUM_VALUE, "NUMERIC(18,4)");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.BOOL_VALUE, "BOOLEAN");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.DATE_VAL, "DATE");
			writer.addProp(metadataTable, VectorDatabaseMetadataCSVTable.TIMESTAMP_VAL, "TIMESTAMP");

			writer.addRelation(embeddingsTable, metadataTable, 
					embeddingsTable+"."+VectorDatabaseCSVTable.SOURCE+"."+metadataTable+"."+VectorDatabaseMetadataCSVTable.SOURCE);
			
			writer.commit();
			writer.export();
		} catch (IOException e) {
			classLogger.error(
					"Failed to create or export OWL metadata for embeddings table '{}' and metadata table '{}' due to an I/O error",
					embeddingsTable, metadataTable, e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			classLogger.error("Interrupted while creating OWL metadata for embeddings table '{}' and metadata table '{}'",
					embeddingsTable, metadataTable, e);
		} catch (Exception e) {
			classLogger.error("Unexpected error creating OWL metadata for embeddings table '{}' and metadata table '{}'",
					embeddingsTable, metadataTable, e);
		}
	}

}
