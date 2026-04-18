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
import prerna.util.Constants;

public class PGVectorQueryUtil extends PostgresQueryUtil {
	
	private static final Logger classLogger = LogManager.getLogger(PGVectorQueryUtil.class);

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
			classLogger.warn("Unable to create the vector extension");
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}
	
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
	 * Add tsvector column and GIN index for full-text search hybrid queries.
	 * Safe to call on existing tables — uses IF NOT EXISTS / checks column existence.
	 */
	public String addFullTextSearchColumn(String table) {
		return "DO $$ BEGIN "
				+ "IF NOT EXISTS ("
				+ "  SELECT 1 FROM information_schema.columns "
				+ "  WHERE table_name = lower('" + table + "') AND column_name = 'content_tsv'"
				+ ") THEN "
				+ "  ALTER TABLE " + table + " ADD COLUMN content_tsv TSVECTOR "
				+ "  GENERATED ALWAYS AS (to_tsvector('english', COALESCE(CONTENT, ''))) STORED; "
				+ "END IF; "
				+ "END $$;";
	}

	/**
	 * Create a GIN index on the tsvector column for efficient full-text search.
	 */
	public String createFullTextSearchIndex(String table) {
		return "CREATE INDEX IF NOT EXISTS idx_" + table.toLowerCase() + "_content_tsv "
				+ "ON " + table + " USING GIN (content_tsv);";
	}

	/**
	 * Build a hybrid search query that combines dense vector similarity with
	 * sparse keyword (full-text) search using Reciprocal Rank Fusion (RRF).
	 * 
	 * @param table         the embeddings table name
	 * @param queryVector   the query embedding vector as a string (e.g., "[0.1, 0.2, ...]")
	 * @param queryText     the original search query text
	 * @param distanceMethod "Cosine Similarity" or "Euclidean"
	 * @param limit         max results
	 * @return hybrid SQL query string
	 */
	public String buildHybridSearchQuery(String table, String queryVector, String queryText,
			String distanceMethod, int limit) {
		String sanitizedQuery = queryText.replace("'", "''");
		int rrfK = 60;
		int retrievalWindow = Math.max(limit * 3, 20);

		String vectorScoreExpr;
		String vectorOrderDir;
		if ("Cosine Similarity".equalsIgnoreCase(distanceMethod)) {
			vectorScoreExpr = "1 - (EMBEDDING <=> '" + queryVector + "')";
			vectorOrderDir = "EMBEDDING <=> '" + queryVector + "'";
		} else {
			vectorScoreExpr = "POWER((EMBEDDING <-> '" + queryVector + "'), 2)";
			vectorOrderDir = "EMBEDDING <-> '" + queryVector + "'";
		}

		return "WITH semantic AS ("
				+ "  SELECT ID, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT, "
				+ vectorScoreExpr + " AS vector_score, "
				+ "  ROW_NUMBER() OVER (ORDER BY " + vectorOrderDir + ") AS rank "
				+ "  FROM " + table
				+ "  ORDER BY " + vectorOrderDir
				+ "  LIMIT " + retrievalWindow
				+ "), "
				+ "keyword AS ("
				+ "  SELECT ID, SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT, "
				+ "  ts_rank(content_tsv, plainto_tsquery('english', '" + sanitizedQuery + "')) AS keyword_score, "
				+ "  ROW_NUMBER() OVER (ORDER BY ts_rank(content_tsv, plainto_tsquery('english', '" + sanitizedQuery + "')) DESC) AS rank "
				+ "  FROM " + table
				+ "  WHERE content_tsv @@ plainto_tsquery('english', '" + sanitizedQuery + "')"
				+ "  LIMIT " + retrievalWindow
				+ ") "
				+ "SELECT COALESCE(s.ID, k.ID) AS ID, "
				+ "  COALESCE(s.SOURCE, k.SOURCE) AS Source, "
				+ "  COALESCE(s.MODALITY, k.MODALITY) AS Modality, "
				+ "  COALESCE(s.DIVIDER, k.DIVIDER) AS Divider, "
				+ "  COALESCE(s.PART, k.PART) AS Part, "
				+ "  COALESCE(s.TOKENS, k.TOKENS) AS Tokens, "
				+ "  COALESCE(s.CONTENT, k.CONTENT) AS Content, "
				+ "  COALESCE(1.0/(" + rrfK + " + s.rank), 0) + COALESCE(1.0/(" + rrfK + " + k.rank), 0) AS Score "
				+ "FROM semantic s FULL OUTER JOIN keyword k ON s.ID = k.ID "
				+ "ORDER BY Score DESC "
				+ "LIMIT " + limit;
	}
	
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
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

}
