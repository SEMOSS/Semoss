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
package prerna.engine.impl.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.reactor.database.upload.rdbms.external.ExternalUpdateJdbcSchemaReactor;
import prerna.reactor.database.upload.rdbms.external.RdbmsExternalUploadReactor;
import prerna.reactor.masterdatabase.SyncDatabaseWithLocalMasterReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

/**
 * Reconciles SEMOSS's OWL metamodel with an engine's real JDBC schema after a
 * migration runs raw SQL directly against the engine's connection.
 * <p>
 * SEMOSS keeps two layers of "schema": the physical JDBC schema, and its own
 * OWL metamodel (concepts/properties/relations — what backs the ER diagram,
 * query builder, and {@code GetDatabaseMetamodel}). Reactors that mutate
 * schema through SEMOSS's own structured editors (e.g.
 * {@code AddDatabaseStructureReactor}) update both together. A migration's
 * raw SQL only touches the first layer — this class runs the same
 * discover-then-reconcile chain the Metadata tab's "sync" button runs
 * manually ({@code ExternalUpdateJdbcSchema} → {@code RdbmsExternalUpload}
 * with {@code existing=true} → {@code SyncDatabaseWithLocalMaster}), so a
 * migration's changes show up in the OWL immediately instead of only after a
 * human visits the Metadata tab.
 */
public class MigrationOwlSyncUtility {

	private static final Logger classLogger = LogManager.getLogger(MigrationOwlSyncUtility.class);

	private MigrationOwlSyncUtility() {
		// static utility class
	}

	/**
	 * Best-effort — logs and returns false on any failure rather than throwing,
	 * since the migration's own DDL already succeeded by the time this runs; a
	 * sync failure means the OWL is stale, not that the migration failed.
	 *
	 * @param insight  the calling insight (carries the user for security checks
	 *                 on the composed reactors)
	 * @param engineId the engine whose OWL should be reconciled
	 * @return true if the full chain completed
	 */
	public static boolean syncOwlMetadata(Insight insight, String engineId) {
		try {
			Map<String, Object> discovered = discoverJdbcSchema(insight, engineId);
			Map<String, Object> metamodel = toUploadMetamodel(discovered);
			uploadMetamodel(insight, engineId, metamodel);
			syncLocalMaster(insight, engineId);
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to sync OWL metadata for engine {} after migration run", engineId, e);
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> discoverJdbcSchema(Insight insight, String engineId) {
		ExternalUpdateJdbcSchemaReactor reactor = new ExternalUpdateJdbcSchemaReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(singleKeyStore(ReactorKeysEnum.DATABASE.getKey(), engineId));
		reactor.In();
		NounMetadata result = reactor.execute();
		return (Map<String, Object>) result.getValue();
	}

	/**
	 * Converts {@code ExternalUpdateJdbcSchemaReactor}'s discovery output into
	 * the {@code nodesAndProps} ("TABLE.PRIMARYKEY" -&gt; all column names) /
	 * {@code relationships} shape {@code RdbmsExternalUploadReactor} expects.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> toUploadMetamodel(Map<String, Object> discovered) {
		Map<String, List<String>> nodesAndProps = new HashMap<>();
		List<Map<String, Object>> tables = (List<Map<String, Object>>) discovered
				.get(ExternalUpdateJdbcSchemaReactor.TABLES_KEY);
		if (tables != null) {
			for (Map<String, Object> table : tables) {
				String tableName = (String) table.get("table");
				List<String> columns = (List<String>) table.get("columns");
				List<Boolean> isPrimKey = (List<Boolean>) table.get("isPrimKey");
				if (tableName == null || columns == null || columns.isEmpty()) {
					continue;
				}

				String primaryKey = null;
				if (isPrimKey != null) {
					for (int i = 0; i < columns.size() && i < isPrimKey.size(); i++) {
						if (Boolean.TRUE.equals(isPrimKey.get(i))) {
							primaryKey = columns.get(i);
							break;
						}
					}
				}
				if (primaryKey == null) {
					// no declared primary key -- fall back to the first column so the
					// table still gets a node rather than being dropped entirely
					primaryKey = columns.get(0);
				}
				nodesAndProps.put(tableName + "." + primaryKey, columns);
			}
		}

		List<Map<String, Object>> relationships = new ArrayList<>();
		List<Map<String, String>> discoveredRelations = (List<Map<String, String>>) discovered
				.get(ExternalUpdateJdbcSchemaReactor.RELATIONS_KEY);
		if (discoveredRelations != null) {
			for (Map<String, String> rel : discoveredRelations) {
				String fromCol = rel.get("fromCol");
				String toCol = rel.get("toCol");
				if (fromCol == null || toCol == null) {
					continue;
				}
				Map<String, Object> uploadRel = new HashMap<>();
				uploadRel.put(Constants.FROM_TABLE, rel.get("fromTable"));
				uploadRel.put(Constants.TO_TABLE, rel.get("toTable"));
				uploadRel.put(Constants.REL_NAME, fromCol + "." + toCol);
				relationships.add(uploadRel);
			}
		}

		Map<String, Object> metamodel = new HashMap<>();
		metamodel.put(ExternalUpdateJdbcSchemaReactor.TABLES_KEY, nodesAndProps);
		metamodel.put(ExternalUpdateJdbcSchemaReactor.RELATIONS_KEY, relationships);
		return metamodel;
	}

	private static void uploadMetamodel(Insight insight, String engineId, Map<String, Object> metamodel) {
		NounStore store = new NounStore("RdbmsExternalUpload-migration-sync");

		GenRowStruct databaseGrs = new GenRowStruct();
		databaseGrs.add(new NounMetadata(engineId, PixelDataType.CONST_STRING));
		store.addNoun(ReactorKeysEnum.DATABASE.getKey(), databaseGrs);

		GenRowStruct metamodelGrs = new GenRowStruct();
		metamodelGrs.add(new NounMetadata(metamodel, PixelDataType.MAP));
		store.addNoun(ReactorKeysEnum.METAMODEL.getKey(), metamodelGrs);

		GenRowStruct existingGrs = new GenRowStruct();
		existingGrs.add(new NounMetadata("true", PixelDataType.CONST_STRING));
		store.addNoun(ReactorKeysEnum.EXISTING.getKey(), existingGrs);

		RdbmsExternalUploadReactor reactor = new RdbmsExternalUploadReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(store);
		reactor.In();
		reactor.execute();
	}

	private static void syncLocalMaster(Insight insight, String engineId) {
		SyncDatabaseWithLocalMasterReactor reactor = new SyncDatabaseWithLocalMasterReactor();
		reactor.setInsight(insight);
		reactor.setNounStore(singleKeyStore(ReactorKeysEnum.DATABASE.getKey(), engineId));
		reactor.In();
		reactor.execute();
	}

	private static NounStore singleKeyStore(String key, String value) {
		NounStore store = new NounStore("migration-sync");
		GenRowStruct grs = new GenRowStruct();
		grs.add(new NounMetadata(value, PixelDataType.CONST_STRING));
		store.addNoun(key, grs);
		return store;
	}

}
