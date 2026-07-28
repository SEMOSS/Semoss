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
package prerna.engine.impl.rdbms.migration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.database.upload.rdbms.RDBMSEngineCreationHelper;
import prerna.util.EngineSyncUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

/**
 * Reconciles SEMOSS's OWL metamodel with an engine's real JDBC schema after a
 * migration runs raw SQL directly against the engine's connection.
 * <p>
 * This is the same diff-based sync the Metadata tab's "Sync" button performs
 * ({@code ExternalUpdateJdbcSchema} discovery -&gt; {@code RdbmsExternalUpload}
 * with {@code existing=true} -&gt; automatic Local Master re-registration), but
 * called as plain Java rather than through that security-checked reactor
 * chain: there is no {@code Insight}/user at engine-{@code open()} time for
 * those reactors' {@code userCanEditEngine} checks to run against. The
 * discovery and OWL-write logic is reused directly:
 * <ul>
 * <li>{@link RDBMSEngineCreationHelper#getExistingRDBMSStructure(prerna.engine.api.IDatabaseEngine)}
 * for JDBC introspection (identical dialect handling the reactor chain
 * uses) — called with no table filter to discover the engine's full current
 * schema.</li>
 * <li>{@link UploadUtilities#getExistingMetamodel(prerna.engine.impl.owl.AbstractOWLEngine)}
 * plus direct {@link WriteOWLEngine} calls for the diff-write, mirroring
 * {@code RdbmsExternalUploadReactor#updateExistingDatabase}.</li>
 * </ul>
 * The Local Master DB re-registration step is <b>not</b> performed here —
 * {@code Utility.loadEngine()} already calls
 * {@code Utility.synchronizeEngineMetadata(engineId)} immediately after
 * {@code open()} returns, for every {@code DATABASE}-type engine, by
 * comparing the OWL file's on-disk timestamp against what Local Master has
 * recorded. Since {@link WriteOWLEngine#export()} below touches that file,
 * that pre-existing hook picks up the change automatically — no need to
 * duplicate it.
 * <p>
 * Table/column relationship (foreign key) diffing is add-only in this first
 * pass -- newly discovered foreign keys are added, but ones removed by a
 * migration are not yet pruned from the OWL. Flagged as a follow-up; it does
 * not affect whether new/changed tables and columns are visible to
 * {@code SelectQueryStruct}-based queries, which is the primary correctness
 * requirement here.
 */
public final class RdbmsMigrationOwlSyncUtils {

	private static final Logger classLogger = LogManager.getLogger(RdbmsMigrationOwlSyncUtils.class);

	/**
	 * Only {@code SEMOSS_SCHEMA_LOCK} is excluded from OWL -- it's transient
	 * concurrency-control plumbing with no query/diagnostic value (unlike
	 * {@code SEMOSS_SCHEMA_HISTORY}, nobody wants to inspect lock rows).
	 * {@code SEMOSS_SCHEMA_HISTORY} is deliberately registered like any other
	 * table -- matching the precedent {@code PGVectorQueryUtil.createOWL()}
	 * already sets for engine-managed (not user-authored) tables living
	 * alongside the user's own data: PGVector's embeddings/metadata tables are
	 * added to OWL the same way, not hidden from the ER diagram.
	 */
	private static final Set<String> RESERVED_TABLE_NAMES = Set.of(SchemaMigrationLock.LOCK_TABLE);

	private RdbmsMigrationOwlSyncUtils() {
		// utility class
	}

	/**
	 * @param engine the engine whose OWL should be reconciled with its current
	 *               JDBC schema
	 * @throws SchemaMigrationException if the sync fails -- by design this
	 *                                   propagates out of
	 *                                   {@code open()} rather than leaving the
	 *                                   engine open with a stale OWL that
	 *                                   {@code SelectQueryStruct} would resolve
	 *                                   incorrectly against
	 */
	public static void syncOwlAfterMigration(IRDBMSEngine engine) {
		try (WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			Map<String, Map<String, SemossDataType>> existingMetamodel = UploadUtilities
					.getExistingMetamodel(owlEngine);
			Map<String, Map<String, String>> newStructure = RDBMSEngineCreationHelper
					.getExistingRDBMSStructure(engine);
			RESERVED_TABLE_NAMES.forEach(newStructure::remove);

			if (existingMetamodel.equals(newStructure)) {
				return;
			}

			removeStaleConceptsAndProps(owlEngine, existingMetamodel, newStructure);
			addNewConceptsAndProps(owlEngine, existingMetamodel, newStructure);
			addNewRelationships(engine, owlEngine, newStructure);


			owlEngine.commit();
			owlEngine.export();
			EngineSyncUtility.clearEngineCache(engine.getEngineId());
		} catch (Exception e) {
			classLogger.error("Failed to sync OWL metadata for engine '{}' after migration run.", engine.getEngineId(),
					e);
			throw new SchemaMigrationException("Unable to sync OWL metadata for engine " + engine.getEngineId(), e);
		}
	}

	private static void removeStaleConceptsAndProps(WriteOWLEngine owlEngine,
			Map<String, Map<String, SemossDataType>> existingMetamodel, Map<String, Map<String, String>> newStructure) {
		existingMetamodel.forEach((existingTableName, existingColumns) -> {
			if (!newStructure.containsKey(existingTableName)) {
				classLogger.info("Removing table '{}' from owl -- no longer present in the JDBC schema",
						Utility.cleanLogString(existingTableName));
				owlEngine.removeConcept(existingTableName);
				return;
			}
			Map<String, String> newColumns = newStructure.get(existingTableName);
			existingColumns.forEach((existingColumnName, existingDataType) -> {
				String newDataType = newColumns.get(existingColumnName);
				if (newDataType == null
						|| SemossDataType.convertStringToDataType(newDataType) != existingDataType) {
					classLogger.info("Removing column '{}' for table '{}' from owl",
							Utility.cleanLogString(existingColumnName), Utility.cleanLogString(existingTableName));
					owlEngine.removeProp(existingTableName, existingColumnName);
				}
			});
		});
	}

	private static void addNewConceptsAndProps(WriteOWLEngine owlEngine,
			Map<String, Map<String, SemossDataType>> existingMetamodel, Map<String, Map<String, String>> newStructure) {
		newStructure.forEach((newTableName, newColumns) -> {
			boolean isNewTable = !existingMetamodel.containsKey(newTableName);
			if (isNewTable) {
				classLogger.info("Adding table '{}' to owl", Utility.cleanLogString(newTableName));
				owlEngine.addConcept(newTableName, null, null);
			}
			Map<String, SemossDataType> existingColumns = existingMetamodel.get(newTableName);
			newColumns.forEach((newColumnName, newDataType) -> {
				boolean columnAlreadyPresent = existingColumns != null && existingColumns.containsKey(newColumnName)
						&& SemossDataType.convertStringToDataType(newDataType) == existingColumns.get(newColumnName);
				if (!columnAlreadyPresent) {
					classLogger.info("Adding column '{}' to table '{}' in owl",
							Utility.cleanLogString(newColumnName), Utility.cleanLogString(newTableName));
					owlEngine.addProp(newTableName, newColumnName, newDataType, null, null);
				}
			});
		});
	}

	/**
	 * Add-only: newly discovered foreign keys are written as OWL relations.
	 * Removed foreign keys are not pruned in this first pass (see class
	 * Javadoc).
	 */
	private static void addNewRelationships(IRDBMSEngine engine, WriteOWLEngine owlEngine,
			Map<String, Map<String, String>> newStructure) {
		Connection conn = null;
		try {
			conn = engine.getConnection();
			DatabaseMetaData meta = conn.getMetaData();
			String catalog = engine.getQueryUtil().getDatabaseMetadataCatalogFilter();
			if (catalog == null) {
				catalog = conn.getCatalog();
			}
			String schema = engine.getQueryUtil().getDatabaseMetadataSchemaFilter();
			if (schema == null) {
				schema = engine.getSchema();
			}

			for (String tableName : newStructure.keySet()) {
				addRelationshipsForTable(owlEngine, meta, catalog, schema, tableName);
			}
		} catch (SQLException e) {
			classLogger.error("Failed to discover foreign key relationships for engine '{}'.", engine.getEngineId(),
					e);
			throw new SchemaMigrationException(
					"Unable to discover foreign key relationships for engine " + engine.getEngineId(), e);
		} finally {
			if (conn != null && engine.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error("Failed to close connection after discovering relationships.", e);
				}
			}
		}
	}

	private static void addRelationshipsForTable(WriteOWLEngine owlEngine, DatabaseMetaData meta, String catalog,
			String schema, String tableName) throws SQLException {
		try (ResultSet exportedKeys = meta.getExportedKeys(catalog, schema, tableName)) {
			while (exportedKeys.next()) {
				String toTable = exportedKeys.getString("FKTABLE_NAME");
				String fromCol = exportedKeys.getString("PKCOLUMN_NAME");
				String toCol = exportedKeys.getString("FKCOLUMN_NAME");
				owlEngine.addRelation(tableName, toTable, fromCol + "." + toCol);
			}
		}
	}

}
