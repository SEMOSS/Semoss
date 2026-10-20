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
package prerna.engine.impl.owl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Shared base for the system-engine OWL creators (security, themes, prompts,
 * scheduler, etc.). Each concrete creator declares its schema in
 * {@link #allSchemas} as a list of tables, where every table is paired with its
 * ordered list of (column name, data type) pairs.
 * <p>
 * The schema declaration only needs the SQL dialect (via {@code queryUtil} in
 * the subclass constructor), so the schema can be read from any instance -
 * {@link #getDBSchema()} / {@link #getSchemaColumns()} - without handing out
 * the (restricted) engine. The engine is only required for the operations that
 * actually read or write the persisted OWL, and is therefore passed in to those
 * methods explicitly:
 * <ul>
 * <li>{@link #needsRemake(IDatabaseEngine)} - whether the persisted OWL is
 * missing any declared table or column</li>
 * <li>{@link #remakeOwl(IDatabaseEngine)} /
 * {@link #writeNewOwl(WriteOWLEngine)} - writing the declared concepts and
 * properties back out</li>
 * </ul>
 * Subclasses add relationships via {@link #writeRelations(WriteOWLEngine)} and
 * any extra remake checks via {@link #additionalRemakeChecks(IDatabaseEngine)}.
 */
public abstract class AbstractOwlCreator {

	private static final Logger classLogger = LogManager.getLogger(AbstractOwlCreator.class);

	/** Column constraint emitted by {@link #syncSchema} when a table is created. */
	private static final String NOT_NULL = "NOT NULL";

	private static final String BASE_CONCEPT_URI = AbstractOWLEngine.BASE_NODE_URI;
	private static final String CONTAINS_RELATION_BASE = AbstractOWLEngine.BASE_PROPERTY_URI + "/";

	// Pairs each table name with its respective columns (column name -> data type).
	// concepts are tables within the db, props are columns within a concept.
	protected List<Pair<String, List<Pair<String, String>>>> allSchemas = new ArrayList<>();

	/**
	 * On constructor, define the database schema
	 * 
	 * @param queryUtil
	 */
	public AbstractOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

	/**
	 * Method that will set the {@link #allSchemas} object with the tables, columns,
	 * and data types based on the rdbms implementation
	 * 
	 * @param queryUtil
	 */
	public abstract void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil);

	/**
	 * The declared schema in (table, [(column, data type)]) form.
	 *
	 * @return the table-to-columns structure backing this creator
	 */
	public List<Pair<String, List<Pair<String, String>>>> getDBSchema() {
		return this.allSchemas;
	}

	/**
	 * Brings the physical tables in line with this creator's declared schema.
	 *
	 * @param engine database holding the tables
	 * @param conn   open connection on that database; the caller owns the commit
	 * @throws SQLException if a create or alter fails
	 */
	public void syncSchema(IRDBMSEngine engine, Connection conn) throws SQLException {
		syncSchema(engine, conn, this.allSchemas, Map.of());
	}

	/**
	 * Brings the physical tables in line with a declared schema.
	 *
	 * @param schemas the table -> [(column, data type)] structure to apply
	 * @throws SQLException if a create or alter fails
	 */
	public static void syncSchema(IRDBMSEngine engine, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> schemas) throws SQLException {
		syncSchema(engine, conn, schemas, Map.of());
	}

	/**
	 * Creates each declared table that does not exist, then adds any declared
	 * column the live table is missing.
	 *
	 * <p>
	 * The second half is the reason this is not just a create: a create statement
	 * only runs against a database that does not have the table yet, so a column
	 * added to a creator after an installation first booted would otherwise never
	 * exist there. Both halves are idempotent, so this is safe to run on every
	 * boot.
	 *
	 * <p>
	 * Columns added to an existing table are always nullable, even when named in
	 * {@code notNullColumns}. Rows already in the table have no value for a new
	 * column, so the constraint can only be applied when the table is created.
	 *
	 * @param engine         database holding the tables
	 * @param conn           open connection on that database; the caller owns the
	 *                       commit
	 * @param schemas        the table -> [(column, data type)] structure to apply
	 * @param notNullColumns columns to create NOT NULL, keyed by table name; empty
	 *                       when the schema has no such constraints
	 * @throws SQLException if a create or alter fails
	 */
	public static void syncSchema(IRDBMSEngine engine, Connection conn,
			List<Pair<String, List<Pair<String, String>>>> schemas, Map<String, Set<String>> notNullColumns)
			throws SQLException {
		if (schemas == null || schemas.isEmpty()) {
			return;
		}
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		String database = engine.getDatabase();
		String schema = engine.getSchema();
		String engineLabel = engineLabel(engine);
		boolean allowIfExists = queryUtil.allowsIfExistsTableSyntax();

		for (Pair<String, List<Pair<String, String>>> tableSchema : schemas) {
			String tableName = tableSchema.getValue0();
			List<Pair<String, String>> declared = tableSchema.getValue1();
			String[] colNames = declared.stream().map(Pair::getValue0).toArray(String[]::new);
			String[] types = declared.stream().map(Pair::getValue1).toArray(String[]::new);
			Set<String> notNull = notNullColumns == null ? Set.of() : notNullColumns.getOrDefault(tableName, Set.of());

			if (allowIfExists || !queryUtil.tableExists(conn, tableName, database, schema)) {
				String sql = createTableSql(queryUtil, allowIfExists, tableName, colNames, types, notNull);
				classLogger.info("[{}] Running sql {}", engineLabel, sql);
				execute(conn, sql);
			}

			List<String> existingColumns = queryUtil.getTableColumns(conn, tableName, database, schema);
			if (existingColumns == null || existingColumns.isEmpty()) {
				continue;
			}
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if (existingColumns.contains(col) || existingColumns.contains(col.toLowerCase())) {
					continue;
				}
				classLogger.info("[{}] Column {} missing from {}; adding it. Existing columns: {}", engineLabel, col,
						tableName, existingColumns);
				String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
				classLogger.info("[{}] Running sql {}", engineLabel, addColumnSql);
				execute(conn, addColumnSql);
			}
		}
	}

	/**
	 * Creates each index that does not already exist.
	 *
	 * <p>
	 * Dialects split on whether they understand "CREATE INDEX IF NOT EXISTS". The
	 * ones that do get the single statement; the rest are probed first. Without
	 * this every index has to be written twice at the call site, once per branch,
	 * which is where they drift apart.
	 *
	 * @param engine  database holding the tables
	 * @param conn    open connection on that database; the caller owns the commit
	 * @param indexes indexes to ensure
	 * @throws SQLException if a create fails
	 */
	public static void syncIndexes(IRDBMSEngine engine, Connection conn, Collection<OwlIndex> indexes)
			throws SQLException {
		if (indexes == null || indexes.isEmpty()) {
			return;
		}
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		String database = engine.getDatabase();
		String schema = engine.getSchema();
		String engineLabel = engineLabel(engine);
		boolean allowIfExists = queryUtil.allowIfExistsIndexSyntax();

		for (OwlIndex index : indexes) {
			if (index.columns() == null || index.columns().isEmpty()) {
				classLogger.warn("[{}] Index {} declares no columns; skipping it.", engineLabel, index.indexName());
				continue;
			}
			boolean single = index.columns().size() == 1;
			String sql;
			if (allowIfExists) {
				sql = single
						? queryUtil.createIndexIfNotExists(index.indexName(), index.tableName(), index.columns().get(0))
						: queryUtil.createIndexIfNotExists(index.indexName(), index.tableName(), index.columns());
			} else {
				if (queryUtil.indexExists(engine, index.indexName(), index.tableName(), database, schema)) {
					continue;
				}
				sql = single ? queryUtil.createIndex(index.indexName(), index.tableName(), index.columns().get(0))
						: queryUtil.createIndex(index.indexName(), index.tableName(), index.columns());
			}
			classLogger.info("[{}] Running sql {}", engineLabel, sql);
			execute(conn, sql);
		}
	}

	/**
	 * A human-readable name for the database a statement is running against, for
	 * logging. Several system databases are reconciled during the same boot, so a
	 * bare "Running sql" line does not say which one it belongs to.
	 *
	 * @param engine database being reconciled
	 * @return its engine name, falling back to the engine id then the physical
	 *         database name
	 */
	private static String engineLabel(IRDBMSEngine engine) {
		String name = engine.getEngineName();
		if (name != null && !name.trim().isEmpty()) {
			return name;
		}
		String id = engine.getEngineId();
		if (id != null && !id.trim().isEmpty()) {
			return id;
		}
		String database = engine.getDatabase();
		return database != null && !database.trim().isEmpty() ? database : "unknown database";
	}

	/**
	 * 
	 * @param queryUtil
	 * @param allowIfExists
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param notNull
	 * @return
	 */
	private static String createTableSql(AbstractSqlQueryUtil queryUtil, boolean allowIfExists, String tableName,
			String[] colNames, String[] types, Set<String> notNull) {
		if (notNull.isEmpty()) {
			return allowIfExists ? queryUtil.createTableIfNotExists(tableName, colNames, types)
					: queryUtil.createTable(tableName, colNames, types);
		}
		String[] constraints = new String[colNames.length];
		for (int i = 0; i < colNames.length; i++) {
			constraints[i] = notNull.contains(colNames[i]) ? NOT_NULL : null;
		}
		return allowIfExists
				? queryUtil.createTableIfNotExistsWithCustomConstraints(tableName, colNames, types, constraints)
				: queryUtil.createTableWithCustomConstraints(tableName, colNames, types, constraints);
	}

	/**
	 * 
	 * @param conn
	 * @param sql
	 * @throws SQLException
	 */
	private static void execute(Connection conn, String sql) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.execute();
		}
	}

	/**
	 * Add a table into the allSchemas list.
	 *
	 * @param tableName
	 * @param columns
	 */
	protected void addTable(String tableName, List<Pair<String, String>> columns) {
		this.allSchemas.add(Pair.with(tableName, columns));
	}

	/**
	 * Flatten this creator's declared schema into table/column/data type rows.
	 *
	 * @return one {@link OwlColumn} per declared column
	 */
	public List<OwlColumn> getSchemaColumns() {
		return getSchemaColumns(this.allSchemas);
	}

	/**
	 * Flatten a declared OWL schema into a list of table/column/data type rows.
	 *
	 * @param schemas the table -> [(column, data type)] structure (e.g. from
	 *                {@link #getDBSchema()})
	 * @return one {@link OwlColumn} per column, preserving table and column order
	 */
	public static List<OwlColumn> getSchemaColumns(List<Pair<String, List<Pair<String, String>>>> schemas) {
		List<OwlColumn> columns = new ArrayList<>();
		if (schemas == null) {
			return columns;
		}
		for (Pair<String, List<Pair<String, String>>> table : schemas) {
			String tableName = table.getValue0();
			for (Pair<String, String> column : table.getValue1()) {
				columns.add(new OwlColumn(tableName, column.getValue0(), column.getValue1()));
			}
		}
		columns.sort(Comparator.comparing(OwlColumn::tableName).thenComparing(OwlColumn::columnName));
		return columns;
	}

	/**
	 * Determine if we need to remake the OWL by checking that every declared table
	 * and column is present in the persisted OWL.
	 *
	 * @param engine the engine whose persisted OWL is being inspected
	 * @return true if the OWL is missing any declared table/column (or fails a
	 *         subclass-specific check)
	 */
	public boolean needsRemake(IDatabaseEngine engine) {
		List<String> cleanConcepts = new ArrayList<>();
		try {
			List<String> concepts = engine.getPhysicalConcepts();
			if (concepts.isEmpty()) {
				return true;
			}
			for (String concept : concepts) {
				if (concept.equals(BASE_CONCEPT_URI)) {
					continue;
				}
				cleanConcepts.add(Utility.getInstanceName(concept));
			}
		} catch (Exception e) {
			// could not read the existing OWL - remake it
			return true;
		}

		if (!cleanConcepts.containsAll(getRequiredConcepts())) {
			return true;
		}

		// check that every declared column exists
		for (Pair<String, List<Pair<String, String>>> table : allSchemas) {
			String tableName = table.getValue0();
			for (Pair<String, String> column : table.getValue1()) {
				if (columnMissing(engine, tableName, column.getValue0())) {
					return true;
				}
			}
		}

		// subclass-specific checks (e.g. required relationships)
		return additionalRemakeChecks(engine);
	}

	/**
	 * Concepts (tables) that must exist in the OWL - derived from the declared
	 * schema.
	 *
	 * @return the list of required table names
	 */
	protected List<String> getRequiredConcepts() {
		List<String> required = new ArrayList<>();
		for (Pair<String, List<Pair<String, String>>> table : allSchemas) {
			required.add(table.getValue0());
		}
		return required;
	}

	/**
	 * Hook for subclass-specific remake checks (e.g. required relationships).
	 * Default implementation requires no extra checks.
	 *
	 * @param engine the engine whose persisted OWL is being inspected
	 * @return true if the OWL needs to be remade based on subclass-specific state
	 */
	protected boolean additionalRemakeChecks(IDatabaseEngine engine) {
		return false;
	}

	/**
	 * @param engine     the engine whose persisted OWL is being inspected
	 * @param tableName  the concept/table to inspect
	 * @param columnName the property/column expected on that table
	 * @return true if the column is not present on the table in the persisted OWL
	 */
	protected boolean columnMissing(IDatabaseEngine engine, String tableName, String columnName) {
		String propsURI = BASE_CONCEPT_URI + "/" + tableName;
		String relationURI = CONTAINS_RELATION_BASE + columnName + "/" + tableName;
		List<String> props = engine.getPropertyUris4PhysicalUri(propsURI);
		return !props.contains(relationURI);
	}

	/**
	 * Remake the OWL from the declared schema.
	 *
	 * @param engine the engine whose OWL is being (re)written
	 * @throws Exception if writing the OWL fails
	 */
	public void remakeOwl(IDatabaseEngine engine) throws Exception {
		try (WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			writeNewOwl(owlEngine);
		}
	}

	/**
	 * Writes the declared concepts/properties, then any subclass relationships,
	 * then commits and exports.
	 *
	 * @param owler the OWL writer
	 * @throws Exception if writing fails
	 */
	protected void writeNewOwl(WriteOWLEngine owler) throws Exception {
		for (Pair<String, List<Pair<String, String>>> table : allSchemas) {
			String tableName = table.getValue0();
			owler.addConcept(tableName, null, null);
			for (Pair<String, String> column : table.getValue1()) {
				owler.addProp(tableName, column.getValue0(), column.getValue1());
			}
		}
		writeRelations(owler);
		owler.commit();
		owler.export();
	}

	/**
	 * Hook for subclasses to add relationships/foreign keys. Default: none.
	 *
	 * @param owler the OWL writer
	 * @throws Exception if writing a relationship fails
	 */
	protected void writeRelations(WriteOWLEngine owler) throws Exception {
		// no relationships by default
	}

	/**
	 * A single table/column/data type triple derived from a declared OWL schema.
	 *
	 * @param tableName  the table (concept) name
	 * @param columnName the column (property) name
	 * @param data       type the declared data type of the column
	 */
	public record OwlColumn(String tableName, String columnName, String dataType) {
	}

	/**
	 * One index to keep in place alongside a declared schema.
	 *
	 * @param indexName index name, unique within the database
	 * @param tableName table the index is on
	 * @param columns   indexed columns, in order
	 */
	public record OwlIndex(String indexName, String tableName, List<String> columns) {

		/**
		 * @param columns indexed columns, in order; at least one
		 * @return an index declaration
		 */
		public static OwlIndex of(String indexName, String tableName, String... columns) {
			return new OwlIndex(indexName, tableName, List.of(columns));
		}
	}
}
