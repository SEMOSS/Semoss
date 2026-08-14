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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IDatabaseEngine;
import prerna.util.Utility;

/**
 * Shared base for the system-engine OWL creators (security, themes, prompts,
 * scheduler, etc.). Each concrete creator declares its schema in
 * {@link #allSchemas} as a list of tables, where every table is paired with its
 * ordered list of (column name, datatype) pairs.
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

	private static final String BASE_CONCEPT_URI = AbstractOWLEngine.BASE_NODE_URI;
	private static final String CONTAINS_RELATION_BASE = AbstractOWLEngine.BASE_PROPERTY_URI + "/";

	// Pairs each table name with its respective columns (column name -> datatype).
	// concepts are tables within the db, props are columns within a concept.
	protected List<Pair<String, List<Pair<String, String>>>> allSchemas = new ArrayList<>();

	/**
	 * The declared schema in (table, [(column, datatype)]) form.
	 *
	 * @return the table-to-columns structure backing this creator
	 */
	public List<Pair<String, List<Pair<String, String>>>> getDBSchema() {
		return this.allSchemas;
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
	 * Flatten this creator's declared schema into table/column/datatype rows.
	 *
	 * @return one {@link OwlColumn} per declared column
	 */
	public List<OwlColumn> getSchemaColumns() {
		return getSchemaColumns(this.allSchemas);
	}

	/**
	 * Flatten a declared OWL schema into a list of table/column/datatype rows.
	 *
	 * @param schemas the table -> [(column, datatype)] structure (e.g. from
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
	 * A single table/column/datatype triple derived from a declared OWL schema.
	 *
	 * @param tableName  the table (concept) name
	 * @param columnName the column (property) name
	 * @param dataType   the declared datatype of the column
	 */
	public record OwlColumn(String tableName, String columnName, String dataType) {
	}
}
