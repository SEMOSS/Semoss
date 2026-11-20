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
package prerna.masterdatabase.utility;

import java.util.ArrayList;
import java.util.Arrays;

import org.javatuples.Pair;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

public class LocalMasterOwlCreator extends AbstractOwlCreator {

	/** A database's join names the two ENGINECONCEPT rows it joins by their PHYSICALNAMEID */
	private static final String SOURCE_CONCEPT_RELATION = "ENGINECONCEPT.PHYSICALNAMEID.ENGINERELATION.SOURCECONCEPTID";
	private static final String TARGET_CONCEPT_RELATION = "ENGINECONCEPT.PHYSICALNAMEID.ENGINERELATION.TARGETCONCEPTID";

	public LocalMasterOwlCreator(AbstractSqlQueryUtil queryUtil) {
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String VARCHAR_255 = "VARCHAR(255)";
		final String VARCHAR_800 = "VARCHAR(800)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		// the local master: a copy of the metamodel of every database, its tables, columns and
		// joins, taken from its OWL file by AddToMasterDB.
		// InternalUtility.synchronizeEngineMetadata copies a database in when it is missing or its
		// OWL file has changed, and MasterDatabaseUtility reads the copies to list, search and
		// connect concepts across databases. CONCEPT and RELATION hold the names every database
		// shares, and ENGINECONCEPT and ENGINERELATION each database's own tables, columns and
		// joins

		// a conceptual name shared across databases: LOCALCONCEPTID is the same for a table or
		// column of that name in every database, with one row per logical name it has (LOGICALNAME,
		// lower case). DOMAINNAME is written as NewDomain and GLOBALID as empty. ENGINECONCEPT rows
		// point here by LOCALCONCEPTID
		addTable("CONCEPT", Arrays.asList(
				Pair.with("LOCALCONCEPTID", VARCHAR_255),
				Pair.with("CONCEPTUALNAME", VARCHAR_255),
				Pair.with("LOGICALNAME", VARCHAR_255),
				Pair.with("DOMAINNAME", VARCHAR_255),
				Pair.with("GLOBALID", VARCHAR_255)));

		// metadata on one table or column of a database, keyed by its ENGINECONCEPT.PHYSICALNAMEID:
		// a METAKEY of logical for each of its logical names, or description for its description
		addTable("CONCEPTMETADATA", Arrays.asList(
				Pair.with("PHYSICALNAMEID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_800),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME)));

		// one row per database copied in: its ID, ENGINENAME, TYPE (TYPE:RDBMS, TYPE:RDF,
		// TYPE:TINKER, TYPE:R or TYPE:JANUS_GRAPH) and MODIFIEDDATE, the time of the OWL file it
		// was copied from, which tells synchronizeEngineMetadata when to copy it again
		addTable("ENGINE", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ENGINENAME", VARCHAR_255),
				Pair.with("MODIFIEDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("TYPE", VARCHAR_255)));

		// one table (concept) or column (property) of a database: ENGINE is the database's ID,
		// SEMOSSNAME and PHYSICALNAME its pixel and physical names, PHYSICALNAMEID its own id and
		// LOCALCONCEPTID its shared CONCEPT. A column names its table in the PARENT columns; a
		// table has none and PK true. IGNORE_DATA marks a table whose data is in its columns (RDBMS
		// and R), and ORIGINAL_TYPE, PROPERTY_TYPE and ADDITIONAL_TYPE give the data type as the
		// OWL has it, its basic type (such as INT, DOUBLE or DATE) and any additional type
		addTable("ENGINECONCEPT", Arrays.asList(
				Pair.with("ENGINE", VARCHAR_255),
				Pair.with("PARENTSEMOSSNAME", VARCHAR_255),
				Pair.with("SEMOSSNAME", VARCHAR_255),
				Pair.with("PARENTPHYSICALNAME", VARCHAR_255),
				Pair.with("PARENTPHYSICALNAMEID", VARCHAR_255),
				Pair.with("PHYSICALNAME", VARCHAR_255),
				Pair.with("PHYSICALNAMEID", VARCHAR_255),
				Pair.with("PARENTLOCALCONCEPTID", VARCHAR_255),
				Pair.with("LOCALCONCEPTID", VARCHAR_255),
				Pair.with("PK", BOOLEAN_DATATYPE_NAME),
				Pair.with("IGNORE_DATA", BOOLEAN_DATATYPE_NAME),
				Pair.with("ORIGINAL_TYPE", VARCHAR_255),
				Pair.with("PROPERTY_TYPE", VARCHAR_255),
				Pair.with("ADDITIONAL_TYPE", VARCHAR_255)));

		// one join in a database: ENGINE is the database's ID, SOURCECONCEPTID and TARGETCONCEPTID
		// the PHYSICALNAMEID of the two ENGINECONCEPT rows it joins, SOURCEPROPERTY and
		// TARGETPROPERTY their pixel names, RELATIONNAME the relationship's name in the OWL,
		// RELATIONID the shared RELATION and INSTANCERELATIONID this row's own id
		addTable("ENGINERELATION", Arrays.asList(
				Pair.with("RELATIONID", VARCHAR_255),
				Pair.with("ENGINE", VARCHAR_255),
				Pair.with("INSTANCERELATIONID", VARCHAR_255),
				Pair.with("SOURCECONCEPTID", VARCHAR_255),
				Pair.with("TARGETCONCEPTID", VARCHAR_255),
				Pair.with("SOURCEPROPERTY", VARCHAR_255),
				Pair.with("TARGETPROPERTY", VARCHAR_255),
				Pair.with("RELATIONNAME", VARCHAR_255)));

		// key and value pairs for LocalMasterConceptIdHash, which loads them into memory at its
		// first use and writes its map back with persistBack: the ids the local master gives names,
		// under keys such as <conceptual name>_CONCEPTUAL, <database name>_ENGINE and
		// <start>_<end>_RELATION, so a name seen again keeps its id
		addTable("KVSTORE", Arrays.asList(
				Pair.with("K", VARCHAR_800),
				Pair.with("V", VARCHAR_800)));

		// a relationship between two conceptual names, shared across databases: SOURCEID and
		// TARGETID are CONCEPT.LOCALCONCEPTID values, and GLOBALID is written as empty. Each
		// database's own joins of the two are ENGINERELATION rows that point here by RELATIONID
		addTable("RELATION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("SOURCEID", VARCHAR_255),
				Pair.with("TARGETID", VARCHAR_255),
				Pair.with("GLOBALID", VARCHAR_255)));

		// where each table sits in a database's metamodel view: one row per ENGINEID and TABLENAME
		// with its XPOS and YPOS. SaveOwlPositionsReactor saves them, and copying a database in
		// reads them from the position file beside its OWL
		addTable("METAMODELPOSITION", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("TABLENAME", VARCHAR_255),
				Pair.with("XPOS", "FLOAT"),
				Pair.with("YPOS", "FLOAT")));
		// @formatter:on
	}

	@Override
	protected void writeRelations(WriteOWLEngine owler) throws Exception {
		// joins
		owler.addRelation("ENGINE", "ENGINECONCEPT", "ENGINE.ID.ENGINECONCEPT.ENGINE");
		owler.addRelation("ENGINE", "ENGINERELATION", "ENGINE.ID.ENGINERELATION.ENGINE");

		owler.addRelation("ENGINECONCEPT", "CONCEPT", "ENGINECONCEPT.LOCALCONCEPTID.CONCEPT.LOCALCONCEPTID");
		owler.addRelation("ENGINECONCEPT", "ENGINERELATION", SOURCE_CONCEPT_RELATION);
		owler.addRelation("ENGINECONCEPT", "ENGINERELATION", TARGET_CONCEPT_RELATION);

		owler.addRelation("ENGINERELATION", "RELATION", "ENGINERELATION.RELATIONID.RELATION.ID");

		owler.addRelation("CONCEPT", "RELATION", "CONCEPT.LOCALCONCEPTID.RELATION.SOURCEID");
		owler.addRelation("CONCEPT", "RELATION", "CONCEPT.LOCALCONCEPTID.RELATION.TARGETID");

		owler.addRelation("METAMODELPOSITION", "ENGINE", "METAMODELPOSITION.ENGINEID.ENGINE.ID");
	}

	/**
	 * Remakes an OWL that does not join a database's joins to its tables and
	 * columns by PHYSICALNAMEID.
	 *
	 * @param engine the local master whose persisted OWL is being inspected
	 * @return true if either join is missing
	 */
	@Override
	protected boolean additionalRemakeChecks(IDatabaseEngine engine) {
		return relationMissing(engine, "ENGINECONCEPT", "ENGINERELATION", SOURCE_CONCEPT_RELATION)
				|| relationMissing(engine, "ENGINECONCEPT", "ENGINERELATION", TARGET_CONCEPT_RELATION);
	}

}
