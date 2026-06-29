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

import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

public class LocalMasterOwlCreator extends AbstractOwlCreator {

	public LocalMasterOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String VARCHAR_255 = "VARCHAR(255)";
		final String VARCHAR_800 = "VARCHAR(800)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable("CONCEPT", Arrays.asList(
				Pair.with("LOCALCONCEPTID", VARCHAR_255),
				Pair.with("CONCEPTUALNAME", VARCHAR_255),
				Pair.with("LOGICALNAME", VARCHAR_255),
				Pair.with("DOMAINNAME", VARCHAR_255),
				Pair.with("GLOBALID", VARCHAR_255)));

		addTable("CONCEPTMETADATA", Arrays.asList(
				Pair.with("PHYSICALNAMEID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_800),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME)));

		addTable("ENGINE", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ENGINENAME", VARCHAR_255),
				Pair.with("MODIFIEDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("TYPE", VARCHAR_255)));

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

		addTable("ENGINERELATION", Arrays.asList(
				Pair.with("RELATIONID", VARCHAR_255),
				Pair.with("ENGINE", VARCHAR_255),
				Pair.with("INSTANCERELATIONID", VARCHAR_255),
				Pair.with("SOURCECONCEPTID", VARCHAR_255),
				Pair.with("TARGETCONCEPTID", VARCHAR_255),
				Pair.with("SOURCEPROPERTY", VARCHAR_255),
				Pair.with("TARGETPROPERTY", VARCHAR_255),
				Pair.with("RELATIONNAME", VARCHAR_255)));

		addTable("KVSTORE", Arrays.asList(
				Pair.with("K", VARCHAR_800),
				Pair.with("V", VARCHAR_800)));

		addTable("RELATION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("SOURCEID", VARCHAR_255),
				Pair.with("TARGETID", VARCHAR_255),
				Pair.with("GLOBALID", VARCHAR_255)));

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
		owler.addRelation("ENGINECONCEPT", "ENGINERELATION",
				"ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.SOURCECONCEPTID");
		owler.addRelation("ENGINECONCEPT", "ENGINERELATION",
				"ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.TARGETCONCEPTID");

		owler.addRelation("ENGINERELATION", "RELATION", "ENGINERELATION.RELATIONID.RELATION.ID");

		owler.addRelation("CONCEPT", "RELATION", "CONCEPT.LOCALCONCEPTID.RELATION.SOURCEID");
		owler.addRelation("CONCEPT", "RELATION", "CONCEPT.LOCALCONCEPTID.RELATION.TARGETID");

		owler.addRelation("METAMODELPOSITION", "ENGINE", "METAMODELPOSITION.ENGINEID.ENGINE.ID");
	}

}
