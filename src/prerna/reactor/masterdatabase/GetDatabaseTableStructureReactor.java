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
package prerna.reactor.masterdatabase;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;

public class GetDatabaseTableStructureReactor extends AbstractReactor {

	/*
	 * PAYLOAD MUST MATCH THAT OF {@link
	 * prerna.sablecc2.reactor.frame.GetFrameTableStructureReactor}
	 */

	private static final String CLASS_NAME = GetDatabaseTableStructureReactor.class.getName();

	public GetDatabaseTableStructureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null) {
			throw new IllegalArgumentException("Need to define the database to get the structure from from");
		}
		engineId = MasterDatabaseUtility.testDatabaseIdIfAlias(engineId);

		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Database does not exist or user does not have access to database");
		}

		Logger logger = getLogger(CLASS_NAME);
		logger.info("Pulling database structure for database " + engineId);
		// if cache exists, return from there
		List<Object[]> data = EngineSyncUtility.getDatabaseStructureCache(engineId);
		if (data == null) {
			data = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
			// store the cache for the database structure
			EngineSyncUtility.setDatabaseStructureCache(engineId, data);
		}
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}

	@Override
	public String getReactorDescription() {
		return """
				Returns the complete structure of a database, including all concepts and their properties.

				Each result row contains:
				  - PARENTSEMOSSNAME   : Logical table name (RDBMS) or vertex name (Graph)
				  - SEMOSSNAME         : Logical column name (RDBMS) or property name (Graph)
				  - PROPERTY_TYPE      : Data type of the column or property
				  - PK                 : Whether this row represents a graph vertex itself, rather than a property on it (only relevant for rdf/graph dbs)
				  - PARENTPHYSICALNAME : Physical table/vertex name as stored in the database
				  - PHYSICALNAME       : Physical column/property name as stored in the database

				Notes:
				  - Logical names (PARENTSEMOSSNAME, SEMOSSNAME) reflect the schema as modeled in SEMOSS
				  - Physical names (PARENTPHYSICALNAME, PHYSICALNAME) reflect the actual database values
				  - For Graph: a row where PK is true and SEMOSSNAME is empty represents the vertex itself, \
				not a property
				  - Results are ordered by PARENTSEMOSSNAME, PK, then SEMOSSNAME
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
