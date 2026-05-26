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
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetDatabaseConnectionsReactor extends AbstractReactor {

	public GetDatabaseConnectionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String databaseId = getDatabaseId();
		if (databaseId != null) {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
		}

		List<String> appliedDatabaseFilters = new Vector<String>();

		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
		List<String> databaseFilters = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		if (!databaseFilters.isEmpty()) {
			if (databaseId != null) {
				// need to make sure it is a valid engine id
				if (!databaseFilters.contains(databaseId)) {
					throw new IllegalArgumentException(
							"Database does not exist or user does not have access to database");
				}
				// we are good
				appliedDatabaseFilters.add(databaseId);
			} else {
				// set default as filters
				appliedDatabaseFilters = databaseFilters;
			}
		} else {
			if (databaseId != null) {
				appliedDatabaseFilters.add(databaseId);
			}
		}

		List<String> inputColumnValues = getColumns();
		List<String> localConceptIds = MasterDatabaseUtility.getLocalConceptIdsFromLogicalName(inputColumnValues);
		localConceptIds.addAll(MasterDatabaseUtility.getConceptualIdsWithSimilarLogicalNames(localConceptIds));

		List<Map<String, Object>> data = MasterDatabaseUtility.getDatabaseConnections(localConceptIds,
				appliedDatabaseFilters);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
	}

	/**
	 * Getter for the list
	 * 
	 * @return
	 */
	private List<String> getColumns() {
		// is it defined within store
		{
			GenRowStruct cGrs = this.store.getGenRowStruct(this.keysToGet[0]);
			if (cGrs != null && !cGrs.isEmpty()) {
				List<String> columns = new Vector<String>();
				for (int i = 0; i < cGrs.size(); i++) {
					String value = cGrs.get(i).toString().toLowerCase();
					if (value.contains("__")) {
						columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
					} else {
						columns.add(value.replaceAll("\\s+", "_"));
					}
				}
				return columns;
			}
		}

		// is it inline w/ currow
		List<String> columns = new Vector<String>();
		for (int i = 0; i < this.curRow.size(); i++) {
			String value = this.curRow.get(i).toString().toLowerCase();
			if (value.contains("__")) {
				columns.add(value.split("__")[1].replaceAll("\\s+", "_"));
			} else {
				columns.add(value.replaceAll("\\s+", "_"));
			}
		}
		return columns;
	}

	private String getDatabaseId() {
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(ReactorKeysEnum.COLUMNS.getKey())) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Returns database connections and traversal options for specified columns/concepts";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.COLUMNS.getKey())) {
			return "The columns/concepts to find connections for";
		} else if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "Optional database id to filter connections to a specific database";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
