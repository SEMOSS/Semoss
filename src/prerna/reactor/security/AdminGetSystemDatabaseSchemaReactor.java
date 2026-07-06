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
package prerna.reactor.security;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.owl.AbstractOwlCreator.OwlColumn;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.SystemDatabaseSchemaUtils;

public class AdminGetSystemDatabaseSchemaReactor extends AbstractReactor {

	public AdminGetSystemDatabaseSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException(
					"User is not an admin and does not have access. Please login as an admin");
		}

		String databaseId = getDatabaseId();

		// pulls the table/column/datatype from the system database's OWL creator;
		// throws if the id is not one of the SystemDefaultDatabases
		List<OwlColumn> columns = SystemDatabaseSchemaUtils.getSystemDatabaseSchema(databaseId);

		List<Map<String, Object>> schema = new ArrayList<>(columns.size());
		for (OwlColumn column : columns) {
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("table", column.tableName());
			row.put("column", column.columnName());
			row.put("dataType", column.dataType());
			schema.add(row);
		}

		return new NounMetadata(schema, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private String getDatabaseId() {
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must define a database id");
		}
		if (grs.size() > 1) {
			throw new IllegalArgumentException("Can only define one database within this call");
		}
		return grs.get(0).toString();
	}

	@Override
	public String getReactorDescription() {
		return "Admin reactor that returns the table, column, and datatype of the OWL schema for a system default database";
	}

}
