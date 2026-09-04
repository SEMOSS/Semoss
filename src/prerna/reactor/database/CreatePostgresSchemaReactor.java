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
package prerna.reactor.database;

import java.util.Locale;
import java.util.regex.Pattern;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * Creates a PostgreSQL schema on the database engine dedicated to this reactor.
 */
public class CreatePostgresSchemaReactor extends AbstractReactor {

	static final String DATABASE_ID = "9e871b9e-46dc-4d96-97ce-ed147136d1c8";
	private static final int POSTGRES_IDENTIFIER_MAX_LENGTH = 63;
	private static final Pattern SAFE_SCHEMA_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

	public CreatePostgresSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SCHEMA.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new SemossPixelException("User must be signed in to create a database schema");
		}
		if (!SecurityEngineUtils.userCanEditEngine(user, DATABASE_ID)) {
			throw new SemossPixelException("User does not have permission to modify this database");
		}

		String schemaName = validateSchemaName(this.keyValue.get(ReactorKeysEnum.SCHEMA.getKey()));
		IDatabaseEngine database = Utility.getDatabase(DATABASE_ID);
		if (!(database instanceof IRDBMSEngine)) {
			throw new SemossPixelException("The configured database is not a relational database");
		}

		IRDBMSEngine rdbms = (IRDBMSEngine) database;
		if (rdbms.getDbType() != RdbmsTypeEnum.POSTGRES) {
			throw new SemossPixelException("The configured database is not PostgreSQL");
		}

		try {
			rdbms.insertData(buildCreateSchemaSql(schemaName));
		} catch (Exception e) {
			throw new SemossPixelException("Unable to create PostgreSQL schema: " + e.getMessage());
		}

		NounMetadata result = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE);
		result.addAdditionalReturn(getSuccess("PostgreSQL schema '" + schemaName + "' is available"));
		return result;
	}

	static String validateSchemaName(String schemaName) {
		if (schemaName == null || schemaName.trim().isEmpty()) {
			throw new IllegalArgumentException("Schema name is required");
		}

		String trimmedName = schemaName.trim();
		if (trimmedName.length() > POSTGRES_IDENTIFIER_MAX_LENGTH || !SAFE_SCHEMA_NAME.matcher(trimmedName).matches()) {
			throw new IllegalArgumentException(
					"Schema name must be 1-63 characters and contain only letters, numbers, and underscores; it cannot start with a number");
		}
		if (trimmedName.toLowerCase(Locale.ROOT).startsWith("pg_")) {
			throw new IllegalArgumentException("Schema names beginning with 'pg_' are reserved by PostgreSQL");
		}
		return trimmedName;
	}

	static String buildCreateSchemaSql(String schemaName) {
		return "CREATE SCHEMA IF NOT EXISTS \"" + schemaName + "\"";
	}

	@Override
	public String getReactorDescription() {
		return "Creates a schema on the configured PostgreSQL database";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.SCHEMA.getKey().equals(key)) {
			return "The PostgreSQL schema name to create";
		}
		return super.getDescriptionForKey(key);
	}
}
