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
package prerna.auth.utils.reactors.admin;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Reloads a database for an admin, whatever permission the admin holds on it:
 * the database is closed, its loaded settings are dropped, and it is opened
 * again from its smss file. This is the admin counterpart to
 * {@link prerna.reactor.database.ReloadDatabaseReactor}, which reloads a
 * database its user can edit.
 */
public class AdminReloadDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminReloadDatabaseReactor.class);

	/**
	 * Creates the reactor with the database key.
	 */
	public AdminReloadDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	/**
	 * Closes the database and opens it again from its smss file, once the user is
	 * known to be an admin and the database is known to exist.
	 *
	 * @return true once the database is open again
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Need to define the database");
		}
		if (!SecurityEngineUtils.engineExists(databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist");
		}

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		String smssFilePath = database.getSmssFilePath();
		try {
			database.close();
		} catch (IOException e) {
			classLogger.error("Failed to close database {} before reloading it", Utility.cleanLogString(databaseId),
					e);
		}
		database.setSmssProp(null);
		try {
			database.open(smssFilePath);
		} catch (Exception e) {
			classLogger.error("Failed to open database {} again while reloading it",
					Utility.cleanLogString(databaseId), e);
			throw new IllegalArgumentException(
					"An error occurred reloading the database. Please reach out to an administrator for assistance");
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Describes the reactor, its database key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Reloads a database regardless of the caller's database permissions. The calling user \
				must be an application admin or the reactor throws "User must be an admin to perform this function".

				This is the admin counterpart to ReloadDatabase, which reloads a database the caller can edit.

				The database is closed and opened again from its smss file, so it picks up changed settings.

				Inputs:
				  database (required) - the database id.

				Returns true (BOOLEAN) once the database is open again.
				""").key(ReactorKeysEnum.DATABASE, "Id of the database to reload").output(Boolean.class,
				"True once the database is open again");
	}

}
