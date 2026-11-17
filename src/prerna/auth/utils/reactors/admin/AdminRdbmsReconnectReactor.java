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

import java.sql.SQLException;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Drops the open connections of an RDBMS database for an admin, whatever
 * permission the admin holds on it, so the next query connects again: the
 * connection pool is closed when the database pools its connections, and its
 * one connection otherwise. This is the admin counterpart to
 * {@link prerna.reactor.database.RdbmsReconnectReactor}, which does the same
 * for a database its user can edit.
 */
public class AdminRdbmsReconnectReactor extends AbstractReactor {

	/**
	 * Creates the reactor with the database key.
	 */
	public AdminRdbmsReconnectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	/**
	 * Closes the connection pool or the connection of the database, once the user
	 * is known to be an admin.
	 *
	 * @return true once the connections are closed, or false with the error when
	 *         closing fails
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

		IDatabaseEngine database = Utility.getDatabase(databaseId);
		if (!(database instanceof IRDBMSEngine rdbms)) {
			throw new IllegalArgumentException("Database must be an RDBMS native engine");
		}

		try {
			if (rdbms.isConnectionPooling()) {
				rdbms.closeDataSource();
			} else {
				rdbms.getConnection().close();
			}
		} catch (SQLException e) {
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(getError(e.getMessage()));
			return noun;
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
				Admin Only: Re-establishes the connection of an RDBMS database regardless of the caller's database \
				permissions. The calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to RdbmsReconnect, which does the same for a database the caller can \
				edit.

				The connection pool of the database is closed, or its one connection when it does not pool, so the \
				next query connects again.

				Inputs:
				  database (required) - the id of the RDBMS database.

				Returns true (BOOLEAN) once the connections are closed, or false with the error message.
				""").key(ReactorKeysEnum.DATABASE, "Id of the RDBMS database to reconnect").output(Boolean.class,
				"True once the connections are closed");
	}

}
