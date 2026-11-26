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

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Backs up the file of a database for an admin, whatever permission the admin
 * holds on it. The file of an H2 database or the journal of an RDF database is
 * copied into the {@code backup} folder of the database, with the date and time
 * in its name. This is the admin counterpart to
 * {@link prerna.reactor.utils.BackupDatabaseReactor}, which backs up a database
 * its user owns.
 */
public class AdminBackupDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminBackupDatabaseReactor.class);

	private static final String BACKUP_FOLDER = "backup";
	private static final String BACKUP_DATE_FORMAT = "ddMMyyyy_HHmmss";
	private static final String UNSUPPORTED_DATABASE_MESSAGE = "Backup failed! "
			+ "Note: only H2 and RDF database support backups.";

	/**
	 * Creates the reactor with the database key.
	 */
	public AdminBackupDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	/**
	 * Copies the file of the database into its backup folder, once the user is
	 * known to be an admin.
	 *
	 * @return null, since a backup returns no value
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
			throw new IllegalArgumentException("Invalid database!");
		}

		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if (engine == null) {
			throw new IllegalArgumentException("Invalid database!");
		}
		DATABASE_TYPE dbType = engine.getDatabaseType();

		String dbDir = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		DateFormat dateFormat = new SimpleDateFormat(BACKUP_DATE_FORMAT);
		String todayDate = dateFormat.format(new Date());

		if (dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
			File originalFile = new File(dbDir + DIR_SEPARATOR + "database.mv.db");
			File newFile = new File(
					dbDir + DIR_SEPARATOR + BACKUP_FOLDER + DIR_SEPARATOR + "database_" + todayDate + ".mv.db");
			copyFile(databaseId, originalFile, newFile);
		} else if (dbType == IDatabaseEngine.DATABASE_TYPE.SESAME) {
			File originalFile = new File(dbDir + DIR_SEPARATOR + databaseId + ".jnl");
			File newFile = new File(
					dbDir + DIR_SEPARATOR + BACKUP_FOLDER + DIR_SEPARATOR + databaseId + "_" + todayDate + ".jnl");
			copyFile(databaseId, originalFile, newFile);
		} else {
			throw new IllegalArgumentException(UNSUPPORTED_DATABASE_MESSAGE);
		}
		return null;
	}

	/**
	 * Copies the file of a database to its backup location.
	 *
	 * @param databaseId   the id of the database, for the log
	 * @param originalFile the file of the database
	 * @param newFile      where the backup is written
	 */
	private void copyFile(String databaseId, File originalFile, File newFile) {
		if (!originalFile.exists()) {
			throw new IllegalArgumentException(UNSUPPORTED_DATABASE_MESSAGE);
		}
		try {
			FileUtils.copyFile(originalFile, newFile);
		} catch (IOException e) {
			classLogger.error("Failed to back up database {} to {}", Utility.cleanLogString(databaseId), newFile, e);
			throw new IllegalArgumentException("Database backup failed! Try again.");
		}
	}

	/**
	 * Describes the reactor and its database key.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Backs up the file of a database regardless of the caller's database permissions. The \
				calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to BackupDatabase, which backs up a database the caller owns.

				The file of an H2 database or the journal of an RDF database is copied into the backup folder of \
				the database, with the date and time in its name.

				Inputs:
				  database (required) - the database id.

				Returns no value.
				""").key(ReactorKeysEnum.DATABASE, "Id of the database to back up");
	}

}
