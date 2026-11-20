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
package prerna.reactor.utils;

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
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class BackupDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(BackupDatabaseReactor.class);

	public BackupDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if (databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Invalid database!");
		}

		User user = this.insight.getUser();
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(user, databaseId);
		if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityEngineUtils.userIsOwner(user, databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId
					+ " does not exist or user does not have permissions to back up the database. "
					+ "User must be the owner or an administrator to perform this function.");
		}

		// get engine details
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if (engine == null) {
			throw new IllegalArgumentException("Invalid database!");
		}
		DATABASE_TYPE dbType = engine.getDatabaseType();

		// get db directory and dates for renaming the backup file
		String dbDir = EngineUtility.getSpecificEngineBaseFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		DateFormat dateFormat = new SimpleDateFormat("ddMMyyyy_HHmmss");
		Date date = new Date();
		String todayDate = dateFormat.format(date);

		// only backup if its an RDBMS or RDF
		if (dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
			File originalFile = new File(dbDir + DIR_SEPARATOR + "database.mv.db");
			File newFile = new File(
					dbDir + DIR_SEPARATOR + "backup" + DIR_SEPARATOR + "database_" + todayDate + ".mv.db");
			copyFile(originalFile, newFile);
		} else if (dbType == IDatabaseEngine.DATABASE_TYPE.SESAME) {
			File originalFile = new File(dbDir + DIR_SEPARATOR + databaseId + ".jnl");
			File newFile = new File(
					dbDir + DIR_SEPARATOR + "backup" + DIR_SEPARATOR + databaseId + "_" + todayDate + ".jnl");
			copyFile(originalFile, newFile);
		} else {
			throw new IllegalArgumentException("Backup failed! Note: only H2 and RDF database support backups.");
		}
		return null;
	}

	/**
	 * 
	 * @param prop
	 * @param dbDir
	 * @param originalFile
	 * @param newFile
	 */
	private void copyFile(File originalFile, File newFile) {
		if (originalFile.exists()) {
			try {
				FileUtils.copyFile(originalFile, newFile);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Database backup failed! Try again.");
			}
		} else {
			throw new IllegalArgumentException("Backup failed! Note: only H2 and RDF database support backups.");
		}
	}
}
