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
package prerna.reactor.database.upload.rdbms.internal;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLiteQueryUtil;

/**
 * Creates a new, empty H2 or SQLite database without requiring any file upload
 * or predefined schema. Tables can be added later via SQL or the metamodel API.
 *
 * Pixel usage:
 * <ul>
 * <li>CreateEmptyRdbmsDatabase(database=["MyDb"]);</li>
 * <li>CreateEmptyRdbmsDatabase(database=["MyDb"], rdbmsType=["SQLITE"]);</li>
 * <li>CreateEmptyRdbmsDatabase(database=["MyDb"], username=["admin"],
 * password=["secret"]);</li>
 * <li>CreateEmptyRdbmsDatabase(database=["MyDb"], rdbmsType=["SQLITE"],
 * username=["admin"], password=["secret"]);</li>
 * </ul>
 */
public class CreateEmptyRdbmsDatabaseReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateEmptyRdbmsDatabaseReactor.class);

	private static final String RDBMS_TYPE_KEY = "rdbmsType";
	private static final String USERNAME_KEY = "username";
	private static final String PASSWORD_KEY = "password";

	protected transient Logger logger;
	protected transient String databaseId;
	protected transient String databaseName;
	protected transient IDatabaseEngine database;
	protected transient File databaseFolder;
	protected transient File tempSmss;
	protected transient File smssFile;
	protected transient boolean error = false;

	public CreateEmptyRdbmsDatabaseReactor() {
		this.keysToGet = new String[] { UploadInputUtility.DATABASE, RDBMS_TYPE_KEY, USERNAME_KEY, PASSWORD_KEY };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.logger = getLogger(this.getClass().getName());
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a database",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyDatabaseAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		this.databaseName = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		RdbmsTypeEnum rdbmsType = getRdbmsType();

		try {
			this.databaseId = UUID.randomUUID().toString();
			this.logger.info("Start validating database");
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.DATABASE, user, this.databaseName, this.databaseId);
			this.logger.info("Done validating database");

			this.logger.info("Start generating database folder");
			this.databaseFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.DATABASE,
					this.databaseId, this.databaseName);
			this.logger.info("Complete");

			this.logger.info("Create metadata for database...");
			File owlFile = UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, this.databaseId,
					this.databaseName);
			this.logger.info("Complete");

			this.logger.info("Create properties file for database...");
			String username = getString(USERNAME_KEY, "sa");
			String password = getString(PASSWORD_KEY, "");
			Map<String, Object> credDetails = new LinkedHashMap<>();
			credDetails.put(AbstractSqlQueryUtil.USERNAME, username);
			credDetails.put(AbstractSqlQueryUtil.PASSWORD, password);
			if (rdbmsType == RdbmsTypeEnum.SQLITE) {
				this.tempSmss = UploadUtilities.createTemporaryExternalRdbmsSmss(this.databaseId, this.databaseName,
						owlFile, RDBMSNativeEngine.class.getName(), RdbmsTypeEnum.SQLITE,
						SQLiteQueryUtil.BASE_SQLITE_FILE_CONNECTION, credDetails, Collections.emptyMap());
			} else {
				this.tempSmss = UploadUtilities.createTemporaryExternalRdbmsSmss(this.databaseId, this.databaseName,
						owlFile, RDBMSNativeEngine.class.getName(), RdbmsTypeEnum.H2_DB,
						H2QueryUtil.BASE_H2_FILE_CONNECTION, credDetails, Collections.emptyMap());
			}
			this.logger.info("Complete");

			this.logger.info("Create database store...");
			this.database = new RDBMSNativeEngine();
			this.database.setEngineId(this.databaseId);
			this.database.setEngineName(this.databaseName);
			Properties smssProps = Utility.loadProperties(this.tempSmss.getAbsolutePath());
			smssProps.put("TEMP", true);
			this.database.open(smssProps);
			this.logger.info("Complete");

			this.smssFile = new File(this.tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(this.tempSmss, this.smssFile);
			this.tempSmss.delete();
			this.database.setSmssFilePath(this.smssFile.getAbsolutePath());
			UploadUtilities.addEngineToDIHelper(this.databaseId, this.databaseName, this.database, this.smssFile);

			this.logger.info("Process database metadata to allow for traversing across databases");
			UploadUtilities.updateMetadata(this.databaseId, user);
			this.logger.info("Complete");
		} catch (Exception e) {
			classLogger.error("Failed to create empty {} database '{}': {}", rdbmsType.getLabel(), this.databaseName,
					e.getMessage(), e);
			this.error = true;
			if (e instanceof SemossPixelException) {
				throw (SemossPixelException) e;
			} else {
				NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING,
						PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} finally {
			if (this.error) {
				UploadUtilities.cleanUpCreateNewError(this.database, this.databaseId, this.tempSmss, this.smssFile,
						this.databaseFolder);
			}
		}

		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider ap : logins) {
			SecurityEngineUtils.addEngineOwner(this.databaseId, user.getAccessToken(ap).getId());
		}

		ClusterUtil.pushEngine(this.databaseId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), this.databaseId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}

	/**
	 * 
	 * @return
	 */
	private RdbmsTypeEnum getRdbmsType() {
		GenRowStruct grs = this.store.getGenRowStruct(RDBMS_TYPE_KEY);
		if (grs != null && !grs.isEmpty()) {
			String typeStr = grs.get(0).toString().trim().toUpperCase();
			if (typeStr.equals("SQLITE")) {
				return RdbmsTypeEnum.SQLITE;
			}
		}
		return RdbmsTypeEnum.H2_DB;
	}

	@Override
	public String getReactorDescription() {
		return "Creates a new, empty H2 or SQLite database without requiring a file upload or predefined schema. "
				+ "Tables can be added later via SQL or the metamodel API.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(UploadInputUtility.DATABASE)) {
			return "The name of the new database to create";
		}
		if (key.equals(RDBMS_TYPE_KEY)) {
			return "The database engine type: H2_DB (default) or SQLITE";
		}
		if (key.equals(USERNAME_KEY)) {
			return "The database username (default: sa for H2, empty for SQLite)";
		}
		if (key.equals(PASSWORD_KEY)) {
			return "The database password (default: empty)";
		}
		return super.getDescriptionForKey(key);
	}

}
