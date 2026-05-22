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
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.InsightFile;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.export.AbstractExportTxtReactor;
import prerna.reactor.export.ToExcelReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class AdminExportAllUsersReactor extends ToExcelReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminExportAllUsersReactor.class);

	private static final String CLASS_NAME = AdminExportAllUsersReactor.class.getName();

	public AdminExportAllUsersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FILE_NAME.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.PASSWORD.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		this.logger = getLogger(CLASS_NAME);
		this.includeLogo = false;
		// must have a password for the file
		String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
		if (password == null || password.isEmpty()) {
			throw new IllegalArgumentException("Must provide a password to encrypt the file");
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PASSWORD"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__SALT"));

		IRDBMSEngine database = SystemEngineRegistry.getSecurityDb();
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
			this.task = new BasicIteratorTask(qs, iterator);

			// get a random file name
			String prefixName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
			if (prefixName == null || prefixName.isEmpty()) {
				prefixName = "All_Users";
			}
			String exportName = AbstractExportTxtReactor.getExportFileName(user, prefixName, "xlsx");
			// grab file path to write the file
			this.fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
			// if the file location is not defined generate a random path and set
			// location so that the front end will download
			if (this.fileLocation == null) {
				String insightFolder = this.insight.getInsightFolder();
				{
					File f = new File(Utility.normalizePath(insightFolder));
					if (!f.exists()) {
						f.mkdirs();
					}
				}
				this.fileLocation = insightFolder + DIR_SEPARATOR + exportName;
			}

			// store the insight file
			// in the insight so the FE can download it
			// only from the given insight
			String downloadKey = UUID.randomUUID().toString();
			InsightFile insightFile = new InsightFile();
			insightFile.setFilePath(this.fileLocation);
			insightFile.setDeleteOnInsightClose(true);
			insightFile.setFileKey(downloadKey);
			this.insight.addExportFile(downloadKey, insightFile);
			NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
					PixelOperationType.FILE_DOWNLOAD);

			buildTask();
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully generated the excel file"));
			return retNoun;
		} catch (Exception e) {
			classLogger.error("Unable to export all users.", e);
			throw new IllegalArgumentException(
					"An error occurred retrieving the users. Message is : " + e.getMessage());
		} finally {
			if (iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error("Unable to export all users.", e);
				}
			}
		}
	}

}
