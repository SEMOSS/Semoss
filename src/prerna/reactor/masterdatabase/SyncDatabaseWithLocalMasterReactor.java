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

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.masterdatabase.AddToMasterDB;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class SyncDatabaseWithLocalMasterReactor extends AbstractReactor {

	public static final String CLASS_NAME = SyncDatabaseWithLocalMasterReactor.class.getName();

	public SyncDatabaseWithLocalMasterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() + "," + ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException(
					"Database " + databaseId + " does not exist or user does not have access to app");
		}

		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting to synchronize metadata");

		logger.info("Starting to remove existing metadata");
		DeleteFromMasterDB remover = new DeleteFromMasterDB();
		remover.deleteEngineRDBMS(databaseId);
		logger.info("Finished removing existing metadata");

		logger.info("Starting to add metadata");
		String smssFile = (String) DIHelper.getInstance().getEngineProperty(databaseId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(smssFile);
		AddToMasterDB adder = new AddToMasterDB();
		adder.registerEngineLocal(smssFile, prop);
		logger.info("Done adding new metadata");

		logger.info("Synchronization complete");
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata(
				"Successfully synchronized " + MasterDatabaseUtility.getDatabaseAliasForId(databaseId) + "'s metadata",
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Synchronizes a database's metadata with the local master database by removing existing metadata and re-registering from the database files";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id to synchronize metadata for";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
