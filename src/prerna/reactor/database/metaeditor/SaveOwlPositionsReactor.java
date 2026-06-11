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
package prerna.reactor.database.metaeditor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class SaveOwlPositionsReactor extends AbstractReactor {

	protected static final Logger classLogger = LogManager.getLogger(SaveOwlPositionsReactor.class);

	public SaveOwlPositionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.POSITION_MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		String databaseId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		if (databaseId == null) {
			throw new IllegalArgumentException("Must pass in the database id");
		}
		// run security tests + alias replacement
		databaseId = testDatabaseId(databaseId, true);
		Map<String, Object> positions = getPositionMap();
		if (positions == null) {
			// since we allow connecting to an empty database, there might be no positions
			// to save
			positions = new HashMap<>();
		}

		// write the json file in the database folder
		// just put it in the same location as the OWL
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		File positionFile = database.getOwlPositionFile();

		try (FileWriter writer = new FileWriter(positionFile)) {
			GSON.toJson(positions, writer);
		} catch (IOException e) {
			classLogger.error("Unable to write the positions map to location {}", positionFile.getAbsolutePath(), e);
		}
		ClusterUtil.pushOwl(databaseId);
		// update the positions cache
		EngineSyncUtility.setMetamodelPositions(databaseId, positions);
		MasterDatabaseUtility.saveMetamodelPositions(databaseId, positions);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private Map<String, Object> getPositionMap() {
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> maps = grs.getNounsOfType(PixelDataType.MAP);
			if (maps != null && !maps.isEmpty()) {
				return (Map<String, Object>) maps.get(0).getValue();
			}
		}

		// check is passed as direct input
		List<NounMetadata> maps = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (maps != null && !maps.isEmpty()) {
			return (Map<String, Object>) maps.get(0).getValue();
		}

		return null;
	}

}
