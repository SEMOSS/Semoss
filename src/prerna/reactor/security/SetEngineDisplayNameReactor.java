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

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class SetEngineDisplayNameReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SetEngineDisplayNameReactor.class);

	public SetEngineDisplayNameReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "name" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.ENGINE.getKey()));
		if (!SecurityEngineUtils.userIsOwner(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine " + engineId
					+ " does not exist or user does not have permissions to set the display name. User must be the owner to perform this function.");
		}
		String displayName = this.keyValue.get("name");
		try {
			SecurityEngineUtils.setEngineDisplayName(this.insight.getUser(), engineId, displayName);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e.getMessage());
		}

		// update smss file, in-memory engine object, and push to cloud
		IEngine engine = Utility.getEngine(engineId);
		if (engine != null) {
			String smssFilePath = engine.getSmssFilePath();
			try {
				Utility.changePropertiesFileValue(smssFilePath, Constants.ENGINE_DISPLAY_NAME, displayName);
			} catch (IOException e) {
				classLogger.error("Error occurred updating the engine smss file for display name", e);
			}
			engine.setDisplayName(displayName);
			ClusterUtil.pushEngineSmss(engineId);
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the display name for the engine"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Set the display name for an engine";
	}

}
