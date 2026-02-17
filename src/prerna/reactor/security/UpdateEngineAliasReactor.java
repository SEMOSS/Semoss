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

import java.io.File;
import java.util.Properties;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UpdateEngineAliasReactor extends AbstractReactor {

	public UpdateEngineAliasReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.ALIAS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide an engine id or alias");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		String newAlias = this.keyValue.get(ReactorKeysEnum.ALIAS.getKey());
		if (newAlias == null || (newAlias = newAlias.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide a non-empty alias");
		}

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to edit");
		}

		String sanitizedName = Utility.sanitizeEngineName(newAlias);
		if (SecurityEngineUtils.engineAliasOrNameExistsForDifferentId(engineId, newAlias, sanitizedName)) {
			throw new IllegalArgumentException("Engine alias already exists. Please provide a unique alias.");
		}

		SecurityEngineUtils.updateEngineAlias(engineId, newAlias);

		String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";
		if (smssFile != null && !smssFile.isEmpty()) {
			File smssF = new File(smssFile);
			if (smssF.exists() && smssF.isFile()) {
				try {
					SmssUtilities.updateEngineAlias(smssFile, newAlias);
				} catch (Exception e) {
					throw new IllegalArgumentException("Unable to update engine alias in smss file");
				}
			}
		}

		IEngine engine = Utility.getEngine(engineId);
		if (engine != null) {
			engine.setEngineName(newAlias);
			Properties smssProp = engine.getSmssProp();
			if (smssProp != null) {
				smssProp.setProperty(Constants.ENGINE_ALIAS, newAlias);
			}
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully updated the engine alias"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Update the human-friendly alias for an engine";
	}
}
