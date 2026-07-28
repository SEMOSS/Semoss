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
package prerna.reactor.database.migration;

import java.util.Properties;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Reports whether {@code ENABLE_MIGRATIONS} is set on an engine's smss --
 * used by the Migrations tab to decide whether to show itself at all,
 * without requiring the OWNER-only access {@code GetEngineSMSS} needs (this
 * only reveals one boolean, not the full smss content, so a view-level check
 * is sufficient).
 *
 * <pre>GetEngineMigrationsEnabled(engine = ["&lt;engineId&gt;"]);</pre>
 *
 * Returns: BOOLEAN.
 */
public class GetEngineMigrationsEnabledReactor extends AbstractReactor {

	public GetEngineMigrationsEnabledReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String rawEngineId = this.keyValue.get(this.keysToGet[0]);
		if (rawEngineId == null || rawEngineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide an engine id to check");
		}

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("User must be logged in");
		}
		String engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, rawEngineId);
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to view it");
		}

		String smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		boolean enabled = false;
		if (smssFile != null) {
			Properties prop = Utility.loadProperties(smssFile);
			enabled = Boolean.parseBoolean(prop.getProperty(Constants.ENABLE_MIGRATIONS));
		}

		return new NounMetadata(enabled, PixelDataType.BOOLEAN);
	}

}
