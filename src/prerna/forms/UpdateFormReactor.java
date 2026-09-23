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
package prerna.forms;

import java.io.IOException;
import java.util.Map;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateFormReactor extends AbstractReactor {

	private static final String FORM_DATA = "form_input";

	private static final Logger classLogger = LogManager.getLogger(UpdateFormReactor.class);

	public UpdateFormReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), FORM_DATA};
	}

	@Override
	public NounMetadata execute() {
		String userId = null;
		User user = this.insight.getUser();
		if(user.getAccessToken(AuthProvider.CAC) != null) {
			userId = user.getAccessToken(AuthProvider.CAC).getId();
		} else if(user.getAccessToken(AuthProvider.SAML) != null) {
			// if not CAC - we are using SMAL
			userId = user.getAccessToken(AuthProvider.SAML).getId();
		}
		if(userId == null) {
			throw new IllegalArgumentException("Could not identify user");
		}
		
		String databaseName = this.store.getGenRowStruct(this.keysToGet[0]).get(0) + "";
		Map<String, Object> engineHash = (Map<String, Object>) this.store.getGenRowStruct(FORM_DATA).get(0);
		databaseName = testDatabaseId(databaseName, true);

		IDatabaseEngine engine = Utility.getDatabase(databaseName);
		AbstractFormBuilder formbuilder = FormFactory.getFormBuilder(engine);
		try {
			formbuilder.commitFormData(engineHash, userId);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	public String getName()
	{
		return "UpdateForms";
	}

}
