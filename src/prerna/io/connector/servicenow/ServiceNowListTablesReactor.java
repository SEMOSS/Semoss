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
package prerna.io.connector.servicenow;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ServiceNowListTablesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowListTablesReactor.class);

	private static final String INSTANCE_URL = "instanceURL";

	public ServiceNowListTablesReactor() {
		this.keysToGet = new String[] { INSTANCE_URL };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		try {
			String instanceURL = this.keyValue.get(this.keysToGet[0]);

			User user = this.insight.getUser();
			String accessToken = ServiceNowUtils.getServiceNowAccessToken(user);

			Map<String, Object> allTables = ServiceNowHelper.getAllTables(instanceURL, accessToken);
			return new NounMetadata(allTables, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Error retrieving ServiceNow table list.", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	@Override
	public String getReactorDescription() {
		return "Fetches all the tables present in a ServiceNow instance via OAuth authentication.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(INSTANCE_URL)) {
			return "Required URL of the ServiceNow user's instance.";
		}
		return super.getDescriptionForKey(key);
	}

}
