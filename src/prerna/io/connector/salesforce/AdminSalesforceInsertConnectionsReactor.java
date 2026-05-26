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
package prerna.io.connector.salesforce;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminExternalConnectorsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminSalesforceInsertConnectionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminSalesforceInsertConnectionsReactor.class);

	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String ALIAS = "alias";

	public AdminSalesforceInsertConnectionsReactor() {
		this.keysToGet = new String[] { CLIENT_ID, CLIENT_SECRET, ALIAS };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminExternalConnectorsUtils adminUtils = SecurityAdminExternalConnectorsUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		this.organizeKeys();

		String clientId = this.keyValue.get(this.keysToGet[0]);
		String clientSecret = this.keyValue.get(this.keysToGet[1]);
		String alias = this.keyValue.get(this.keysToGet[2]);

		Map<Object, Object> responseMap = new HashMap<>();
		try {
			String profileId = adminUtils.insertSalesforceConnection(clientId, clientSecret, alias);
			responseMap.put("id", profileId);
			responseMap.put("success", profileId != null && !profileId.isEmpty());
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to insert Salesforce connection.", e);
			String error = "Error inserting Salesforce connection: " + e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}

	@Override
	public String getReactorDescription() {
		return "Creates and stores a Salesforce connection entry (client id, client secret, and alias).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (CLIENT_ID.equals(key)) {
			return "Required Salesforce connected-app client id.";
		} else if (CLIENT_SECRET.equals(key)) {
			return "Required Salesforce connected-app client secret.";
		} else if (ALIAS.equals(key)) {
			return "Required unique alias used to identify this saved Salesforce connection.";
		}
		return super.getDescriptionForKey(key);
	}
}
