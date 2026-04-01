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

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceRecordByIdReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceRecordByIdReactor.class);

	private static final String SOBJECT_NAME = "sObjectName";
	private static final String RECORD_ID = "recordId";

	public SalesforceRecordByIdReactor() {
		this.keysToGet = new String[] { SOBJECT_NAME, RECORD_ID };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String sObjectName = this.keyValue.get(this.keysToGet[0]);
		String recordId = this.keyValue.get(this.keysToGet[1]);
		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);
			Map<String, Object> result = SalesforceHelper.fetchRecordById(accessToken, instanceUrl, sObjectName,
					recordId);

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (Exception e) {
			classLogger.error("Error while fetching Salesforce record by Id", e);
			throw new SemossPixelException("Unable to retrieve Salesforce record: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Fetches a single Salesforce record by object API name and record Id.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SOBJECT_NAME.equals(key)) {
			return "Required Salesforce object API name (for example, Account).";
		} else if (RECORD_ID.equals(key)) {
			return "Required Salesforce record Id to retrieve.";
		}
		return super.getDescriptionForKey(key);
	}

}
