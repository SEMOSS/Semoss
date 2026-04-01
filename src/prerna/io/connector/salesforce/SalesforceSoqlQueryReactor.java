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

public class SalesforceSoqlQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceSoqlQueryReactor.class);

	private static final String SOQL_QUERY = "soqlQuery";

	public SalesforceSoqlQueryReactor() {
		this.keysToGet = new String[] { SOQL_QUERY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String soqlQuery = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = SalesforceUtils.getSalesforceAccessToken(user);
			String instanceUrl = SalesforceUtils.getSalesforceInstanceUrl(user);
			Map<String, Object> result = SalesforceHelper.runSoqlQuery(accessToken, instanceUrl, soqlQuery);

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (Exception e) {
			classLogger.error("Error running SOQL query on Salesforce", e);
			throw new SemossPixelException("Unable to execute SOQL query: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Executes a SOQL query against Salesforce and returns the query result payload.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SOQL_QUERY.equals(key)) {
			return "Required SOQL query string to execute (for example, SELECT Id, Name FROM Account LIMIT 10).";
		}
		return super.getDescriptionForKey(key);
	}

}
