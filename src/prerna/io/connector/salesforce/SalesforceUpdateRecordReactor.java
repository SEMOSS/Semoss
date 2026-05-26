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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SalesforceUpdateRecordReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceUpdateRecordReactor.class);

	private static final String SOBJECT_NAME = "sObjectName";
	private static final String RECORD_ID = "recordId";

	public SalesforceUpdateRecordReactor() {
		this.keysToGet = new String[] { SOBJECT_NAME, RECORD_ID, ReactorKeysEnum.MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
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

			Map<String, Object> fieldValues = getInputFieldMap();
			if (fieldValues == null || fieldValues.isEmpty()) {
				classLogger.error("Input MAP (field-values) missing or empty.");
				throw new IllegalArgumentException("Input MAP (field-values) missing or empty.");
			}
			Map<String, Object> result = SalesforceHelper.updateRecord(accessToken, instanceUrl, sObjectName, recordId,
					fieldValues);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Error updating Salesforce record", e);
			throw new SemossPixelException("Error updating Salesforce record: " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInputFieldMap() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.MAP.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, Object>) mapNouns.get(0).getValue();
			}
		}

		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, Object>) mapNouns.get(0).getValue();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Updates a Salesforce record using object API name, record Id, and a field/value map.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SOBJECT_NAME.equals(key)) {
			return "Required Salesforce object API name containing the record (for example, Opportunity).";
		} else if (RECORD_ID.equals(key)) {
			return "Required Salesforce record Id to update.";
		} else if (ReactorKeysEnum.MAP.getKey().equals(key)) {
			return "Required map of Salesforce field API names to updated values.";
		}
		return super.getDescriptionForKey(key);
	}

}
