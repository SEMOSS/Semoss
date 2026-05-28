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

public class ServiceNowUpdateRecordReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowUpdateRecordReactor.class);

	private static final String SYS_ID = "sysId";
	private static final String INSTANCE_URL = "instanceURL";

	public ServiceNowUpdateRecordReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TABLE.getKey(), SYS_ID, INSTANCE_URL,
				ReactorKeysEnum.MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		try {
			String table = this.keyValue.get(this.keysToGet[0]);
			String sysId = this.keyValue.get(this.keysToGet[1]);
			String instanceURL = this.keyValue.get(this.keysToGet[2]);

			User user = this.insight.getUser();
			String accessToken = ServiceNowUtils.getServiceNowAccessToken(user);

			Map<String, Object> fieldValues = getInputFieldMap();
			Map<String, Object> updateRecord = ServiceNowHelper.updateRecord(instanceURL, accessToken, table, sysId,
					fieldValues);

			return new NounMetadata(updateRecord, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Error updating ServiceNow record.", e);
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInputFieldMap() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.MAP.getKey());
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
		return "Updates (PATCH) a record in a ServiceNow table via OAuth authentication.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "Required ServiceNow table name where the record will be updated.";
		} else if (key.equals(SYS_ID)) {
			return "Required sys_id of the record to update in the ServiceNow table.";
		} else if (key.equals(INSTANCE_URL)) {
			return "Required URL of the ServiceNow user's instance.";
		} else if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "Required map of field-value pairs representing the fields to update in the record.";
		}
		return super.getDescriptionForKey(key);
	}
}
