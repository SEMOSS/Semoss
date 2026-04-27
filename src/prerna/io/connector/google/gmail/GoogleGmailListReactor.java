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
package prerna.io.connector.google.gmail;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGmailListReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailListReactor.class);

	public GoogleGmailListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String limitStr = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			int limit = Integer.parseInt(limitStr);
			if (limit <= 0) {
				throw new SemossPixelException("The limit must be a positive integer.");
			}
			List<Map<String, Object>> result = GoogleGmailHelper.getEmailList(accessToken, limit);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid Gmail list limit '{}'", limitStr, e);
			throw new SemossPixelException("The limit must be a positive integer.");
		} catch (SemossPixelException e) {
			classLogger.error("Error while listing Gmail emails", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to list Gmail emails", e);
			throw new SemossPixelException("An error occurred getting the emails. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve a list of emails.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum number of emails to return.";
		}
		return super.getDescriptionForKey(key);
	}
}
