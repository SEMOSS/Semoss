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
package prerna.io.connector.google.docs;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsReadReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleDocsReadReactor.class);

	public GoogleDocsReadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		if (id == null || id.trim().isEmpty()) {
			throw new SemossPixelException("Document ID is required.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			Map<String, Object> result = GoogleDocsHelper.readDoc(accessToken, id);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while reading a Google Docs document", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to read a Google Docs document", e);
			throw new SemossPixelException("An error occurred reading the document. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Read a Google Docs document by ID.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Unique identifier of the Google Docs document to read (" + ReactorKeysEnum.ID.getKey() + ").";
		}
		return super.getDescriptionForKey(key);
	}

}
