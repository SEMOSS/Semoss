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
package prerna.reactor.prompt;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.prompt.PromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a new prompt with metadata, tags, and optional global visibility.
 *
 * Pixel usage: AddPrompt(map=[{"title": "...", "context": "...", ...}]);
 *
 * Map parameters: title (String, required) - Prompt name context (String,
 * required) - The prompt text/template intent (String, optional) - Description
 * of the prompt's purpose global (Boolean, optional, default: false) - Whether
 * visible to all users tags (List of String, optional) - Tags for
 * categorization metaMap (Map of String to Collection of String, optional) -
 * Arbitrary metadata key-value pairs
 *
 * Returns: CONST_STRING - the UUID of the newly created prompt.
 */
public class AddPromptReactor extends AbstractReactor {

	public AddPromptReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		String userId = this.insight.getUserId();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a prompt",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		organizeKeys();
		Map<String, Object> promptDetails = getPromptDetails();
		String promptId = PromptUtils.addPrompt(promptDetails, user, userId);
		NounMetadata nm = new NounMetadata(promptId, PixelDataType.CONST_STRING);
		return nm;
	}

	private Map<String, Object> getPromptDetails() {
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

		throw new NullPointerException("Must define the prompt to store it correctly");
	}
}
