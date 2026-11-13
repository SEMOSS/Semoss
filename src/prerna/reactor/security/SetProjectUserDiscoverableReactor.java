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
package prerna.reactor.security;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

/**
 * Let agents find and use a project for the signed in user, or stop them. This
 * is the user's own preference, saved for each of their logins, and is off
 * until the user turns it on.
 */
public class SetProjectUserDiscoverableReactor extends AbstractReactor {

	private static final String DISCOVERABLE = "discoverable";

	public SetProjectUserDiscoverableReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), DISCOVERABLE };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		boolean discoverable = parseDiscoverable(this.keyValue.get(DISCOVERABLE));
		try {
			SecurityProjectUtils.setProjectUserDiscoverable(this.insight.getUser(), projectId, discoverable);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e.getMessage());
		} catch (Exception e) {
			throw new IllegalArgumentException("Could not save whether the project is discoverable to agents", e);
		}
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(discoverable
				? "The project is now discoverable to your agents"
				: "The project is no longer discoverable to your agents"));
		return noun;
	}

	/**
	 * Read the discoverable input, which must be true or false.
	 *
	 * @param value the input
	 * @return the value
	 * @throws IllegalArgumentException when the input is not true or false
	 */
	static boolean parseDiscoverable(String value) {
		if ("true".equalsIgnoreCase(value)) {
			return true;
		}
		if ("false".equalsIgnoreCase(value)) {
			return false;
		}
		throw new IllegalArgumentException("discoverable must be true or false");
	}

	@Override
	public String getReactorDescription() {
		return "Set whether a project is discoverable to the user's agents. Projects are not discoverable until the user turns this on";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DISCOVERABLE)) {
			return "true to let the user's agents find and use the project, false to stop them";
		}
		return super.getDescriptionForKey(key);
	}

}
