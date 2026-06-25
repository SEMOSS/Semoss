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
package prerna.reactor.platformprofile;

import java.util.Map;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a new platform profile with the given name and optional description.
 *
 * <p>Pixel: {@code CreatePlatformProfile(name=["My Profile"], description=["Optional description"]);}</p>
 */
public class CreatePlatformProfileReactor extends AbstractReactor {

	public CreatePlatformProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.DESCRIPTION.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!PlatformProfileUtils.canManage(user)) {
			throw new IllegalArgumentException("User must be an admin to manage platform profiles.");
		}
		String name = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String description = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());
		Map<String, Object> result = PlatformProfileUtils.createProfile(name, description, user);
		NounMetadata noun = new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Platform profile created."));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Create a platform profile.";
	}
}
