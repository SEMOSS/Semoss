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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.auth.utils.reactors;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityPrincipalTokenLimitUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveProjectUserTokenLimitReactor extends AbstractPrincipalTokenLimitReactor {

	public RemoveProjectUserTokenLimitReactor() {
		this.keysToGet = new String[] { PROJECT_ID_KEY, USER_ID_KEY, SCOPED_ENGINE_ID_KEY, USAGE_FREQUENCY_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = value(PROJECT_ID_KEY);
		String usageFrequency = value(USAGE_FREQUENCY_KEY);
		validateFrequency(usageFrequency);
		requireCanEditProject(user, projectId);
		SecurityPrincipalTokenLimitUtils.removeProjectUserTokenLimit(value(USER_ID_KEY), projectId,
				value(SCOPED_ENGINE_ID_KEY), usageFrequency);
		Map<String, Object> ret = new HashMap<>();
		ret.put("success", true);
		ret.put("projectId", projectId);
		ret.put("userId", value(USER_ID_KEY));
		ret.put("scopedEngineId", value(SCOPED_ENGINE_ID_KEY));
		ret.put("usageFrequency", usageFrequency.toUpperCase());
		return new NounMetadata(ret, PixelDataType.MAP);
	}
}
