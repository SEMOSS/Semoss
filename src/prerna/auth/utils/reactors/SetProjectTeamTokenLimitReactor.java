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

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityGroupProjectUtils;
import prerna.auth.utils.SecurityPrincipalTokenLimitUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetProjectTeamTokenLimitReactor extends AbstractPrincipalTokenLimitReactor {

	public SetProjectTeamTokenLimitReactor() {
		this.keysToGet = new String[] { PROJECT_ID_KEY, GROUP_ID_KEY, GROUP_TYPE_KEY, SCOPED_ENGINE_ID_KEY,
				USAGE_FREQUENCY_KEY, EXISTING_USAGE_FREQUENCY_KEY, MAX_TOKENS_KEY, MAX_INPUT_TOKENS_KEY,
				MAX_OUTPUT_TOKENS_KEY, MAX_RESPONSE_TIME_KEY, RESTRICT_PER_MODEL_KEY, IS_ACTIVE_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String projectId = value(PROJECT_ID_KEY);
		String groupId = value(GROUP_ID_KEY);
		String groupType = value(GROUP_TYPE_KEY);
		String usageFrequency = value(USAGE_FREQUENCY_KEY);
		validateFrequency(usageFrequency);
		requireCanEditProject(user, projectId);
		if (SecurityGroupProjectUtils.getGroupProjectPermission(groupId, groupType, projectId) == null) {
			throw new IllegalArgumentException("Team does not currently have access to this project.");
		}
		Pair<String, String> details = currentUserDetails(user);
		SecurityPrincipalTokenLimitUtils.setProjectTeamTokenLimit(groupId, groupType, projectId,
				value(SCOPED_ENGINE_ID_KEY), usageFrequency, value(EXISTING_USAGE_FREQUENCY_KEY),
				longValue(MAX_TOKENS_KEY, -1), longValue(MAX_INPUT_TOKENS_KEY, -1),
				longValue(MAX_OUTPUT_TOKENS_KEY, -1), doubleValue(MAX_RESPONSE_TIME_KEY, null),
				booleanValue(RESTRICT_PER_MODEL_KEY, false), booleanValue(IS_ACTIVE_KEY, true), details.getValue0(),
				details.getValue1());
		Map<String, Object> ret = new HashMap<>();
		ret.put("success", true);
		ret.put("projectId", projectId);
		ret.put("groupId", groupId);
		ret.put("groupType", groupType);
		ret.put("scopedEngineId", value(SCOPED_ENGINE_ID_KEY));
		ret.put("usageFrequency", usageFrequency.toUpperCase());
		return new NounMetadata(ret, PixelDataType.MAP);
	}
}
