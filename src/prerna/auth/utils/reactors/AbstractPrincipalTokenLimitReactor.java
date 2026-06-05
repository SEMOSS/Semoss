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
 * 	MERMERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils.reactors;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityPrincipalTokenLimitUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;

abstract class AbstractPrincipalTokenLimitReactor extends AbstractReactor {

	protected static final String USER_ID_KEY = "userId";
	protected static final String GROUP_ID_KEY = "groupId";
	protected static final String GROUP_TYPE_KEY = "groupType";
	protected static final String ENGINE_ID_KEY = "engineId";
	protected static final String PROJECT_ID_KEY = "projectId";
	protected static final String SCOPED_ENGINE_ID_KEY = "scopedEngineId";
	protected static final String USAGE_FREQUENCY_KEY = "usageFrequency";
	protected static final String EXISTING_USAGE_FREQUENCY_KEY = "existingUsageFrequency";
	protected static final String MAX_TOKENS_KEY = "maxTokens";
	protected static final String MAX_INPUT_TOKENS_KEY = "maxInputTokens";
	protected static final String MAX_OUTPUT_TOKENS_KEY = "maxOutputTokens";
	protected static final String MAX_RESPONSE_TIME_KEY = "maxResponseTime";
	protected static final String RESTRICT_PER_MODEL_KEY = "restrictPerModel";
	protected static final String IS_ACTIVE_KEY = "isActive";

	protected String value(String key) {
		String value = this.keyValue.get(key);
		return value == null ? null : value.trim();
	}

	protected long longValue(String key, long defaultValue) {
		String value = value(key);
		if (value == null || value.isEmpty()) {
			return defaultValue;
		}
		try {
			return Long.parseLong(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be a valid integer value");
		}
	}

	protected Double doubleValue(String key, Double defaultValue) {
		String value = value(key);
		if (value == null || value.isEmpty()) {
			return defaultValue;
		}
		try {
			return Double.valueOf(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(key + " must be a valid number");
		}
	}

	protected boolean booleanValue(String key, boolean defaultValue) {
		String value = value(key);
		if (value == null || value.isEmpty()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(value);
	}

	protected Pair<String, String> currentUserDetails(User user) {
		return User.getPrimaryUserIdAndTypePair(user);
	}

	protected void requireCanEditEngine(User user, String engineId) {
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId) && !SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this engine's token limits.");
		}
	}

	protected void requireCanEditProject(User user, String projectId) {
		if (!SecurityProjectUtils.userCanEditProject(user, projectId) && !SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this project's token limits.");
		}
	}

	protected void validateFrequency(String usageFrequency) {
		SecurityPrincipalTokenLimitUtils.validateFrequency(usageFrequency);
	}
}
