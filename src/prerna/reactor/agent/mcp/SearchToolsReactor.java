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
package prerna.reactor.agent.mcp;

import java.util.Collections;
import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SearchToolsReactor extends AbstractReactor {

	public SearchToolsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.ENGINE_TYPE.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String query = this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey());
		if (query == null || query.trim().isEmpty()) {
			throw new IllegalArgumentException("Search query is required");
		}

		List<String> engineTypes = getListString(ReactorKeysEnum.ENGINE_TYPE.getKey(), Collections.emptyList());
		String engineTypesStr = this.keyValue.get(ReactorKeysEnum.ENGINE_TYPE.getKey());
		if (engineTypes.isEmpty() && engineTypesStr != null && !engineTypesStr.trim().isEmpty()) {
			engineTypes = List.of(engineTypesStr.split(",")).stream().map(String::trim).filter(s -> !s.isEmpty())
					.toList();
		}
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 10);
		int offset = getInt(ReactorKeysEnum.OFFSET.getKey(), 0);
		Object response = new MCPToolDiscoveryService().search(user, query, engineTypes, limit, offset);
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Search MCP tools across engines and projects/apps the user can access.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.QUERY_KEY.getKey())) {
			return "The search query string to find relevant MCP tools.";
		} else if (key.equals(ReactorKeysEnum.ENGINE_TYPE.getKey())) {
			return "Optional engine type filters such as DATABASE, STORAGE, VECTOR, FUNCTION, MODEL, or PROJECT.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Maximum number of search results to return.";
		} else if (key.equals(ReactorKeysEnum.OFFSET.getKey())) {
			return "Number of search results to skip for pagination.";
		}
		return super.getDescriptionForKey(key);
	}
}
