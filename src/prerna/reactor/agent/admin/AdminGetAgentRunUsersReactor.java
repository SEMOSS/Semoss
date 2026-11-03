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
package prerna.reactor.agent.admin;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the users of the rooms on the admin Agent Runs page, most recently
 * active first, each with their login type, name and email, for the page's user
 * filter. Admins only.
 */
public class AdminGetAgentRunUsersReactor extends AbstractReactor {

	private static final long DEFAULT_LIMIT = 25L;
	private static final long MAX_LIMIT = 100L;

	public AdminGetAgentRunUsersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.LIMIT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		AdminAgentRunReadService readService = AdminAgentRunReadService.getInstance(this.insight.getUser());
		organizeKeys();
		String limitValue = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
		long limit = limitValue == null ? DEFAULT_LIMIT : Long.parseLong(limitValue);
		if (limit <= 0 || limit > MAX_LIMIT) {
			throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT);
		}
		String search = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.SEARCH.getKey()));
		return new NounMetadata(readService.getUserOptions(search, limit), PixelDataType.VECTOR,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Admins only. Lists the users of every room with agent runs, most recently active first, each with their id, login type, name and email.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.SEARCH.getKey().equals(key)) {
			return "Lists only the users whose name, username, email or id contains this text, in any case.";
		} else if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Maximum number to return. Defaults to " + DEFAULT_LIMIT + " and must be between 1 and " + MAX_LIMIT
					+ ".";
		}
		return super.getDescriptionForKey(key);
	}
}
