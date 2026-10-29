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

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunStatus;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists one page of rooms with agent runs across every user, for the admin
 * Agent Runs page, most recently active first. Each room is a user's
 * conversation with an agent: its user, its agent, how many runs it holds and
 * how many are still going or failed. Admins only.
 */
public class AdminGetAgentRunRoomsReactor extends AbstractReactor {

	private static final String AGENT_ID_KEY = "agentId";
	private static final String USER_ID_KEY = "userId";
	private static final String USER_TYPE_KEY = "userType";
	private static final String STATUS_KEY = "status";
	private static final long DEFAULT_LIMIT = 25L;
	private static final long MAX_LIMIT = 200L;

	public AdminGetAgentRunRoomsReactor() {
		this.keysToGet = new String[] { AGENT_ID_KEY, USER_ID_KEY, USER_TYPE_KEY, ReactorKeysEnum.SEARCH.getKey(),
				STATUS_KEY, ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
	}

	@Override
	public NounMetadata execute() {
		AdminAgentRunReadService readService = AdminAgentRunReadService.getInstance(this.insight.getUser());
		organizeKeys();
		long limit = getPageValue(ReactorKeysEnum.LIMIT.getKey(), DEFAULT_LIMIT);
		long offset = getPageValue(ReactorKeysEnum.OFFSET.getKey(), 0L);
		if (limit <= 0 || limit > MAX_LIMIT) {
			throw new IllegalArgumentException("limit must be between 1 and " + MAX_LIMIT);
		}
		if (offset < 0) {
			throw new IllegalArgumentException("offset must be greater than or equal to 0");
		}

		AgentRunStore.RoomFilter filter = new AgentRunStore.RoomFilter(
				StringUtils.trimToNull(this.keyValue.get(AGENT_ID_KEY)),
				StringUtils.trimToNull(this.keyValue.get(USER_ID_KEY)),
				StringUtils.trimToNull(this.keyValue.get(USER_TYPE_KEY)),
				StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.SEARCH.getKey())), getStatuses());
		return new NounMetadata(readService.getRooms(filter, limit, offset), PixelDataType.MAP,
				PixelOperationType.OPERATION);
	}

	/**
	 * The statuses a listed room must hold a run in, every value of the status key.
	 *
	 * @return the statuses, or none for any status
	 */
	private Set<AgentRunStatus> getStatuses() {
		Set<AgentRunStatus> statuses = EnumSet.noneOf(AgentRunStatus.class);
		GenRowStruct values = this.store.getGenRowStruct(STATUS_KEY);
		if (values == null) {
			return statuses;
		}
		for (String value : values.getAllStrValues()) {
			String name = StringUtils.trimToNull(value);
			if (name == null) {
				continue;
			}
			try {
				statuses.add(AgentRunStatus.valueOf(name.toUpperCase(Locale.ROOT)));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Unknown agent run status: " + name);
			}
		}
		return statuses;
	}

	private long getPageValue(String key, long defaultValue) {
		String value = this.keyValue.get(key);
		return value == null || value.trim().isEmpty() ? defaultValue : Long.parseLong(value.trim());
	}

	@Override
	public String getReactorDescription() {
		return "Admins only. Lists a page of rooms with agent runs across every user, most recently active first, with each room's user and login type, agent, run counts and first and last activity, and how many rooms match in all.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (AGENT_ID_KEY.equals(key)) {
			return "Lists only the rooms holding a run of this agent. This corresponds to AGENT_RUN.WORKSPACE_ID.";
		} else if (USER_ID_KEY.equals(key)) {
			return "Lists only this user's rooms.";
		} else if (USER_TYPE_KEY.equals(key)) {
			return "The login type of the user filtered on, such as NATIVE. Runs that store no login type match the user id alone.";
		} else if (ReactorKeysEnum.SEARCH.getKey().equals(key)) {
			return "Lists only the rooms whose name or id contains this text, in any case.";
		} else if (STATUS_KEY.equals(key)) {
			return "Lists only the rooms holding a run in one of these statuses: SUBMITTED, RUNNING, INPUT_REQUIRED, COMPLETED, FAILED or CANCELLED.";
		} else if (ReactorKeysEnum.LIMIT.getKey().equals(key)) {
			return "Maximum number of rooms to return. Defaults to " + DEFAULT_LIMIT + " and must be between 1 and "
					+ MAX_LIMIT + ".";
		} else if (ReactorKeysEnum.OFFSET.getKey().equals(key)) {
			return "Number of rooms to skip. Defaults to 0 and must not be negative.";
		}
		return super.getDescriptionForKey(key);
	}
}
