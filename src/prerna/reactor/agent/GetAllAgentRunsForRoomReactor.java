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
package prerna.reactor.agent;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns every run the caller has in one of their rooms, newest first, for the
 * agent's Activity tab: its root runs and the subagent runs it holds, each with
 * the room's name and whether it is closed. Only the caller's runs are read, by
 * the id and login type of their primary login.
 */
public class GetAllAgentRunsForRoomReactor extends AbstractReactor {

	public GetAllAgentRunsForRoomReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String roomId = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey()));
		if (roomId == null) {
			throw new IllegalArgumentException("Room id is required");
		}
		return new NounMetadata(AgentRunStore.getRunsForRoomByLogin(this.insight, roomId), PixelDataType.VECTOR,
				PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Returns every run you have in one of your rooms, newest first, subagent runs included, with the room's name and whether it is closed.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.ROOM_ID.getKey().equals(key)) {
			return "Your room whose runs are returned.";
		}
		return super.getDescriptionForKey(key);
	}
}
