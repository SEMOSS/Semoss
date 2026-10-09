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
package prerna.reactor.collaboration;

import prerna.auth.User;
import prerna.collaboration.BrainThreadRoomUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainListRoomThreads(roomId=["..."], withIds=[false]);
public class BrainListRoomThreadsReactor extends AbstractCollaborationReactor {

	private static final String ROOM_ID = "roomId";
	private static final String WITH_IDS = "withIds";

	public BrainListRoomThreadsReactor() {
		this.keysToGet = new String[] { ROOM_ID, WITH_IDS };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		return mapResult(BrainThreadRoomUtils.listRoomThreads(user, getString(ROOM_ID),
				Boolean.TRUE.equals(getBoolean(WITH_IDS))));
	}

	@Override
	public String getReactorDescription() {
		return "Lists the email threads a chat includes, most recent mail first, with how many of each thread's "
				+ "emails are new to the chat's assistant";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ROOM_ID.equals(key)) {
			return "The chat's room id";
		} else if (WITH_IDS.equals(key)) {
			return "true adds the ids of each thread's emails that the chat shows; defaults to false";
		}
		return super.getDescriptionForKey(key);
	}
}
