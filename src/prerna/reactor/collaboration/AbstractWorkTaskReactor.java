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

import java.util.ArrayList;
import java.util.List;

import prerna.collaboration.CollaborationUtils;
import prerna.collaboration.WorkTaskUtils;
import prerna.engine.impl.model.Room;
import prerna.sablecc2.om.GenRowStruct;

// shared by the work item reactors, which are also the assistant's chat tools
public abstract class AbstractWorkTaskReactor extends AbstractCollaborationReactor {

	// a call from a chat is the assistant's, one from a page the owner's
	protected String origin() {
		return this.insight != null && this.insight.getRoomId() != null ? WorkTaskUtils.ASSISTANT : WorkTaskUtils.OWNER;
	}

	// the thread of the chat this runs in, when it is a thread's chat
	protected String chatThreadId() {
		String roomId = this.insight == null ? null : this.insight.getRoomId();
		if (roomId == null || this.insight.getUser() == null) {
			return null;
		}
		Room room = this.insight.getUser().getRoomHash().get(roomId);
		return room == null ? null : CollaborationUtils.threadIdOf(room);
	}

	// each value of a list argument
	protected List<String> strings(String key) {
		List<String> out = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(key);
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				Object v = grs.get(i);
				if (v instanceof List<?> list) {
					list.forEach(x -> out.add(String.valueOf(x)));
				} else if (v != null) {
					out.add(String.valueOf(v));
				}
			}
		}
		out.removeIf(s -> s.isBlank());
		return out;
	}
}
