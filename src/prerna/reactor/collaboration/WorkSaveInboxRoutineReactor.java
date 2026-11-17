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

import prerna.collaboration.WorkInboxRoutine;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.Map;

public class WorkSaveInboxRoutineReactor extends AbstractCollaborationReactor {
	public WorkSaveInboxRoutineReactor() {
		this.keysToGet = new String[] { "settings" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		return mapResult(WorkInboxRoutine.save(this.insight.getUser(), getMapFromKeyOrCurRow("settings")));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Opts the signed-in user into an inbox brief or pauses it")
				.key("settings", key -> key.type(Map.class).description(
						"The inbox brief settings, as a map: enabled (true or false), time (HH:mm, 24-hour), days (one or more distinct weekdays, MON to SUN), timezone (an IANA time zone), lookbackHours (a whole number from 1 to 720) and preferences (optional free text). Optional sections (any of priorities, replies, meetings, actions; all by default), priorityCount (1 to 10, default 5) and writeDrafts (true or false, default true) shape the brief."));
	}
}
