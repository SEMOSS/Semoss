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

import java.util.Map;

import prerna.collaboration.WorkTwinPersona;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSetAssistantPersona(enabled=[false]);
public class WorkSetAssistantPersonaReactor extends AbstractCollaborationReactor {

	private static final String ENABLED = "enabled";

	public WorkSetAssistantPersonaReactor() {
		this.keysToGet = new String[] { ENABLED };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		Boolean enabled = getBoolean(ENABLED);
		if (enabled == null) {
			throw new IllegalArgumentException("Pass enabled as true or false");
		}
		// the persona is the signed-in person's own, so there is nothing else to check
		return mapResult(WorkTwinPersona.setAssistant(getUser(), enabled));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Switches the signed-in user's persona on or off for work done as them in Collaboration. While it "
						+ "is on, their own Digital Twin (the Collaboration Agent in their chats) thinks with it, and "
						+ "any agent that writes email or Teams messages for them, a routine included, writes with "
						+ "it through GetWritingStyle. The Digital Twins they share are not affected")
				.key(ENABLED, key -> key.type(Boolean.class).description(
						"true for the persona to be used when thinking and writing for them, false for neither"))
				.output(Map.class, "enabled as it now is");
	}
}
