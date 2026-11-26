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

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.collaboration.WorkTwinPersona;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSetTwinPersona(project=["<the twin's id>"], enabled=[false]);
public class WorkSetTwinPersonaReactor extends AbstractCollaborationReactor {

	private static final String ENABLED = "enabled";

	public WorkSetTwinPersonaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ENABLED };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String projectId = getString(ReactorKeysEnum.PROJECT.getKey());
		Boolean enabled = getBoolean(ENABLED);
		if (projectId == null || projectId.isBlank() || enabled == null) {
			throw new IllegalArgumentException("Pass the Digital Twin's id as project, and enabled as true or false");
		}
		projectId = projectId.trim();
		// One answer whether it is missing, someone else's, or not a twin. The persona is its creator's own, so it
		// is theirs to switch whatever access they have to the twin now, and nobody else's.
		if (!SecurityProjectUtils.isProjectCreator(user, projectId) || !SecurityProjectUtils.isDigitalTwin(projectId)) {
			throw new IllegalArgumentException("Digital Twin " + projectId + " does not exist or is not yours");
		}
		return mapResult(WorkTwinPersona.setTwin(user, projectId, enabled));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Switches the signed-in user's persona on or off for one Digital Twin they created. A twin uses its "
						+ "creator's persona until it is switched off")
				.key(ReactorKeysEnum.PROJECT, "The Digital Twin's id")
				.key(ENABLED, key -> key.type(Boolean.class)
						.description("true for the twin to use the persona, false for it to answer without it"))
				.output(Map.class, "project, the twin's id, and enabled as it now is");
	}
}
