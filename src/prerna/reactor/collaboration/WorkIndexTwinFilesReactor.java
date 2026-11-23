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
import prerna.collaboration.WorkTwinIndex;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkIndexTwinFiles(project=["<the twin's id>"]);
// takes no model on purpose: the text model is the one an admin set in RDF_Map
public class WorkIndexTwinFilesReactor extends AbstractCollaborationReactor {

	public WorkIndexTwinFilesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String projectId = getString(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || projectId.isBlank()) {
			throw new IllegalArgumentException("Pass the Digital Twin's id as project");
		}
		projectId = projectId.trim();
		// one answer whether it is missing, not the caller's to change, or not a twin:
		// only someone who can change a twin is told that it is one
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)
				|| !SecurityProjectUtils.isDigitalTwin(projectId)) {
			throw new IllegalArgumentException(
					"Digital Twin " + projectId + " does not exist or is not yours to change");
		}
		return mapResult(WorkTwinIndex.sync(user, projectId));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Brings a Digital Twin's index of what its files hold up to date. A model writes one line about "
						+ "each of up to twelve files in the twin's files folder that are new or have changed, and "
						+ "the lines of files that are gone are dropped. A chat with the twin is then told what each "
						+ "file is about. Only someone who can edit the twin can run it. Call it again while "
						+ "remaining is above zero")
				.key(ReactorKeysEnum.PROJECT, "The Digital Twin's id")
				.output(Map.class, "described, how many files this call described; remaining, how many still wait; "
						+ "and note, why none could be described, when Collaboration has no text model the caller "
						+ "can use");
	}
}
