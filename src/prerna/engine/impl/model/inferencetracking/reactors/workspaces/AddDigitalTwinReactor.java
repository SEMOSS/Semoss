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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.List;
import java.util.Map;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates the calling user's Digital Twin: an agent, made the way
 * {@link AddWorkspaceReactor} makes one, that answers from copies of emails,
 * chats and files its owner adds to its files. It is marked
 * ({@link SecurityProjectUtils#DIGITAL_TWIN_METAKEY}) so it can only ever be
 * shared with named people. A user has one.
 *
 * <pre>
 * AddDigitalTwin(name, description, systemPrompt, mcp, prompts, skills, useDefaultAgentTools)
 * </pre>
 */
public class AddDigitalTwinReactor extends AddWorkspaceReactor {

	@Override
	public NounMetadata execute() {
		User user = requireUser();
		// shortcut: two creates sent at once can both pass this check and leave two
		// twins, each still shared with named people only; lock per user if that
		// ever happens
		boolean hasTwin = !SecurityProjectUtils
				.getUserProjectList(user, List.of(IProject.PROJECT_TYPE.WORKSPACE.name()), null, false,
						Map.of(SecurityProjectUtils.DIGITAL_TWIN_METAKEY, List.of("true")), null, null, null, null,
						null, false, List.of(AccessPermissionEnum.OWNER.getId()), User.getUserIdAndType(user))
				.isEmpty();
		if (hasTwin) {
			return getError("You already have a Digital Twin");
		}

		NounMetadata created = super.execute();
		if (created.getNounType() != PixelDataType.ERROR) {
			// shortcut: if this write fails the new agent stays, as an ordinary private
			// agent; delete it here if that ever happens
			SecurityProjectUtils.markDigitalTwin((String) created.getValue());
		}
		return created;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Create the calling user's Digital Twin: an agent that answers from copies of emails, "
						+ "chats and files its owner adds to its files, and that can only be shared with named "
						+ "people. A user has one.")
				.key(NAME,
						"The twin's name. It starts with a letter and holds letters, numbers, spaces, hyphens "
								+ "and underscores.")
				.key(DESCRIPTION, "What the twin is for")
				.key(SYSTEM_PROMPT, "The twin's instructions")
				.key(ReactorKeysEnum.MCP,
						key -> key.type(List.class)
								.description("The toolboxes the twin can use, each a map with its id and type"))
				.key(PROMPTS, key -> key.list(String.class).description("Ids of the prompts attached to the twin"))
				.key(SKILLS, key -> key.list(String.class).description("Ids of the skills attached to the twin"))
				.key(USE_DEFAULT_AGENT_TOOLS, key -> key.type(Boolean.class).description(
						"Whether the twin has the general built-in tools, which it reads its own files with. "
								+ "Defaults to true."))
				.output(String.class, "The new Digital Twin's id");
	}

}
