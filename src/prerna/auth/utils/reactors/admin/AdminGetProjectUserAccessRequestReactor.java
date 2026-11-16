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
package prerna.auth.utils.reactors.admin;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Lists the access requests waiting for a decision on a project for an admin,
 * whatever permission the admin holds on the project. This is the admin
 * counterpart to
 * {@link prerna.reactor.project.GetProjectUserAccessRequestReactor}, which
 * lists them for a user who can edit the project.
 */
public class AdminGetProjectUserAccessRequestReactor extends AbstractReactor {

	/**
	 * Creates the reactor with the project key.
	 */
	public AdminGetProjectUserAccessRequestReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
	}

	/**
	 * Reads the project's access requests that are waiting for a decision, once
	 * the user is known to be an admin.
	 *
	 * @return one map for each request
	 */
	@Override
	public NounMetadata execute() {
		User user = requireUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (projectId == null) {
			throw new IllegalArgumentException("Please define the project id.");
		}

		List<Map<String, Object>> requests = SecurityProjectUtils.getUserAccessRequestsByProject(projectId);
		return new NounMetadata(requests, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

	/**
	 * Describes the reactor, its project key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Admin Only: Returns the access requests waiting for a decision on a project regardless of the \
				caller's project permissions. The calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to GetProjectUserAccessRequest, which returns them to a user who can \
				edit the project.

				Inputs:
				  project (required) - the project id.

				Returns a list (PROJECT_INFO/CUSTOM_DATA_STRUCTURE) with one map for each request: its id, the \
				person who asked (id, login type, name, email, username), when they asked, the project id, the \
				permission asked for, and the approver fields, which are empty until a decision is made.
				""").key(ReactorKeysEnum.PROJECT, "Id of the project to list access requests for").outputList(
				Map.class, "The access requests waiting for a decision on the project");
	}

}
