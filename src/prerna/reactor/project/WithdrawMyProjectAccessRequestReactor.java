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
package prerna.reactor.project;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Withdraws the signed in user's own access request on a project that waits for a
 * decision. The request is kept, marked withdrawn by the user, and no longer
 * shows to the project's owners. It touches only the user's own request, so anyone
 * signed in may run it on any project.
 */
public class WithdrawMyProjectAccessRequestReactor extends AbstractReactor {

	/**
	 * Creates the reactor with the project key.
	 */
	public WithdrawMyProjectAccessRequestReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	/**
	 * Withdraws the user's waiting request on the project.
	 *
	 * @return true once the request is withdrawn
	 * @throws IllegalArgumentException when the user has no request waiting
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = requireUser();
		String id = this.keyValue.get(this.keysToGet[0]);
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("Please define the project id.");
		}
		if (!SecurityProjectUtils.withdrawUserAccessRequest(user, id.trim())) {
			throw new IllegalArgumentException("You have no access request waiting on this project");
		}
		NounMetadata result = new NounMetadata(true, PixelDataType.BOOLEAN);
		result.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Your access request was withdrawn"));
		return result;
	}

	/**
	 * Describes the reactor, its project key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Withdraws the signed in user's own access request on a project that waits for an owner's \
				decision. The request is kept, marked withdrawn, and no longer shows to the project's owners. \
				Fails when the user has no request waiting.""")
				.key(ReactorKeysEnum.PROJECT, "The id of the project")
				.output(Boolean.class, "True once the request is withdrawn");
	}

}
