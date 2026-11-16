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
package prerna.reactor.engine;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the signed in user's own access request on an engine that waits for a
 * decision, so the engine's page can show what they asked for, and they can change
 * or withdraw it. It reads only the user's own request, so anyone signed in may
 * run it on any engine.
 */
public class GetMyEngineAccessRequestReactor extends AbstractReactor {

	/**
	 * Creates the reactor with the engine key.
	 */
	public GetMyEngineAccessRequestReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	/**
	 * Reads the user's waiting request on the engine.
	 *
	 * @return the request, or null when the user has none waiting
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = requireUser();
		String id = this.keyValue.get(this.keysToGet[0]);
		if (id == null || id.isBlank()) {
			throw new IllegalArgumentException("Please define the engine id.");
		}
		Map<String, Object> request = SecurityEngineUtils.getUserWaitingAccessRequest(user, id.trim());
		return new NounMetadata(request, request == null ? PixelDataType.NULL_VALUE : PixelDataType.MAP,
				PixelOperationType.ENGINE_INFO);
	}

	/**
	 * Describes the reactor, its engine key and what it returns.
	 *
	 * @return the definition of the reactor
	 */
	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("""
				Returns the signed in user's own access request on an engine that waits for an owner's decision: \
				the level asked for, the reason given and when it was asked, or null when the user has none \
				waiting. Asking again replaces a waiting request, so there is at most one.""")
				.key(ReactorKeysEnum.ENGINE, "The id of the engine")
				.output(Map.class, "The request: id, permission (the level asked for), comment and requestedAt "
						+ "(UTC, ISO 8601), or null");
	}

}
