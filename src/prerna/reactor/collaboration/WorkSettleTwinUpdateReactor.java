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

import prerna.collaboration.WorkTwinUpdates;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSettleTwinUpdate(updateId=["<id>"], action=["applied"]);
public class WorkSettleTwinUpdateReactor extends AbstractCollaborationReactor {

	private static final String UPDATE_ID = "updateId";
	private static final String ACTION = "action";

	public WorkSettleTwinUpdateReactor() {
		this.keysToGet = new String[] { UPDATE_ID, ACTION };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(Map.of("settled", WorkTwinUpdates.settle(getUser(), getString(UPDATE_ID), getString(ACTION))));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Records what the signed-in user did about one suggestion WorkListTwinUpdates listed. It changes no "
						+ "file: the user's page updates or removes the copy itself and then calls this")
				.key(UPDATE_ID, "The suggestion's id")
				.key(ACTION, "applied once the item was saved again over its copy, which is then current as of now; "
						+ "dismissed to keep the copy as it is until the original changes again, and once a copy "
						+ "was removed")
				.output(Map.class, "settled, whether a suggestion of theirs was changed");
	}
}
