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

// WorkListTwinUpdates();
public class WorkListTwinUpdatesReactor extends AbstractCollaborationReactor {

	public WorkListTwinUpdatesReactor() {
		this.keysToGet = new String[] {};
		this.keyRequired = new int[] {};
	}

	@Override
	public NounMetadata execute() {
		return mapResult(WorkTwinUpdates.list(getUser()));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Lists what the last checks found for the signed-in user's own Digital Twins and has not been acted "
						+ "on: copies whose original has changed since, copies whose original is gone, and the older "
						+ "of two copies of one item. Starts a check first when one is due, which the user sets with "
						+ "the twinCheckEveryHours setting")
				.noKeys()
				.output(Map.class, "updates, the newest first, each with id, twinId, kind (email, chat or file), "
						+ "change (changed, gone or duplicate), path (the copy), key (the item it was copied from), "
						+ "copiedAt and, where they apply, driveId, title, count, latestFrom, latestAt, webUrl and "
						+ "newerCopy; and lastCheck, the job of the last check, absent when there was none");
	}
}
