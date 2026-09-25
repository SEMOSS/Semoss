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
import prerna.collaboration.BrainPeopleUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSavePerson(person=[{"id": "...", "vip": true, "neverIngest": false, "channelScope": {"teams": false}}]);
public class BrainSavePersonReactor extends AbstractCollaborationReactor {

	private static final String KEY = "person";

	public BrainSavePersonReactor() {
		this.keysToGet = new String[] { KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Map<String, Object> changes = getMapFromKeyOrCurRow(KEY);
		if (changes == null) {
			throw new IllegalArgumentException("Must pass a person map");
		}
		return mapResult(BrainPeopleUtils.savePerson(user, changes));
	}

	@Override
	public String getReactorDescription() {
		return "Edits a Brain person; never ingest and channel scope save as rules the gate enforces";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (KEY.equals(key)) {
			return "Partial person with id: relationship, vip, accountId, neverIngest, channelScope {email, teams}";
		}
		return super.getDescriptionForKey(key);
	}
}
