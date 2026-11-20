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

import prerna.collaboration.BrainResetUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainResetMyData(confirm=["reset"]);
public class BrainResetMyDataReactor extends AbstractCollaborationReactor {

	private static final String CONFIRM = "confirm";

	public BrainResetMyDataReactor() {
		this.keysToGet = new String[] { CONFIRM };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		// the word itself, so a stray call never wipes anything
		if (!"reset".equals(getString(CONFIRM))) {
			throw new IllegalArgumentException("Pass confirm=[\"reset\"] to delete all of your Collaboration data");
		}
		return mapResult(BrainResetUtils.resetMyData(getUser()));
	}

	@Override
	public String getReactorDescription() {
		return "Deletes all of the caller's Collaboration data (profile, people, topics, threads, rules, work items, "
				+ "jobs) so onboarding starts fresh; keeps the Microsoft link";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (CONFIRM.equals(key)) {
			return "Must be \"reset\"";
		}
		return super.getDescriptionForKey(key);
	}
}
