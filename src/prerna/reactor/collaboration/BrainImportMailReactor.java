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

import prerna.collaboration.BrainMailImport;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainImportMail(); or BrainImportMail(days=[30]);
public class BrainImportMailReactor extends AbstractCollaborationReactor {

	private static final String DAYS = "days";

	public BrainImportMailReactor() {
		this.keysToGet = new String[] { DAYS };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		Integer days = getIntFromKeyOrCurRow(DAYS);
		return mapResult(BrainMailImport.start(getUser(), days == null ? 30 : days));
	}

	@Override
	public String getReactorDescription() {
		return "Starts a background import of Inbox and Sent headers (people, threads, participants) through the rules "
				+ "gate; returns the job to poll with BrainGetJob. Re-runnable (Refresh)";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (DAYS.equals(key)) {
			return "How many days back to import (default 30, at most 180)";
		}
		return super.getDescriptionForKey(key);
	}
}
