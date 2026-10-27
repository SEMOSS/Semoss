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

import java.io.IOException;

import prerna.collaboration.WorkRoutineDocuments;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkRoutineDocuments(routineId=["..."]);
public class WorkRoutineDocumentsReactor extends AbstractCollaborationReactor {
	private static final String ROUTINE_ID = "routineId";

	public WorkRoutineDocumentsReactor() {
		this.keysToGet = new String[] { ROUTINE_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		try {
			return mapResult(WorkRoutineDocuments.open(getUser(), getString(ROUTINE_ID)));
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not open the routine's documents: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Opens the reference documents folder of one of the signed-in user's routines in their user assets, "
				+ "creating it when missing: its path for the user assets file reactors, and the documents in it. "
				+ "Every run of the routine gets a copy of these files.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ROUTINE_ID.equals(key)) {
			return "Id of the routine.";
		}
		return super.getDescriptionForKey(key);
	}
}
