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

import prerna.collaboration.WorkInboxRoutine;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkRunInboxRoutine(); is the scheduler's recipe and keeps to the brief's days; now=[true] starts a run off schedule on the
// scheduler, which runs WorkRunInboxRoutine(manual=[true]); on any day
public class WorkRunInboxRoutineReactor extends AbstractCollaborationReactor {
	private static final String NOW = "now";
	private static final String MANUAL = "manual";

	public WorkRunInboxRoutineReactor() {
		this.keysToGet = new String[] { NOW, MANUAL };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		if (Boolean.TRUE.equals(getBoolean(NOW))) {
			WorkInboxRoutine.runNow(this.insight.getUser());
			return mapResult(Map.of("status", "started"));
		}
		if (Boolean.TRUE.equals(getBoolean(MANUAL))) {
			return mapResult(WorkInboxRoutine.runManual(this.insight));
		}
		return mapResult(WorkInboxRoutine.run(this.insight));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Runs the opted-in inbox brief once per selected day for the signed-in user, or now on the scheduler with now=true")
				.key(NOW, "Start a run now, off the schedule")
				.key(MANUAL, "Set by the scheduler on the run that now=true started");
	}
}
