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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.WorkTwinPersonaInterview;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkTwinPersonaInterview(sources=["mail", "teams", "memory"]);   starts it
// WorkTwinPersonaInterview();                                       where it is
// WorkTwinPersonaInterview(answer=[{"stepId": "dials", ...}]);      answers the step that is waiting
// takes no model on purpose: the text model is the one an admin set in RDF_Map
public class WorkTwinPersonaInterviewReactor extends AbstractCollaborationReactor {

	private static final String ANSWER = "answer";
	private static final String WRAP = "wrap";
	private static final String SOURCES = "sources";

	public WorkTwinPersonaInterviewReactor() {
		this.keysToGet = new String[] { ANSWER, WRAP, SOURCES };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		List<String> sources = getNounAsStringList(SOURCES);
		// shortcut: a step a model writes is one ordinary request, so one slower than the load balancer allows fails
		// and the answer is sent again; run those through the persona job, as reading and writing are, if that shows up
		return mapResult(
				WorkTwinPersonaInterview.step(user, getMapFromKeyOrCurRow(ANSWER), getBoolean(WRAP), sources));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"One call of the interview that writes the signed-in user's persona for their Digital Twins. With "
						+ "sources it starts the interview over; with answer it takes the answer to the step that "
						+ "is waiting and returns the next; with neither it returns where the interview is. Nothing "
						+ "changes the persona their twins use: the result is returned for review and saved with "
						+ "WorkSaveTwinPersona")
				.key(ANSWER, key -> key.type(Map.class).description(
						"The answer to the waiting step, with its stepId and what that kind of step takes; "
								+ "{stepId, skipped: true} skips it; {stepId: \"mirror\", retry, flagged, note} asks "
								+ "for one answer to be written again"))
				.key(WRAP, key -> key.type(Boolean.class).description(
						"true to go straight to the closing steps; false to go on with an interview that was "
								+ "wrapped up"))
				.key(SOURCES, key -> key.list(String.class).description(
						"Starts the interview over, first reading how the user writes from these places of their "
								+ "own: mail, teams, memory; or none to read nothing"))
				.output(Map.class, "phase: none, reading or writing with the job to poll with BrainGetJob, asking "
						+ "with the step that waits, or done with brief.sections to review; asking and done also "
						+ "carry progress (answered, of), learned and notes");
	}
}
