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

import prerna.collaboration.WorkTwinPersona;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkGetTwinPersona();
public class WorkGetTwinPersonaReactor extends AbstractCollaborationReactor {

	public WorkGetTwinPersonaReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		return mapResult(WorkTwinPersona.get(getUser()));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Returns the signed-in user's persona: how they sound, how they decide and what is left to them. "
						+ "Every Digital Twin they created is given it unless they switched it off for that twin, and "
						+ "their own assistant writes for them with it unless they switched that off. Only its owner "
						+ "can read it")
				.noKeys()
				.output(Map.class, "exists, whether any section holds text; sections, the text by section key "
						+ "(short, voice, decide, review, stands, people, limits, examples); offTwins, the ids of "
						+ "the twins it is switched off for; assistant, whether their own assistant writes with it; "
						+ "interview, null or how far the interview that writes it is (answered, of, wrapped, "
						+ "done); and updatedAt");
	}
}
