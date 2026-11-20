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
import prerna.collaboration.WorkTwinPersona;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSaveTwinPersona(sections=[{"voice": "...", "limits": "..."}]);
public class WorkSaveTwinPersonaReactor extends AbstractCollaborationReactor {

	private static final String SECTIONS = "sections";

	public WorkSaveTwinPersonaReactor() {
		this.keysToGet = new String[] { SECTIONS };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Map<String, Object> sections = getMapFromKeyOrCurRow(SECTIONS);
		if (sections == null) {
			throw new IllegalArgumentException("Must pass a sections map");
		}
		return mapResult(WorkTwinPersona.save(user, sections));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Writes sections of the signed-in user's persona. A section that is passed replaces what was "
						+ "there and one left out stays as it is. Their Digital Twins use the result from their next "
						+ "chat, and their own assistant from the next thing it writes for them")
				.key(SECTIONS, key -> key.type(Map.class).description(
						"Plain text by section key: short, voice, decide, review, stands, people, limits, examples. "
								+ "Blank text empties a section; at least one section must hold text"))
				.output(Map.class, "The persona as WorkGetTwinPersona returns it");
	}
}
