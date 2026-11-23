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

import prerna.collaboration.WorkMainTwin;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSaveMainTwin(name=["Robin's Digital Twin"], image=["<base64 picture>"]);
public class WorkSaveMainTwinReactor extends AbstractCollaborationReactor {

	private static final String NAME = "name";
	private static final String IMAGE = "image";

	public WorkSaveMainTwinReactor() {
		this.keysToGet = new String[] { NAME, IMAGE };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		// the twin is the signed-in person's own, so there is nothing else to check. A key left out reads as
		// null and stays as it is; one passed empty reads as "" and is cleared
		return mapResult(WorkMainTwin.save(getUser(), getString(NAME), getString(IMAGE)));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Changes the name, the picture or both of the signed-in user's own Digital Twin: the Collaboration "
						+ "Agent as they see it in their own chats. Pass at least one key; a key left out stays as it "
						+ "is. The Digital Twins they share with other people are not affected")
				.key(NAME, "What to call it, as one line of up to 80 characters. Empty goes back to the default, "
						+ "which is made from the user's first name")
				.key(IMAGE, "The picture as base64 with no data: prefix, in any common image format, up to 1,400,000 "
						+ "characters (about 1 MB of picture; an option from WorkGenerateTwinAvatar fits). It is kept "
						+ "as a PNG of at most 192 pixels. Empty removes the picture")
				.output(Map.class, "name as it now is, and image (a base64 PNG) when a picture is set");
	}
}
