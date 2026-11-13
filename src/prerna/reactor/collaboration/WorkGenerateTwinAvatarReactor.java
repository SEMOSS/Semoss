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

import prerna.collaboration.WorkTwinAvatar;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkGenerateTwinAvatar(description=["..."], variant=[4]);
// takes no model on purpose: the image model is the one an admin set in RDF_Map
public class WorkGenerateTwinAvatarReactor extends AbstractCollaborationReactor {

	private static final String DESCRIPTION = "description";
	private static final String VARIANT = "variant";

	public WorkGenerateTwinAvatarReactor() {
		this.keysToGet = new String[] { DESCRIPTION, VARIANT };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(
				WorkTwinAvatar.generate(getUser(), getString(DESCRIPTION), getIntFromKeyOrCurRow(VARIANT)));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Makes one cartoon avatar option for the signed-in user's Digital Twin with the image model set in "
						+ "RDF_Map: from their own Microsoft profile photo, or from the description when one is "
						+ "passed. Nothing is saved; the caller uploads the option it keeps as the twin's image")
				.key(DESCRIPTION, "How the avatar should look, up to 500 characters. Leave out to use the profile photo")
				.key(VARIANT, key -> key.type(Integer.class).description(
						"Which look to draw. Any whole number: it is counted round the looks there are, so "
								+ "three numbers in a row give three different looks. Leave out for any look"))
				.output(Map.class, "image, a base64 PNG; or needsDescription true when the user has no profile "
						+ "photo and passed no description");
	}
}
