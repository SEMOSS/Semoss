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

import prerna.collaboration.BrainTopicLinks;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrainRefreshTopicLinksReactor extends AbstractCollaborationReactor {
	public BrainRefreshTopicLinksReactor() {
		this.keysToGet = new String[] { "topicId", "roomId" };
		this.keyRequired = new int[] { 0, 0 };
	}
	@Override
	public NounMetadata execute() {
		String topicId = getString("topicId"), roomId = getString("roomId");
		if ((topicId == null) == (roomId == null)) throw new IllegalArgumentException("Pass either topicId or roomId");
		return mapResult(BrainTopicLinks.refresh(getUser(), topicId, roomId));
	}
	@Override
	public String getReactorDescription() { return "Re-reads the name, location, size and last change of every Microsoft 365 file linked to a topic, or to the topics of an owned chat."; }
	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "topicId" -> "Owned topic id";
			case "roomId" -> "Owned collaboration chat id; refreshes the files of its linked topics";
			default -> super.getDescriptionForKey(key);
		};
	}
}
