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

import prerna.collaboration.BrainTopicFiles;
import prerna.collaboration.BrainTopicLinks;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrainCopyTopicFileToRoomReactor extends AbstractCollaborationReactor {
	public BrainCopyTopicFileToRoomReactor() {
		this.keysToGet = new String[] { "topicId", "fileId", "linkId", "roomId" };
		this.keyRequired = new int[] { 1, 0, 0, 1 };
	}
	@Override
	public NounMetadata execute() {
		getUser();
		String fileId = getString("fileId"), linkId = getString("linkId");
		if ((fileId == null) == (linkId == null)) throw new IllegalArgumentException("Pass either fileId or linkId");
		return mapResult(linkId != null
				? BrainTopicLinks.copyToRoom(insight, getString("topicId"), linkId, getString("roomId"))
				: BrainTopicFiles.copyToRoom(insight, getString("topicId"), fileId, getString("roomId")));
	}
	@Override
	public String getReactorDescription() { return "Copies an uploaded or linked file from a linked topic into an owned collaboration room for use in the next message."; }
	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "topicId" -> "Topic id from ListTopics or ListTopicFiles";
			case "fileId" -> "Uploaded file id from ListTopicFiles";
			case "linkId" -> "Linked Microsoft 365 file id from ListTopicFiles";
			case "roomId" -> "Owned collaboration chat id; file access is restricted to its linked topics";
			default -> super.getDescriptionForKey(key);
		};
	}
}
