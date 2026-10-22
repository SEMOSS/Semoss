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
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrainListTopicFilesReactor extends AbstractCollaborationReactor {
	public BrainListTopicFilesReactor() {
		this.keysToGet = new String[] { "topicId", "roomId", "limit", "offset" };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}
	@Override
	public NounMetadata execute() {
		Integer limit = getIntFromKeyOrCurRow("limit"), offset = getIntFromKeyOrCurRow("offset");
		return mapResult(BrainTopicFiles.list(getUser(), getString("topicId"), getString("roomId"), limit == null ? 50 : limit, offset == null ? 0 : offset));
	}
	@Override
	public String getReactorDescription() { return "Lists original files on an owned topic, or only topics linked to roomId. Returns paginated metadata; use ReadTopicFile to access contents."; }
	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "topicId" -> "Topic id from ListTopics or ListTopicFiles";
			case "roomId" -> "Owned collaboration chat id; file access is restricted to its linked topics";
			case "limit" -> "Page size, 1 to 100; default 50";
			case "offset" -> "Number of rows to skip; default 0";
			default -> super.getDescriptionForKey(key);
		};
	}
}
