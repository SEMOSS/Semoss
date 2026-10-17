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

import prerna.collaboration.BrainMailFolders;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainLinkTopicFolder(topicId=["..."], folderId=["..."], remove=[false]);
public class BrainLinkTopicFolderReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String FOLDER_ID = "folderId";
	private static final String REMOVE = "remove";

	public BrainLinkTopicFolderReactor() {
		this.keysToGet = new String[] { TOPIC_ID, FOLDER_ID, REMOVE };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(BrainMailFolders.linkTopic(getUser(), getString(TOPIC_ID), getString(FOLDER_ID),
				Boolean.TRUE.equals(getBoolean(REMOVE))));
	}

	@Override
	public String getReactorDescription() {
		return "The owner links one of their Outlook folders to a topic, or removes the link; mail in a linked "
				+ "folder is filed under the topic";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC_ID.equals(key)) {
			return "Topic id";
		} else if (FOLDER_ID.equals(key)) {
			return "Outlook folder id, from BrainListMailFolders";
		} else if (REMOVE.equals(key)) {
			return "true removes the link";
		}
		return super.getDescriptionForKey(key);
	}
}
