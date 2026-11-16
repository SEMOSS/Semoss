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
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainLinkTopicFile(topicId=["..."], driveId=["..."], itemId=["..."], source=["onedrive"], remove=[false]);
public class BrainLinkTopicFileReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String DRIVE_ID = "driveId";
	private static final String ITEM_ID = "itemId";
	private static final String SOURCE = "source";
	private static final String REMOVE = "remove";

	public BrainLinkTopicFileReactor() {
		this.keysToGet = new String[] { TOPIC_ID, DRIVE_ID, ITEM_ID, SOURCE, REMOVE };
		this.keyRequired = new int[] { 1, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(BrainTopicFiles.link(getUser(), getString(TOPIC_ID), getString(DRIVE_ID), getString(ITEM_ID),
				getString(SOURCE), Boolean.TRUE.equals(getBoolean(REMOVE))));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"The owner links a OneDrive or Teams file to a topic, or removes the link; the topic lists the file "
						+ "and its assistant is told of it. Returns the topic's files")
				.key(TOPIC_ID, "Topic id")
				.key(DRIVE_ID, "Id of the drive holding the file, as the OneDrive and Teams listings return it; the "
						+ "owner's own OneDrive when omitted")
				.key(ITEM_ID, "Id of the file in that drive, as the OneDrive and Teams listings return it")
				.key(SOURCE, "The app the file was picked in: onedrive (the default) or teams")
				.key(REMOVE, "true removes the link");
	}
}
