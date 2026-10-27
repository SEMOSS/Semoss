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

public class BrainBrowseTopicDriveReactor extends AbstractCollaborationReactor {
	public BrainBrowseTopicDriveReactor() {
		this.keysToGet = new String[] { "view", "driveId", "itemId", "teamId", "channelId", "siteId", "search" };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0 };
	}
	@Override
	public NounMetadata execute() {
		return mapResult(BrainTopicLinks.browse(getUser(), getString("view"), getString("driveId"), getString("itemId"),
				getString("teamId"), getString("channelId"), getString("siteId"), getString("search")));
	}
	@Override
	public String getReactorDescription() { return "Lists OneDrive, SharePoint and Teams files and places the signed-in user can pick from to link to a topic."; }
	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "view" -> "mine, shared, teams or sites when nothing more specific is given; default mine";
			case "driveId" -> "Drive to list; with itemId, the folder in it";
			case "itemId" -> "Folder to list in driveId; the drive root when omitted";
			case "teamId" -> "Team whose channels to list; with channelId, the channel's files";
			case "channelId" -> "Channel whose files folder to list";
			case "siteId" -> "SharePoint site whose document libraries to list";
			case "search" -> "Text to find across OneDrive and SharePoint";
			default -> super.getDescriptionForKey(key);
		};
	}
}
