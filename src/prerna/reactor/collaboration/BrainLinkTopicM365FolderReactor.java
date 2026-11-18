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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.collaboration.BrainTopicLinks;
import prerna.collaboration.BrainTopicLinks.FolderPointer;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveItemMapper;
import prerna.io.connector.ms.teams.MicrosoftTeamsHelper;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

// BrainLinkTopicM365Folder(topicId=["..."], kind=["drive_folder"], driveId=["..."], itemId=["..."]);
public class BrainLinkTopicM365FolderReactor extends AbstractCollaborationReactor {

	private static final Logger classLogger = LogManager.getLogger(BrainLinkTopicM365FolderReactor.class);

	private static final String TOPIC_ID = "topicId";
	private static final String KIND = "kind";
	private static final String DRIVE_ID = "driveId";
	private static final String ITEM_ID = "itemId";
	private static final String TEAM_ID = "teamId";
	private static final String CHANNEL_ID = "channelId";
	private static final String LINK_ID = "linkId";
	private static final String REMOVE = "remove";

	public BrainLinkTopicM365FolderReactor() {
		this.keysToGet = new String[] { TOPIC_ID, KIND, DRIVE_ID, ITEM_ID, TEAM_ID, CHANNEL_ID, LINK_ID, REMOVE };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String topicId = getStringFromKeyOrCurRow(TOPIC_ID);
		if (Boolean.TRUE.equals(getBoolean(REMOVE))) {
			return mapResult(BrainTopicLinks.unlink(user, topicId, getString(LINK_ID)));
		}

		String kind = ValueUtils.trimToNull(getString(KIND));
		if (kind == null) {
			throw new IllegalArgumentException("A folder kind is required");
		}
		try {
			String accessToken = null;
			String fixturePath = MicrosoftOneDriveHelper.fixturePath();
			if (fixturePath == null) {
				accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			}
			FolderPointer folder = resolveFolder(accessToken, kind);
			return mapResult(BrainTopicLinks.link(user, topicId, folder));
		} catch (SemossPixelException e) {
			classLogger.error("Could not get a Microsoft token to link a topic folder", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input while linking a Microsoft folder to a topic", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Could not verify a Microsoft folder for a topic link", e);
			throw new SemossPixelException("Could not verify folder access in Microsoft 365: " + e.getMessage());
		}
	}

	private FolderPointer resolveFolder(String accessToken, String kind) throws Exception {
		if (BrainTopicLinks.DRIVE_FOLDER.equals(kind)) {
			String driveId = ValueUtils.trimToNull(getString(DRIVE_ID));
			String itemId = ValueUtils.trimToNull(getString(ITEM_ID));
			if (driveId == null || itemId == null) {
				throw new IllegalArgumentException("A drive id and item id are required for a OneDrive folder");
			}
			Map<String, Object> item;
			String fixturePath = MicrosoftOneDriveHelper.fixturePath();
			if (fixturePath != null) {
				item = MicrosoftOneDriveHelper.getItemFixture(driveId, itemId);
			} else {
				item = MicrosoftOneDriveHelper.getItem(accessToken, driveId, itemId, null);
			}
			if (!Boolean.TRUE.equals(item.get("isFolder"))) {
				throw new IllegalArgumentException("The selected OneDrive item is not a folder");
			}
			String resolvedDriveId = ValueUtils.toNonBlankString(item.get("driveId"));
			if (resolvedDriveId == null) {
				resolvedDriveId = driveId;
			}
			String resolvedItemId = MicrosoftOneDriveItemMapper.itemIdOf(item);
			return new FolderPointer(kind, resolvedDriveId, resolvedItemId, null, null, null,
					MicrosoftOneDriveItemMapper.nameOf(item), string(item, "webUrl"));
		}
		if (BrainTopicLinks.CHANNEL_FILES.equals(kind)) {
			String teamId = ValueUtils.trimToNull(getString(TEAM_ID));
			String channelId = ValueUtils.trimToNull(getString(CHANNEL_ID));
			if (teamId == null || channelId == null) {
				throw new IllegalArgumentException("A team id and channel id are required for Teams channel files");
			}
			Map<String, Object> folder;
			String fixturePath = MicrosoftOneDriveHelper.fixturePath();
			if (fixturePath != null) {
				folder = MicrosoftTeamsHelper.getChannelFilesFolderFixture(teamId, channelId);
			} else {
				folder = MicrosoftTeamsHelper.getChannelFilesFolder(accessToken, teamId, channelId);
			}
			return new FolderPointer(kind, string(folder, "driveId"), string(folder, "folderId"), teamId, channelId,
					null, string(folder, "name"), string(folder, "webUrl"));
		}
		if (BrainTopicLinks.SHAREPOINT_FOLDER.equals(kind)) {
			throw new IllegalArgumentException("SharePoint folder linking is not available in this operation");
		}
		throw new IllegalArgumentException("Unsupported folder kind");
	}

	private static String string(Map<String, Object> values, String key) {
		return ValueUtils.toNonBlankString(values.get(key));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Links a OneDrive folder or Teams channel's Files folder to a topic after checking it "
						+ "with the owner's Microsoft account; set remove to true with linkId to unlink it.")
				.key(TOPIC_ID, "Topic id")
				.key(KIND, "Folder kind: drive_folder for OneDrive or channel_files for Teams")
				.key(DRIVE_ID, "Microsoft drive id of a OneDrive folder")
				.key(ITEM_ID, "Microsoft item id of a OneDrive folder")
				.key(TEAM_ID, "Microsoft Teams team id that owns the channel")
				.key(CHANNEL_ID, "Microsoft Teams channel id whose Files folder should be linked")
				.key(LINK_ID, "Linked folder id, required when removing a link")
				.key(REMOVE, "true removes the existing link instead of creating one")
				.output(Map.class, "The topic id and its linked OneDrive and Teams folders");
	}
}
