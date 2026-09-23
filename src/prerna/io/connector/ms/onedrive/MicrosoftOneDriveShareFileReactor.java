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
package prerna.io.connector.ms.onedrive;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a sharing link to a OneDrive file or folder.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.ReadWrite} for {@code POST /me/drive/items/{id}/createLink},
 * or {@code Files.ReadWrite.All} for an item in a drive somebody else
 * owns.</li>
 * </ul>
 *
 * <p>
 * The scope defaults to {@code organization}, so a link works for people signed
 * in to the same tenant and no further. {@code anonymous} hands out a link that
 * works for anybody holding it, and the tenant's sharing policy may refuse it
 * outright, so it is only ever used when it is asked for by name.
 * </p>
 *
 * <p>
 * Whoever the link is given to sees the file as shared with them, and the
 * reactor that reads it from the other side is {@code MicrosoftOneDriveGetFile}
 * with the {@code shareUrl} key.
 * </p>
 */
public class MicrosoftOneDriveShareFileReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveShareFileReactor.class);

	private static final String LINK_TYPE = "linkType";
	private static final String SCOPE = "scope";
	private static final String RECIPIENTS = "recipients";
	private static final String PASSWORD = "password";
	private static final String EXPIRATION_DATE_TIME = "expirationDateTime";

	public MicrosoftOneDriveShareFileReactor() {
		this.keysToGet = new String[] { DRIVE_ID, ITEM_ID, PATH, LINK_TYPE, SCOPE, RECIPIENTS, PASSWORD,
				EXPIRATION_DATE_TIME };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String driveId = trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = trimToNull(this.keyValue.get(ITEM_ID));
		String path = trimToNull(this.keyValue.get(PATH));
		String linkType = trimToNull(this.keyValue.get(LINK_TYPE));
		String scope = trimToNull(this.keyValue.get(SCOPE));
		String password = trimToNull(this.keyValue.get(PASSWORD));
		String expirationDateTime = trimToNull(this.keyValue.get(EXPIRATION_DATE_TIME));
		String[] recipients = values(RECIPIENTS);

		if (itemId == null && path == null) {
			throw new SemossPixelException("An item id or a path is required to share a OneDrive item.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			Map<String, Object> link = MicrosoftOneDriveHelper.createSharingLink(accessToken, driveId, itemId, path,
					linkType, scope, recipients, password, expirationDateTime);
			return new NounMetadata(link, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while sharing a OneDrive item", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to share a OneDrive item", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to share a OneDrive item", e);
			throw new SemossPixelException("An error occurred sharing the item. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Create a sharing link to a OneDrive file or folder so somebody else can reach it.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ITEM_ID)) {
			return "Id of the item to share, as returned by the OneDrive listings. Not needed when a path is given.";
		} else if (key.equals(PATH)) {
			return "Path of the item to share, relative to the drive root. Not needed when an item id is given.";
		} else if (key.equals(LINK_TYPE)) {
			return "Optional kind of link, one of view, edit or embed. Defaults to view.";
		} else if (key.equals(SCOPE)) {
			return "Optional reach of the link, one of anonymous, organization or users. Defaults to organization, which keeps the link inside the tenant.";
		} else if (key.equals(RECIPIENTS)) {
			return "Email addresses the link is for, required when the scope is users, passed as several values or as one comma separated value.";
		} else if (key.equals(PASSWORD)) {
			return "Optional password the link asks for before it opens the item.";
		} else if (key.equals(EXPIRATION_DATE_TIME)) {
			return "Optional moment the link stops working, as an ISO 8601 date and time such as 2026-12-31T23:59:59Z.";
		}
		return super.getDescriptionForKey(key);
	}
}
