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
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ValueUtils;

/**
 * Creates an empty file in OneDrive.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Files.ReadWrite} for {@code PUT /me/drive/root:/{path}:/content},
 * or {@code Files.ReadWrite.All} to create in a drive somebody else owns and
 * shared with write access.</li>
 * </ul>
 *
 * <p>
 * A name that is already taken fails, and there is no way to ask for anything
 * else: replacing a file with an empty one discards what is in it. The folder
 * has to exist, so create it with {@code MicrosoftOneDriveCreateFolder} first.
 * </p>
 */
public class MicrosoftOneDriveCreateFileReactor extends AbstractMicrosoftOneDriveReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOneDriveCreateFileReactor.class);

	public MicrosoftOneDriveCreateFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), DRIVE_ID, ITEM_ID, PATH };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		String name = ValueUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.NAME.getKey()));
		String driveId = ValueUtils.trimToNull(this.keyValue.get(DRIVE_ID));
		String itemId = ValueUtils.trimToNull(this.keyValue.get(ITEM_ID));
		String path = ValueUtils.trimToNull(this.keyValue.get(PATH));

		if (name == null) {
			throw new SemossPixelException("A file name is required to create a OneDrive file.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			Map<String, Object> file = MicrosoftOneDriveHelper.createFile(accessToken, driveId, itemId, path, name);
			return new NounMetadata(file, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while creating a OneDrive file", e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to create a OneDrive file", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to create a OneDrive file", e);
			throw new SemossPixelException("An error occurred creating the file. Error message: " + e.getMessage());
		}
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Create an empty file in OneDrive, in the signed in user's own drive or in a folder shared with them. Fails when a file of that name is already there.")
				.key(DRIVE_ID, DRIVE_ID_DESCRIPTION)
				.key(ReactorKeysEnum.NAME, "Name of the file to create, with its extension.")
				.key(ITEM_ID, "Optional id of the folder to create inside. The drive root is used when omitted.")
				.key(PATH,
						"Optional path of the folder to create inside, relative to the drive root, or relative to the folder the item id names when both are given. The folder has to exist already.");
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// writes to the user's OneDrive, so an agent asks before running it
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}
}
