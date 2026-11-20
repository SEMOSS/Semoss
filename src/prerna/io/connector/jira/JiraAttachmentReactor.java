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
package prerna.io.connector.jira;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JiraAttachmentReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraAttachmentReactor.class);

	private static final String ACTION = "action";
	private static final String JIRAID = "jiraid";
	private static final String FILE_PATH = "filePath";
	private static final String ATTACHMENT_ID = "attachmentId";

	public JiraAttachmentReactor() {
		this.keysToGet = new String[] { ACTION, JIRAID, FILE_PATH, ATTACHMENT_ID };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String action = this.keyValue.get(ACTION);
			if (action == null || action.trim().isEmpty()) {
				throw new SemossPixelException("The action parameter is required. Valid values are: get, add, delete.");
			}
			action = action.trim().toLowerCase();

			User user = this.insight.getUser();
			Pair<String, String> jiraCreds = JiraUtils.getJiraCredentials(user);
			String accessToken = jiraCreds.getValue0();
			String baseUrl = jiraCreds.getValue1();

			switch (action) {
			case "get": {
				String issueKey = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));
				List<Map<String, Object>> result = JiraHelper.getAttachments(accessToken, baseUrl, issueKey);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "add": {
				String issueKey = JiraUtils.validateIssueKey(this.keyValue.get(JIRAID));
				String filePath = this.keyValue.get(FILE_PATH);
				if (filePath == null || filePath.trim().isEmpty()) {
					throw new SemossPixelException("File path (filePath) is required for the add action.");
				}
				String localFilePath = getLocalFilePath(filePath);
				File localFile = new File(localFilePath);
				if (!localFile.exists() || !localFile.isFile()) {
					throw new SemossPixelException(
							"File not found in insight folder: " + new File(filePath).getName());
				}
				Map<String, Object> result = JiraHelper.addAttachment(accessToken, baseUrl, issueKey, localFilePath);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			case "delete": {
				String attachmentId = JiraUtils.validateNumericId(this.keyValue.get(ATTACHMENT_ID), "attachmentId");
				Map<String, Object> result = JiraHelper.deleteAttachment(accessToken, baseUrl, attachmentId);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			default:
				throw new SemossPixelException("Invalid action '" + action + "'. Valid values are: get, add, delete.");
			}
		} catch (SemossPixelException e) {
			classLogger.error("Error in JiraAttachmentReactor", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to execute JiraAttachmentReactor", e);
			throw new SemossPixelException(
					"An error occurred in JiraAttachmentReactor. Error message: " + e.getMessage());
		}
	}

	public String getLocalFilePath(String filePath) {
		String insightFolder = this.insight.getInsightFolder();
		String audioFileName = new File(filePath).getName();
		String tempFilePath = insightFolder + File.separator + audioFileName;
		return new File(tempFilePath).getAbsolutePath();
	}

	@Override
	public String getReactorDescription() {
		return "Manages file attachments on a Jira issue. Supports listing, uploading, and deleting attachments.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ACTION)) {
			return "Required operation to perform. Valid values: get - lists all attachments (jiraid required). add - uploads a file attachment (jiraid, filePath required). delete - removes an attachment (attachmentId required).";
		} else if (key.equals(JIRAID)) {
			return "Jira issue key in KEY-NUMBER format (for example, RTJ-123). Required for get and add actions.";
		} else if (key.equals(FILE_PATH)) {
			return "Name or path of the file to upload from the insight folder (for example, screenshot.png). Required for add action.";
		} else if (key.equals(ATTACHMENT_ID)) {
			return "Numeric attachment id. Required for delete action.";
		}
		return super.getDescriptionForKey(key);
	}
}
