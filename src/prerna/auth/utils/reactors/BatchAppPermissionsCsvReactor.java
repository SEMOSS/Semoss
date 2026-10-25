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
package prerna.auth.utils.reactors;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class BatchAppPermissionsCsvReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(BatchAppPermissionsCsvReactor.class);

	private static final String EMAIL_HEADER = "email";
	private static final String PERMISSION_HEADER = "permission";

	public BatchAppPermissionsCsvReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PROJECT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		// determine if this is an engine or project operation
		String engineId = this.keyValue.get(this.keysToGet[1]);
		String projectId = this.keyValue.get(this.keysToGet[2]);

		boolean isProject = false;
		String targetId;

		if (projectId != null && !projectId.trim().isEmpty()) {
			isProject = true;
			targetId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		} else if (engineId != null && !engineId.trim().isEmpty()) {
			targetId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
		} else {
			throw new IllegalArgumentException("Must provide either engine or project id");
		}

		// authorization check - must be OWNER or admin
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if (isProject) {
			if (!isAdmin && !SecurityProjectUtils.userIsOwner(user, targetId)) {
				throw new IllegalArgumentException("Only project owners can batch assign permissions.");
			}
		} else {
			if (!isAdmin && !SecurityEngineUtils.userIsOwner(user, targetId)) {
				throw new IllegalArgumentException("Only engine owners can batch assign permissions.");
			}
		}

		// resolve file path
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File uploadFile = new File(Utility.normalizePath(filePath));
		if (!uploadFile.exists() || !uploadFile.isFile()) {
			throw new IllegalArgumentException("Could not find the specified file");
		}

		// parse CSV and process
		int added = 0;
		int updated = 0;
		List<String> missingUsers = new ArrayList<>();
		List<Map<String, String>> failures = new ArrayList<>();

		CSVFileHelper helper = new CSVFileHelper();
		try {
			helper.parse(filePath);
			String[] headers = helper.getHeaders();
			List<String> headersList = Arrays.asList(headers);

			// normalize headers to lowercase for matching
			int emailIdx = -1;
			int permIdx = -1;
			for (int i = 0; i < headers.length; i++) {
				String h = headers[i].trim().toLowerCase();
				if (h.equals(EMAIL_HEADER)) {
					emailIdx = i;
				} else if (h.equals(PERMISSION_HEADER)) {
					permIdx = i;
				}
			}
			if (emailIdx < 0 || permIdx < 0) {
				throw new IllegalArgumentException(
						"CSV must contain 'email' and 'permission' headers. Found: " + headersList);
			}

			// read all rows into a deduplicated map (last occurrence wins)
			LinkedHashMap<String, String> emailToPermission = new LinkedHashMap<>();
			String[] row;
			while ((row = helper.getNextRow()) != null) {
				String email = row.length > emailIdx ? row[emailIdx] : null;
				String perm = row.length > permIdx ? row[permIdx] : null;
				if (email == null || email.trim().isEmpty()) {
					continue;
				}
				emailToPermission.put(email.trim().toLowerCase(), perm != null ? perm.trim().toUpperCase() : null);
			}

			// process each entry
			for (Map.Entry<String, String> entry : emailToPermission.entrySet()) {
				String email = entry.getKey();
				String permStr = entry.getValue();

				try {
					// validate permission value
					if (permStr == null || permStr.isEmpty()) {
						Map<String, String> failure = new HashMap<>();
						failure.put("email", email);
						failure.put("error", "Permission value is empty");
						failures.add(failure);
						continue;
					}

					try {
						AccessPermissionEnum.valueOf(permStr);
					} catch (IllegalArgumentException e) {
						Map<String, String> failure = new HashMap<>();
						failure.put("email", email);
						failure.put("error", "Invalid permission value: " + permStr);
						failures.add(failure);
						continue;
					}

					// look up userId and type by email
				String[] userInfo = SecurityQueryUtils.getUserIdAndTypeByEmail(email);
					if (userInfo == null) {
						missingUsers.add(email);
						continue;
					}
					String userId = userInfo[0];
					String userType = userInfo[1];

					if (isProject) {
						Integer existingPermission = SecurityProjectUtils.getUserProjectPermission(userId, targetId);
						if (existingPermission != null) {
							SecurityProjectUtils.editProjectUserPermission(user, userId, userType, targetId, permStr, null);
							updated++;
						} else {
							SecurityProjectUtils.addProjectUser(user, userId, targetId, permStr, null);
							added++;
						}
					} else {
						Integer existingPermission = SecurityEngineUtils.getUserEnginePermission(userId, targetId);
						if (existingPermission != null) {
							SecurityEngineUtils.editEngineUserPermission(user, userId, userType, targetId, permStr, null, null, null, 0, 0);
							updated++;
						} else {
							SecurityEngineUtils.addEngineUser(user, userId, targetId, permStr, null, null, null, 0, 0);
							added++;
						}
					}

				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					Map<String, String> failure = new HashMap<>();
					failure.put("email", email);
					failure.put("error", e.getMessage());
					failures.add(failure);
				}
			}

		} finally {
			helper.clear();
		}

		// build result
		Map<String, Object> result = new HashMap<>();
		result.put("success", true);
		result.put("added", added);
		result.put("updated", updated);
		result.put("missingUsers", missingUsers);
		result.put("failures", failures);

		return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	@Override
	public String getReactorDescription() {
		return "Batch assigns or updates user permissions for an engine or project by reading a CSV file. "
				+ "The CSV must contain 'email' and 'permission' columns. "
				+ "Only project/engine owners or admins can perform this operation.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The file path to the CSV file containing 'email' and 'permission' columns";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id of the engine to assign permissions for";
		} else if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id of the project to assign permissions for";
		}
		return super.getDescriptionForKey(key);
	}

}
