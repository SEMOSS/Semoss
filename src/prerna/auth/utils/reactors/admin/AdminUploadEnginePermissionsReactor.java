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
package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.poi.main.helper.excel.ExcelBlock;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.poi.main.helper.excel.ExcelWorkbookFilePreProcessor;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class AdminUploadEnginePermissionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminUploadEnginePermissionsReactor.class);

	private static final String CLASS_NAME = AdminUploadEnginePermissionsReactor.class.getName();

	static final String ENGINE_ID_KEY = "ENGINEID";
	static final String USER_ID_KEY = "USERID";
	static final String PERMISSION_KEY = "PERMISSION";

	private static String insertQuery = null;
	private static Map<String, Integer> psIndex = new HashMap<>();
	static {
		String[] headers = new String[] { ENGINE_ID_KEY, USER_ID_KEY, PERMISSION_KEY };
		StringBuilder builder = new StringBuilder("INSERT INTO ENGINEPERMISSION (");
		for (int i = 0; i < headers.length; i++) {
			if (i > 0) {
				builder.append(", ");
			}
			builder.append(headers[i]);
		}
		builder.append(") VALUES (");
		for (int i = 0; i < headers.length; i++) {
			if (i > 0) {
				builder.append(", ");
			}
			builder.append("?");

			// also keep track of header to index for the file uploading
			psIndex.put(headers[i], (i + 1));
		}
		insertQuery = builder.append(")").toString();
	}

	private Logger logger = null;

	public AdminUploadEnginePermissionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		File uploadFile = new File(Utility.normalizePath(filePath));
		if (!uploadFile.exists() || !uploadFile.isFile()) {
			throw new IllegalArgumentException("Could not find the specified file");
		}

		this.logger = getLogger(CLASS_NAME);

		IRDBMSEngine database = SystemEngineRegistry.getSecurityDb();
		long start = System.currentTimeMillis();
		{
			ExcelSheetFileIterator it = null;
			try {
				it = getExcelIterator(filePath);
				loadExcelFile(database, it);
			} catch (Exception e) {
				classLogger.error("Unable to upload database permissions from the provided file.", e);
				throw new IllegalArgumentException("Error loading admin users : " + e.getMessage());
			} finally {
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error("Unable to upload database permissions from the provided file.", e);
					}
				}
			}
		}
		long end = System.currentTimeMillis();
		return new NounMetadata("Time to finish = " + (end - start) + "ms", PixelDataType.CONST_STRING);
	}

	private ExcelSheetFileIterator getExcelIterator(String fileLocation) {
		// get range
		ExcelWorkbookFilePreProcessor processor = new ExcelWorkbookFilePreProcessor();
		processor.parse(fileLocation);
		processor.determineTableRanges();
		Map<String, ExcelSheetPreProcessor> sheetProcessors = processor.getSheetProcessors();
		// get sheetName and headers
		String sheetName = processor.getSheetNames().get(0);
		String range = null;
		ExcelSheetPreProcessor sProcessor = sheetProcessors.get(sheetName);
		{
			List<ExcelBlock> blocks = sProcessor.getAllBlocks();
			// for(int i = 0; i < blocks.size(); i++) {
			ExcelBlock block = blocks.get(0);
			List<ExcelRange> blockRanges = block.getRanges();
			for (int j = 0; j < 1; j++) {
				ExcelRange r = blockRanges.get(j);
				logger.info("Found range = {}", r.getRangeSyntax());
				range = r.getRangeSyntax();
			}
		}
		processor.clear();

		ExcelQueryStruct qs = new ExcelQueryStruct();
		qs.setSheetName(sheetName);
		qs.setSheetRange(range);
		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(fileLocation);
		ExcelSheetFileIterator it = helper.getSheetIterator(qs);

		return it;
	}

	private void loadExcelFile(IRDBMSEngine database, ExcelSheetFileIterator helper) throws Exception {
		Connection conn = null;
		boolean hasInsert = false;
		PreparedStatement insertPs = null;
		try {
			conn = database.getConnection();
			insertPs = conn.prepareStatement(insertQuery);

			String[] excelHeaders = helper.getHeaders();
			List<String> excelHeadersList = Arrays.asList(excelHeaders);

			int idxEngine = excelHeadersList.indexOf(ENGINE_ID_KEY);
			int idxUser = excelHeadersList.indexOf(USER_ID_KEY);
			int idxRole = excelHeadersList.indexOf(PERMISSION_KEY);

			if (idxEngine < 0 || idxUser < 0 || idxRole < 0) {
				throw new IllegalArgumentException("One or more headers are missing from the excel");
			}

			int counter = 0;
			Object[] row = null;
			while ((helper.hasNext())) {
				row = helper.next().getRawValues();

				String engineId = (String) row[idxEngine];
				String userId = (String) row[idxUser];
				String role = (String) row[idxRole];

				if (engineId == null || engineId.isEmpty()) {
					throw new IllegalArgumentException(
							"Must have the engine id for the user defined - check row " + counter);
				}
				if (userId == null || userId.isEmpty()) {
					throw new IllegalArgumentException(
							"Must have the user id for the user defined - check row " + counter);
				}
				if (role == null || role.isEmpty()) {
					throw new IllegalArgumentException(
							"Must have the role for the user defined - check row " + counter);
				}

				AccessPermissionEnum permission = AccessPermissionEnum.valueOf(role);
				if (permission == null) {
					throw new IllegalArgumentException("Must have a valid permission role - check row " + counter);
				}

				// check if the ID already exists
				if (SecurityEngineUtils.checkUserHasAccessToDatabase(engineId, userId)) {
					// TODO: update based on user id instead of continue?
					logger.info("User id = {} alraedy exists for app = {} - skipping record for upload", userId,
							engineId);
					continue;
				} else {
					hasInsert = true;
					// add to insert ps
					insertPs.setString(psIndex.get(ENGINE_ID_KEY), engineId);
					insertPs.setString(psIndex.get(USER_ID_KEY), userId);
					insertPs.setInt(psIndex.get(PERMISSION_KEY), permission.getId());

					insertPs.addBatch();
				}

				counter++;
			}
			// we execute for insert and updates
			if (hasInsert) {
				insertPs.executeBatch();
			}
			if (conn != null && !conn.getAutoCommit()) {
				conn.commit();
			}
			logger.info("Done with updates, total rows = {}", counter);
		} catch (Exception e) {
			logger.error("Unable to read database-permission records from the uploaded Excel file.", e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(database, conn, insertPs);
		}
	}
}
