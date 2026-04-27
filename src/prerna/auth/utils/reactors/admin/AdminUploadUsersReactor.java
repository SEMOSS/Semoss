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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class AdminUploadUsersReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminUploadUsersReactor.class);
	private static final String CLASS_NAME = AdminUploadUsersReactor.class.getName();

	static final String NAME_KEY = "NAME";
	static final String EMAIL_KEY = "EMAIL";
	static final String TYPE_KEY = "TYPE";
	static final String ID_KEY = "ID";
	static final String PASSWORD_KEY = "PASSWORD";
	static final String SALT_KEY = "SALT";
	static final String USERNAME_KEY = "USERNAME";
	static final String ADMIN_KEY = "ADMIN";
	static final String PUBLISHER_KEY = "PUBLISHER";

	private static String insertQuery = null;
	private static Map<String, Integer> psIndex = new HashMap<>();
	static {
		String[] headers = new String[] { NAME_KEY, EMAIL_KEY, TYPE_KEY, ID_KEY, PASSWORD_KEY, SALT_KEY, USERNAME_KEY,
				ADMIN_KEY, PUBLISHER_KEY };
		StringBuilder builder = new StringBuilder("INSERT INTO SMSS_USER (");
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

	public AdminUploadUsersReactor() {
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
				classLogger.error("Unable to upload users from the provided file.", e);
				throw new IllegalArgumentException("Error loading admin users : " + e.getMessage());
			} finally {
				if (it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error("Unable to upload users from the provided file.", e);
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
		// see if there is a user limit applied
		int currentUserCount = -1;

		String userLimitStr = Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT);
		int userLimit = -1;
		if (userLimitStr != null && !userLimitStr.trim().isEmpty()) {
			try {
				userLimit = Integer.parseInt(userLimitStr);
				currentUserCount = SecurityQueryUtils.getApplicationUserCount();
			} catch (NumberFormatException e) {
				classLogger.error("Unable to read user records from the uploaded Excel file.", e);
				classLogger.error("User limit is not a valid numeric value");
			}
		}

		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = database.getConnection();
			ps = conn.prepareStatement(insertQuery);

			String[] excelHeaders = helper.getHeaders();
			List<String> excelHeadersList = Arrays.asList(excelHeaders);

			int idxName = excelHeadersList.indexOf(NAME_KEY);
			int idxEmail = excelHeadersList.indexOf(EMAIL_KEY);
			int idxType = excelHeadersList.indexOf(TYPE_KEY);
			int idxId = excelHeadersList.indexOf(ID_KEY);
			int idxPassword = excelHeadersList.indexOf(PASSWORD_KEY);
			int idxSalt = excelHeadersList.indexOf(SALT_KEY);
			int idxUsername = excelHeadersList.indexOf(USERNAME_KEY);
			int idxAdmin = excelHeadersList.indexOf(ADMIN_KEY);
			int idxPublisher = excelHeadersList.indexOf(PUBLISHER_KEY);

			if (idxName < 0 || idxEmail < 0 || idxType < 0 || idxId < 0 || idxPassword < 0 || idxSalt < 0
					|| idxUsername < 0 || idxAdmin < 0 || idxPublisher < 0) {
				throw new IllegalArgumentException("One or more headers are missing from the excel");
			}

			int counter = 0;
			Object[] row = null;
			while ((helper.hasNext())) {
				// if we hit the limit - throw an error
				if (userLimit > 0 && currentUserCount + 1 > userLimit) {
					throw new SemossPixelException("User Limit exceeded the max value of " + userLimit);
				}

				row = helper.next().getRawValues();
				// id could be string or # based on the type
				Object idObj = row[idxId];
				if (idObj == null || idObj.toString().trim().isEmpty()) {
					throw new IllegalArgumentException("Must have the id for the user defined - check row " + counter);
				}
				String id = null;
				if (idObj instanceof Number) {
					id = BigDecimal.valueOf(((Number) idObj).doubleValue()).toPlainString();
				} else {
					id = idObj + "";
				}
				String name = (String) row[idxName];
				String email = (String) row[idxEmail];
				String type = (String) row[idxType];
				String password = (String) row[idxPassword];
				String salt = (String) row[idxSalt];
				String username = (String) row[idxUsername];
				boolean admin = Boolean.parseBoolean(row[idxAdmin] + "");
				boolean publisher = Boolean.parseBoolean(row[idxPublisher] + "");

				if (id == null || id.isEmpty()) {
					throw new IllegalArgumentException("Must have the id for the user defined - check row " + counter);
				}
				// check if the ID already exists
				if (SecurityQueryUtils.checkUserExist(id)) {
					logger.info("User id = {} alraedy exists - skipping record for upload", id);
					continue;
				}

				if (type == null || type.isEmpty()) {
					throw new IllegalArgumentException(
							"Must have the type of login for the user defined - check row " + counter);
				}
				AuthProvider provider = AuthProvider.valueOf(type.trim().toUpperCase());
				if (provider == null) {
					throw new IllegalArgumentException(
							"Could not determine the correct provider type for " + type + " - check row " + counter);
				}

				// these 2 are required
				ps.setString(psIndex.get(TYPE_KEY), provider.toString());
				ps.setString(psIndex.get(ID_KEY), id.trim());
				// boolean is always true/false and defaults to false for parseBoolean logic
				ps.setBoolean(psIndex.get(ADMIN_KEY), admin);
				ps.setBoolean(psIndex.get(PUBLISHER_KEY), publisher);

				if (name == null) {
					ps.setNull(psIndex.get(NAME_KEY), java.sql.Types.VARCHAR);
				} else {
					ps.setString(psIndex.get(NAME_KEY), name.trim());
				}

				if (email == null) {
					ps.setNull(psIndex.get(EMAIL_KEY), java.sql.Types.VARCHAR);
				} else {
					ps.setString(psIndex.get(EMAIL_KEY), email.trim().toLowerCase());
				}

				if (username == null) {
					ps.setNull(psIndex.get(USERNAME_KEY), java.sql.Types.VARCHAR);
				} else {
					ps.setString(psIndex.get(USERNAME_KEY), username.trim());
				}

				// if password is there
				// but no salt
				// make the salt and store that
				if (password != null && !password.isEmpty() && salt != null && !salt.isEmpty()) {
					ps.setString(psIndex.get(PASSWORD_KEY), password.trim());
					ps.setString(psIndex.get(SALT_KEY), salt.trim());
				} else if (password != null && !password.isEmpty() && (salt == null || salt.isEmpty())) {
					String genSalt = AbstractSecurityUtils.generateSalt();
					String hashedPassword = (AbstractSecurityUtils.hash(password, genSalt));
					ps.setString(psIndex.get(PASSWORD_KEY), hashedPassword.trim());
					ps.setString(psIndex.get(SALT_KEY), genSalt.trim());
				} else {
					ps.setNull(psIndex.get(PASSWORD_KEY), java.sql.Types.VARCHAR);
					ps.setNull(psIndex.get(SALT_KEY), java.sql.Types.VARCHAR);
				}

				ps.addBatch();
				counter++;
			}
			ps.executeBatch();
			if (conn != null && !conn.getAutoCommit()) {
				conn.commit();
			}
			logger.info("Done with updates, total rows = {}", counter);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(database, conn, ps);
		}
	}

}
