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
package prerna.prompt;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;

public final class PromptUtils {

	private static Logger classLogger = LogManager.getLogger(PromptUtils.class);

	static boolean initialized = false;

	private final static String PROMPT = "PROMPT";
	private final static List<String> PROMPT_COLUMNS = Arrays.asList("ID", "TITLE", "CONTEXT", "VERSION", "INTENT",
			"CREATED_BY", "DATE_CREATED", "IS_LATEST", "GLOBAL");

	private final static String INSERT_PROMPT_QUERY = "INSERT INTO PROMPT (ID, TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED, IS_LATEST, GLOBAL) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	/**
	 * 
	 * @throws Exception
	 */
	public static void loadPromptDatabase() throws Exception {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		PromptOwlCreator owlCreator = new PromptOwlCreator(promptDb.getQueryUtil());
		if (owlCreator.needsRemake(promptDb)) {
			owlCreator.remakeOwl(promptDb);
		}
		initialize();
		initialized = true;
	}

	/**
	 * 
	 * @throws Exception
	 */
	private static void initialize() throws Exception {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		String database = promptDb.getDatabase();
		String schema = promptDb.getSchema();
		String[] colNames = null;
		String[] types = null;

		AbstractSqlQueryUtil queryUtil = promptDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();

		// PROMPT
		colNames = new String[] { "ID", "TITLE", "CONTEXT", "VERSION", "INTENT", "CREATED_BY", "DATE_CREATED",
				"IS_LATEST", "GLOBAL" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, INTEGER_DATATYPE_NAME,
				"VARCHAR(255)", "VARCHAR(255)", TIMESTAMP_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME };
		if (allowIfExistsTable) {
			promptDb.insertData(queryUtil.createTableIfNotExists("PROMPT", colNames, types));
		} else {
			// see if table exists
			if (!queryUtil.tableExists(promptDb.getConnection(), "PROMPT", database, schema)) {
				// make the table
				promptDb.insertData(queryUtil.createTable("PROMPT", colNames, types));
			}
		}

		// check all the columns we want are there
		{
			List<String> allCols = queryUtil.getTableColumns(promptDb.getConnection(), "PROMPT", database, schema);
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					String type = types[i];
					String addColumnSql;
					if (BOOLEAN_DATATYPE_NAME.equals(type)) {
						addColumnSql = queryUtil.alterTableAddColumnWithDefault("PROMPT", col, type, false);
					} else {
						addColumnSql = queryUtil.alterTableAddColumn("PROMPT", col, types[i]);
					}
					classLogger.info("Running sql " + addColumnSql);
					promptDb.insertData(addColumnSql);
				}
			}
		}

		// PROMPTMETA
		// check if column exists
		colNames = new String[] { "PROMPT_ID", "METAKEY", "METAVALUE", "METAORDER" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, INTEGER_DATATYPE_NAME };
		if (allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("PROMPTMETA", colNames, types);
			classLogger.info("Running sql " + sql);
			promptDb.insertData(sql);
		} else {
			// see if table exists
			if (!queryUtil.tableExists(promptDb.getConnection(), "PROMPTMETA", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("PROMPTMETA", colNames, types);
				classLogger.info("Running sql " + sql);
				promptDb.insertData(sql);
			}
		}

		if (allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("PROMPTMETA_PROMPT_ID_INDEX", "PROMPTMETA", "PROMPT_ID");
			classLogger.info("Running sql " + sql);
			promptDb.insertData(sql);
		} else {
			// see if index exists
			if (!queryUtil.indexExists(promptDb, "PROMPTMETA_PROMPT_ID_INDEX", "PROMPTMETA", database, schema)) {
				String sql = queryUtil.createIndex("PROMPTMETA_PROMPT_ID_INDEX", "PROMPTMETA", "PROMPT_ID");
				classLogger.info("Running sql " + sql);
				promptDb.insertData(sql);
			}
		}

		// all have the same columns and default values
		colNames = new String[] { "METAKEY", "SINGLEMULTI", "DISPLAYORDER", "DISPLAYOPTIONS", "DEFAULTVALUES" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(500)" };
		if (allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists(Constants.PROMPT_METAKEYS, colNames, types);
			classLogger.info("Running sql " + sql);
			promptDb.insertData(sql);
		} else {
			// see if table exists
			if (!queryUtil.tableExists(promptDb.getConnection(), Constants.PROMPT_METAKEYS, database, schema)) {
				// make the table
				String sql = queryUtil.createTable(Constants.PROMPT_METAKEYS, colNames, types);
				classLogger.info("Running sql " + sql);
				promptDb.insertData(sql);
			}
		}
		// check all the columns we want are there
		{
			List<String> allCols = queryUtil.getTableColumns(promptDb.getConnection(), Constants.PROMPT_METAKEYS,
					database, schema);
			for (int i = 0; i < colNames.length; i++) {
				String col = colNames[i];
				if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
					classLogger.info(
							"Column '" + col + "' is not present in current list of columns: " + allCols.toString());
					String addColumnSql = queryUtil.alterTableAddColumn(Constants.PROMPT_METAKEYS, col, types[i]);
					classLogger.info("Running sql " + addColumnSql);
					promptDb.insertData(addColumnSql);
				}
			}
		}
		// see if there are any default values
		{
			try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(promptDb,
					"select count(*) from " + Constants.PROMPT_METAKEYS)) {
				if (wrapper.hasNext()) {
					int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
					if (numrows < 6) {
						promptDb.removeData("DELETE FROM " + Constants.PROMPT_METAKEYS + " WHERE 1=1");
						int order = 0;
						promptDb.insertData(queryUtil.insertIntoTable(Constants.PROMPT_METAKEYS, colNames, types,
								new Object[] { Constants.MARKDOWN, "single", order++, "markdown", null }));
						promptDb.insertData(queryUtil.insertIntoTable(Constants.PROMPT_METAKEYS, colNames, types,
								new Object[] { "description", "single", order++, "textarea", null }));
						promptDb.insertData(queryUtil.insertIntoTable(Constants.PROMPT_METAKEYS, colNames, types,
								new Object[] { "tag", "multi", order++, "multi-typeahead", null }));
						promptDb.insertData(queryUtil.insertIntoTable(Constants.PROMPT_METAKEYS, colNames, types,
								new Object[] { "domain", "multi", order++, "multi-typeahead", null }));
						promptDb.insertData(queryUtil.insertIntoTable(Constants.PROMPT_METAKEYS, colNames, types,
								new Object[] { "data classification", "multi", order++, "select-box",
										"Confidential,FOUO,Internal Only,IP,PII,PHI,Public,Restricted" }));
						promptDb.insertData(queryUtil.insertIntoTable(Constants.PROMPT_METAKEYS, colNames, types,
								new Object[] { "data restrictions", "multi", order++, "select-box",
										"Confidential Allowed,FOUO Allowed,Internal Allowed,IP Allowed,PII Allowed,PHI Allowed,Restricted Allowed" }));
					}
				}
			} catch (Exception e) {
				classLogger.error("Failed to initialize default records in {}.", Constants.PROMPT_METAKEYS, e);
			}
		}

		// commit the changes
		promptDb.commit();

	}

	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return PromptUtils.initialized;
	}

	/**
	 * Checks if a prompt with the specified title exists and is accessible to the
	 * user. Only returns true for prompts that are either global or created by the
	 * requesting user.
	 * 
	 * @param promptTitle The title of the prompt to check for existence
	 * @param user        The user making the request, used for access control
	 * @return true if a prompt with the given title exists and is accessible to the
	 *         user, false otherwise
	 */
	public static Boolean checkPromptTitle(String promptTitle, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__TITLE", "==", promptTitle));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));

		// Apply appropriate visibility filters based on user role
		applyPromptVisibilityFilters(user, qs);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(promptDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to check if prompt title '{}' exists.", promptTitle, e);
		}

		return false;
	}

	/**
	 * Creates a filter that allows prompts that are either global or created by the
	 * user. Returns an OR query filter: GLOBAL = true OR CREATED_BY = userId
	 * 
	 * @param user The user to create the filter for
	 * @return OrQueryFilter combining global and created_by conditions
	 */
	private static OrQueryFilter createGlobalOrCreatedByFilter(User user) {
		String userId = user.getPrimaryLoginToken().getId();
		OrQueryFilter globalOrCreatedByFilter = new OrQueryFilter();
		globalOrCreatedByFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__GLOBAL", "==", true));
		globalOrCreatedByFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__CREATED_BY", "==", userId));
		return globalOrCreatedByFilter;
	}

	/**
	 * Applies the filter GLOBAL = true OR CREATED_BY = userId to the
	 * SelectQueryStruct
	 * 
	 * @param user The user to apply filters for
	 * @param qs   The SelectQueryStruct to add filters to
	 */
	private static void applyPromptVisibilityFilters(User user, SelectQueryStruct qs) {
		qs.addExplicitFilter(createGlobalOrCreatedByFilter(user));
	}

	/**
	 * Retrieves a list of prompts accessible to the user, with optional filtering.
	 * Each prompt is returned as a Map containing: - Basic prompt information (ID,
	 * TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED, GLOBAL) - tags:
	 * List of String values where METAKEY equals "tag" - metaKeys: Map<String,
	 * Collection<String>> containing all other metadata organized by metakey
	 * 
	 * Only returns prompts that are either global or created by the requesting
	 * user.
	 * 
	 * @param user                 The user requesting prompts, used for access
	 *                             control
	 * @param filters              Optional GenRowFilters for additional filtering
	 *                             criteria
	 * @param promptMetadataFilter Optional map of specific metadata key-value pairs
	 *                             to filter by
	 * @param limit                Optional limit on the number of results returned
	 * @param offset               Optional offset for pagination
	 * @return List of prompt maps, each containing prompt details, tags, and
	 *         metadata
	 */
	public static List<Map<String, Object>> getPrompts(User user, GenRowFilters filters,
			Map<String, Object> promptMetadataFilter, String limit, String offset) {
		List<Map<String, Object>> promptDetails = appendPromptInfo(user, filters, promptMetadataFilter, limit, offset);
		Map<String, Integer> listIndexPromptMapping = new HashMap<>();
		List<String> promptIdList = new ArrayList<>();
		Integer i = 0;
		for (Map<String, Object> prompt : promptDetails) {
			String promptId = (String) prompt.get("id");
			promptIdList.add(promptId);
			listIndexPromptMapping.put(promptId, i++);
		}

		appendPromptTags(promptDetails, listIndexPromptMapping, promptIdList);
		return promptDetails;
	}

	/**
	 * Creates a new prompt with the provided details and inserts records into
	 * PROMPT and PROMPTMETA tables.
	 * 
	 * Expected promptDetails map keys: - title: String (required) - context: String
	 * (required) - intent: String (optional) - global: Boolean (optional, defaults
	 * to false) - tags: List<String> (optional) - metaMap: Map<String,
	 * Collection<String>> (optional)
	 * 
	 * @param promptDetails Map containing all prompt information
	 * @param user          The user creating the prompt
	 * @param userId        The ID of the user creating the prompt
	 * @return The UUID of the newly created prompt
	 * @throws IllegalArgumentException if validation fails
	 */
	public static String addPrompt(Map<String, Object> promptDetails, User user, String userId) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		boolean allowClob = promptDb.getQueryUtil().allowClobJavaObject();

		List<String> tags = (List<String>) promptDetails.get("tags");
		Map<String, Collection<String>> userSelectedMeta = (Map<String, Collection<String>>) promptDetails
				.get("metaMap");
		if (userSelectedMeta == null) {
			userSelectedMeta = new HashMap<>();
		}
		userSelectedMeta.remove("tags"); // shouldn't be passed in the metaMap

		String promptId = UUID.randomUUID().toString();

		promptDetailsValidation(promptDetails);

		insertPrompt(promptDetails, userId, allowClob, promptId);
		insertTagsAndMeta(tags, userSelectedMeta, promptId);

		return promptId;
	}

	/**
	 * Updates an existing prompt with new details by creating a new version.
	 * 
	 * Expected promptDetails map keys: - id: String (required) - The ID of the
	 * prompt to update - title: String (required) - context: String (required) -
	 * intent: String (optional) - tags: List<String> (optional) - metaMap:
	 * Map<String, Collection<String>> (optional)
	 * 
	 * @param promptDetails Map containing updated prompt information, must include
	 *                      "id"
	 * @param user          The user updating the prompt, used for authorization
	 * @throws IllegalArgumentException if validation fails or user lacks
	 *                                  permissions
	 */
	public static void editPrompt(Map<String, Object> promptDetails, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		String userId = user.getPrimaryLoginToken().getId();
		boolean allowClob = promptDb.getQueryUtil().allowClobJavaObject();

		List<String> tags = (List<String>) promptDetails.get("tags");
		Map<String, Collection<String>> userSelectedMeta = (Map<String, Collection<String>>) promptDetails
				.get("metaMap");
		if (userSelectedMeta == null) {
			userSelectedMeta = new HashMap<>();
		}
		userSelectedMeta.remove("tags"); // shouldn't be passed in the metaMap

		String promptId = (String) promptDetails.get("id");

		// Check authorization: user can only update their own prompts or global prompts
		// unless they're admin
		validatePromptUpdateAuthorization(promptId, user);

		promptDetailsValidation(promptDetails);
		updatePrompt(promptId);
		insertPrompt(promptDetails, userId, allowClob, promptId);
		updatePromptTags(promptId, userSelectedMeta, tags);
	}

	/**
	 * Validates that a user has permission to update a specific prompt.
	 * Authorization rules: - Regular users can only update prompts they created
	 * (regardless of global status) - Admins can update any global prompt OR any
	 * prompt they created
	 * 
	 * @param promptId The ID of the prompt to validate update permission for
	 * @param user     The user object for authorization checks
	 * @throws IllegalArgumentException if user lacks permission to update the
	 *                                  prompt
	 */
	private static void validatePromptUpdateAuthorization(String promptId, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		String userId = user.getPrimaryLoginToken().getId();

		// Query to get prompt details (CREATED_BY and GLOBAL fields)
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__CREATED_BY"));
		qs.addSelector(new QueryColumnSelector("PROMPT__GLOBAL"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));

		try {
			List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(promptDb, qs);
			if (results.isEmpty()) {
				throw new IllegalArgumentException("Prompt not found with ID: " + promptId);
			}

			Map<String, Object> promptData = results.get(0);
			String createdBy = (String) promptData.get("CREATED_BY");
			Boolean isGlobal = (Boolean) promptData.get("GLOBAL");

			if (isGlobal == null) {
				isGlobal = false;
			}

			boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
			boolean isCreator = userId.equals(createdBy);

			// Regular users can only update their own prompts
			if (!isAdmin && !isCreator) {
				throw new IllegalArgumentException("User does not have permission to update this prompt. "
						+ "Only the prompt creator can make changes.");
			}

			// Admins can update their own prompts or any global prompt
			if (isAdmin && !isCreator && !isGlobal) {
				throw new IllegalArgumentException(
						"Admin users can only update global prompts or prompts they created.");
			}
		} catch (Exception e) {
			if (e instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e;
			}
			classLogger.error("Failed to validate update authorization for prompt ID '{}'.", promptId, e);
			throw new IllegalArgumentException("Error validating prompt update authorization: " + e.getMessage());
		}
	}

	/**
	 * Marks an existing prompt as no longer the latest version. Sets IS_LATEST =
	 * false for the specified prompt ID.
	 * 
	 * @param promptId The ID of the prompt to mark as not latest
	 */
	private static void updatePrompt(String promptId) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		String[] colToUpdate = { "IS_LATEST" };
		String[] whereCol = { "ID" };
		String promptPermissionQuery = promptDb.getQueryUtil().createUpdatePreparedStatementString("PROMPT",
				colToUpdate, whereCol);

		PreparedStatement ps = null;
		try {
			ps = promptDb.getPreparedStatement(promptPermissionQuery);
			int i = 1;
			ps.setBoolean(i++, false);
			ps.setString(i++, promptId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to mark previous versions as non-latest for prompt ID '{}'.", promptId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
		}
	}

	/**
	 * Replaces all metadata for a prompt with new tags and metadata values.
	 * 
	 * @param promptId         The ID of the prompt to update metadata for
	 * @param userSelectedMeta Map of metadata keys to collections of values
	 * @param tags             List of tag values to associate with the prompt
	 */
	public static void updatePromptTags(String promptId, Map<String, Collection<String>> userSelectedMeta,
			List<String> tags) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		// first do a delete
		String deleteQ = "DELETE FROM PROMPTMETA WHERE PROMPT_ID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = promptDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, promptId);
			deletePs.execute();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to clear existing metadata for prompt ID '{}' before update.", promptId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, deletePs);
		}
		if (tags != null && !tags.isEmpty()) {
			insertTagsAndMeta(tags, userSelectedMeta, promptId);
		}
	}

	/**
	 * Queries PROMPTMETA table and appends tags and metadata to prompt details.
	 * Entries with METAKEY="tag" are added to a "tags" list. All other entries are
	 * added to a "metaKeys" map organized by metakey.
	 * 
	 * @param promptDetails          List of prompt detail maps to append metadata
	 *                               to
	 * @param listIndexPromptMapping Map of prompt IDs to their index in
	 *                               promptDetails list
	 * @param promptIdList           List of prompt IDs to query metadata for
	 */
	private static void appendPromptTags(List<Map<String, Object>> promptDetails,
			Map<String, Integer> listIndexPromptMapping, List<String> promptIdList) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// Add selectors with lowercase aliases for consistent API response keys
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE", "metavalue"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAORDER", "metaorder"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID", "prompt_id"));

		if (promptIdList != null && !promptIdList.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__PROMPT_ID", "==", promptIdList));
		}
		qs.addOrderBy("PROMPTMETA__PROMPT_ID");
		qs.addOrderBy("PROMPTMETA__METAORDER");

		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(promptDb, qs);
		for (Map<String, Object> ret : retList) {
			String promptId = (String) ret.get("prompt_id");
			String metaKey = (String) ret.get("metakey");
			String metaValue = (String) ret.get("metavalue");
			Integer loc = listIndexPromptMapping.get(promptId);
			// Skip if this prompt is not in our filtered result set
			if (loc == null) {
				continue;
			}

			Map<String, Object> promptDetail = promptDetails.get(loc);

			if ("tag".equals(metaKey)) {
				// Handle tags
				List<String> tagList = (List<String>) promptDetail.get("tags");
				if (tagList == null) {
					tagList = new ArrayList<>();
					promptDetail.put("tags", tagList);
				}
				tagList.add(metaValue);
			} else {
				// Handle other metadata in metaKeys
				Map<String, List<String>> metaKeys = (Map<String, List<String>>) promptDetail.get("metaKeys");
				if (metaKeys == null) {
					metaKeys = new HashMap<>();
					promptDetail.put("metaKeys", metaKeys);
				}
				List<String> valueList = metaKeys.get(metaKey);
				if (valueList == null) {
					valueList = new ArrayList<>();
					metaKeys.put(metaKey, valueList);
				}
				valueList.add(metaValue);
			}
		}
	}

	/**
	 * Queries the PROMPT table and returns basic prompt information. Applies access
	 * control (only global prompts or prompts created by the user), optional
	 * metadata filters, and pagination.
	 * 
	 * @param user                 The user requesting prompts, used for access
	 *                             control
	 * @param filters              Optional additional filters to apply
	 * @param promptMetadataFilter Optional map of specific metadata key-value pairs
	 *                             to filter by
	 * @param limit                Optional limit on number of results (as string)
	 * @param offset               Optional offset for pagination (as string)
	 * @return List of maps containing prompt information (ID, TITLE, CONTEXT,
	 *         VERSION, INTENT, CREATED_BY, DATE_CREATED, GLOBAL)
	 */
	private static List<Map<String, Object>> appendPromptInfo(User user, GenRowFilters filters,
			Map<String, Object> promptMetadataFilter, String limit, String offset) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		// QUERY PROMPT get ID, TITLE, CONTEXT, IS Public, other small thigngs
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pc : PROMPT_COLUMNS) {
			if (!"IS_LATEST".equals(pc)) {
				// Add selector with lowercase alias for consistent API response keys
				qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc, pc.toLowerCase()));
			}
		}

		if (promptMetadataFilter != null && !promptMetadataFilter.isEmpty()) {
			for (String k : promptMetadataFilter.keySet()) {
				SelectQueryStruct subMetaQs = new SelectQueryStruct();
				subMetaQs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));
				subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAKEY", "==", k));
				subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAVALUE", "==",
						promptMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROMPT__ID", "==", subMetaQs));
			}
		}

		// Apply appropriate visibility filters based on user role
		applyPromptVisibilityFilters(user, qs);

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));

		if (filters != null && !filters.isEmpty()) {
			qs.mergeExplicitFilters(filters);
		}
		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
			qs.setLimit(long_limit);
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
			qs.setOffSet(long_offset);
		}

		List<Map<String, Object>> promptDetails = QueryExecutionUtility.flushRsToMap(promptDb, qs);
		return promptDetails;
	}

	/**
	 * Validates prompt details before insertion or update. Ensures title and
	 * context are present and non-empty. Validates tags if provided.
	 * 
	 * @param promptDetails Map containing prompt information to validate
	 * @throws IllegalArgumentException if validation fails
	 */
	private static void promptDetailsValidation(Map<String, Object> promptDetails) {
		validatePromptBaseDetails(promptDetails);
		List<String> tags = (List<String>) promptDetails.get("tags");

		if (tags != null && !tags.isEmpty()) {
			validatePromptTags(tags);
		}
	}

	/**
	 * Validates that all tags in the list are non-null and non-empty.
	 * 
	 * @param tags List of tag strings to validate
	 * @throws IllegalArgumentException if any tag is null or empty
	 */
	private static void validatePromptTags(List<String> tags) {
		for (String tag : tags) {
			if (tag == null || tag.isEmpty()) {
				throw new IllegalArgumentException("Tag must be string and not empty");
			}
		}
	}

	/**
	 * Validates the required fields of a prompt (title and context).
	 * 
	 * @param promptDetails Map containing prompt details to validate
	 * @throws IllegalArgumentException if required fields are missing or invalid
	 */
	private static void validatePromptBaseDetails(Map<String, Object> promptDetails) {
		validateString(promptDetails, "title", false, false);
		validateString(promptDetails, "context", false, false);
	}

	/**
	 * Validates a string field in the prompt details map.
	 * 
	 * @param promptDetails Map containing the field to validate
	 * @param mapKey        The key of the field to validate
	 * @param nullable      Whether null values are allowed
	 * @param allowEmpty    Whether empty strings are allowed
	 * @throws IllegalArgumentException if validation fails
	 */
	private static void validateString(Map<String, Object> promptDetails, String mapKey, boolean nullable,
			boolean allowEmpty) {
		String value = null;
		try {
			value = (String) promptDetails.get(mapKey);
			value = value != null ? value.trim() : value;
			if (value == null && !nullable) {
				throw new IllegalArgumentException(mapKey + " cannot be null when adding a new prompt.");
			}
			if (value != null && value.isEmpty() && !allowEmpty) {
				throw new IllegalArgumentException(mapKey + " cannot be null when adding a new prompt.");
			}
		} catch (Exception e) {
			classLogger.error("Failed to validate prompt field '{}'.", mapKey, e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	/**
	 * Inserts tags and metadata entries into the PROMPTMETA table. Tags are stored
	 * with METAKEY="tag", other metadata uses the actual metakey.
	 * 
	 * @param tags             List of tag values to insert (stored with
	 *                         METAKEY="tag")
	 * @param userSelectedMeta Map of metadata keys to collections of values
	 * @param promptId         The ID of the prompt to associate metadata with
	 */
	private static void insertTagsAndMeta(List<String> tags, Map<String, Collection<String>> userSelectedMeta,
			String promptId) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		// First ensure all metakeys exist in PROMPTMETAKEYS
		for (String metaKey : userSelectedMeta.keySet()) {
			ensureUserMetaKeyExistsInPromptMetaKeys(metaKey);
		}

		// now we do the new insert with the order of the tags
		String promptMetaQuery = promptDb.getQueryUtil().createInsertPreparedStatementString("PROMPTMETA",
				new String[] { "PROMPT_ID", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = promptDb.getPreparedStatement(promptMetaQuery);
			int i = 0;
			for (String tag : tags) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, promptId);
				ps.setString(parameterIndex++, "tag");
				ps.setString(parameterIndex++, tag);
				ps.setInt(parameterIndex++, i++);
				ps.addBatch();
			}
			// Now add for every meta value
			for (Map.Entry<String, Collection<String>> entry : userSelectedMeta.entrySet()) {
				int order = 0;
				String metaKey = entry.getKey();
				Collection<String> metaValues = entry.getValue();
				for (String metaValue : metaValues) {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, promptId);
					ps.setString(parameterIndex++, metaKey);
					ps.setString(parameterIndex++, metaValue);
					ps.setInt(parameterIndex++, order++);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert prompt tags/metadata for prompt ID '{}'.", promptId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
		}
	}

	/**
	 * Ensures that a metakey exists in PROMPTMETAKEYS table. If it doesn't exist,
	 * copies it from security.USERMETAKEYS table.
	 * 
	 * @param metaKey The metakey to ensure exists
	 */
	private static void ensureUserMetaKeyExistsInPromptMetaKeys(String metaKey) {
		if (!metaKeyExistsInPromptMetaKeys(metaKey)) {
			copyMetaKeyFromUserMetaKeys(metaKey);
		}
	}

	/**
	 * Checks if a metakey exists in the PROMPTMETAKEYS table.
	 * 
	 * @param metaKey The metakey to check for
	 * @return true if the metakey exists in PROMPTMETAKEYS, false otherwise
	 */
	private static boolean metaKeyExistsInPromptMetaKeys(String metaKey) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPTMETAKEYS__METAKEY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETAKEYS__METAKEY", "==", metaKey));

		try {
			List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(promptDb, qs);
			return !results.isEmpty();
		} catch (Exception e) {
			classLogger.error("Failed to verify metakey '{}' in PROMPTMETAKEYS.", metaKey, e);
			return false;
		}
	}

	/**
	 * Copies a metakey from the security database USERMETAKEYS table to
	 * PROMPTMETAKEYS. Copies METAKEY, SINGLEMULTI, DISPLAYOPTIONS, and
	 * DEFAULTVALUES.
	 * 
	 * @param metaKey The metakey to copy from USERMETAKEYS to PROMPTMETAKEYS
	 */
	private static void copyMetaKeyFromUserMetaKeys(String metaKey) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		// Get the security database
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		// Query USERMETAKEYS for the metakey
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__SINGLEMULTI"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYOPTIONS"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DEFAULTVALUES"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETAKEYS__METAKEY", "==", metaKey));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				String fetchedMetaKey = (String) values[0];
				String singleMulti = (String) values[1];
				String displayOptions = (String) values[2];
				String defaultValues = (String) values[3];

				// Insert into PROMPTMETAKEYS (excluding DISPLAYORDER)
				String insertQuery = promptDb.getQueryUtil().createInsertPreparedStatementString("PROMPTMETAKEYS",
						new String[] { "METAKEY", "SINGLEMULTI", "DISPLAYOPTIONS", "DEFAULTVALUES" });
				PreparedStatement ps = null;
				try {
					ps = promptDb.getPreparedStatement(insertQuery);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, fetchedMetaKey);
					ps.setString(parameterIndex++, singleMulti);
					ps.setString(parameterIndex++, displayOptions);
					ps.setString(parameterIndex++, defaultValues);
					ps.execute();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to copy metakey '{}' into PROMPTMETAKEYS.", metaKey, e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to read USERMETAKEYS for metakey '{}'.", metaKey, e);
		}
	}

	/**
	 * Inserts a prompt record into the PROMPT table. Uses CLOB for CONTEXT field if
	 * database supports it, otherwise uses String. Automatically determines and
	 * increments the version number.
	 * 
	 * @param promptDetails Map containing prompt information (title, context,
	 *                      intent, global)
	 * @param userId        The ID of the user creating/updating the prompt
	 * @param allowClob     Whether the database supports CLOB objects for the
	 *                      context field
	 * @param promptId      The UUID for the prompt (generated for new prompts,
	 *                      reused for updates)
	 * @throws IllegalArgumentException if insertion fails
	 */
	private static void insertPrompt(Map<String, Object> promptDetails, String userId, boolean allowClob,
			String promptId) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		PreparedStatement promptPS = null;
		try {
			promptPS = promptDb.getPreparedStatement(INSERT_PROMPT_QUERY);
			int index = 1;
			promptPS.setString(index++, promptId);
			promptPS.setString(index++, (String) promptDetails.get("title"));
			if (allowClob) {
				Clob toclob = promptDb.getConnection().createClob();
				toclob.setString(1, (String) promptDetails.get("context"));
				promptPS.setClob(index++, toclob);
			} else {
				promptPS.setString(index++, (String) promptDetails.get("context"));
			}
			// Get version of existing prompt
			Integer version = getVersionNumber(promptId);
			promptPS.setInt(index++, version);
			promptPS.setString(index++, (String) promptDetails.get("intent"));
			promptPS.setString(index++, userId);
			promptPS.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			promptPS.setBoolean(index++, true);
			// Set GLOBAL value, default to false if not provided
			Boolean global = (Boolean) promptDetails.get("global");
			promptPS.setBoolean(index++, global != null ? global : false);
			promptPS.execute();
			if (!promptPS.getConnection().getAutoCommit()) {
				promptPS.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert prompt record for prompt ID '{}'.", promptId, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, null, promptPS, null);
		}
	}

	/**
	 * Retrieves the next version number for a prompt. Returns the most recent
	 * version + 1, or 0 for new prompts.
	 * 
	 * @param promptId The ID of the prompt to get the version number for
	 * @return The next version number (existing version + 1, or 0 if no versions
	 *         exist)
	 */
	private static Integer getVersionNumber(String promptId) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		Integer version = 0;
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__VERSION"));
		qs.addSelector(new QueryColumnSelector("PROMPT__DATE_CREATED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptId));
		qs.addOrderBy("PROMPT__DATE_CREATED", "desc");
		qs.setLimit(1);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(promptDb, qs)) {
			if (wrapper.hasNext()) {
				version = (Integer) wrapper.next().getValues()[0];
				version += 1;
				return version;
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve current prompt version for prompt ID '{}'.", promptId, e);
		}
		return version;
	}

	/**
	 * Deletes a prompt and all its associated metadata. Removes entries from PROMPT
	 * and PROMPTMETA tables.
	 * 
	 * @param promptId The ID of the prompt to delete
	 * @param user     The user object for authorization checks
	 * @return The UUID of the deleted prompt
	 */
	public static String deletePrompt(String promptId, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		// Check authorization: user can only delete their own prompts or global prompts
		// unless they're admin
		validatePromptUpdateAuthorization(promptId, user);

		List<String> deletes = new ArrayList<>();
		deletes.add("DELETE FROM PROMPT WHERE ID=?");
		deletes.add("DELETE FROM PROMPTMETA WHERE PROMPT_ID=?");

		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = promptDb.getPreparedStatement(deleteQuery);
				ps.setString(1, promptId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to execute prompt delete statement for prompt ID '{}'.", promptId, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
			}
		}

		return promptId;
	}

	/**
	 * Retrieves all available metadata values for specified metakeys with usage
	 * counts. Results are grouped by METAKEY and METAVALUE.
	 * 
	 * @param metaKeys List of metakeys to retrieve values for
	 * @return List of maps containing metakey, metavalue, and count for each entry
	 */
	public static List<Map<String, Object>> getAvailableMetaValues(List<String> metaKeys) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE", "metavalue"));
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE"));
		qs.addSelector(fSelector);
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAKEY", "==", metaKeys));

		// group
		qs.addGroupBy(new QueryColumnSelector("PROMPTMETA__METAKEY"));
		qs.addGroupBy(new QueryColumnSelector("PROMPTMETA__METAVALUE"));

		return QueryExecutionUtility.flushRsToMap(promptDb, qs);
	}

	/**
	 * Retrieves a specific prompt by ID with access control. Returns the prompt
	 * only if it is global or created by the requesting user. The returned map
	 * includes: - Basic prompt information (ID, TITLE, CONTEXT, VERSION, INTENT,
	 * CREATED_BY, DATE_CREATED, GLOBAL) - tags: List of String values where METAKEY
	 * equals "tag" - metaKeys: Map<String, List<String>> containing all other
	 * metadata organized by metakey
	 * 
	 * @param promptID The ID of the prompt to retrieve
	 * @param user     The user requesting the prompt, used for access control
	 * @return Map containing prompt details, tags, and metadata (empty map if not
	 *         found or no access)
	 */
	public static Map<String, Object> getPrompt(String promptID, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pc : PROMPT_COLUMNS) {
			if (pc != "IS_LATEST") {
				// Add selector with lowercase alias for consistent API response keys
				qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc, pc.toLowerCase()));
			}
		}

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptID));

		// Apply appropriate visibility filters based on user role
		applyPromptVisibilityFilters(user, qs);

		List<Map<String, Object>> promptDetails = QueryExecutionUtility.flushRsToMap(promptDb, qs);

		Map<String, Object> promptDetail = promptDetails.isEmpty() ? new HashMap<String, Object>()
				: promptDetails.get(0);

		// Append Tags
		if (!promptDetail.isEmpty()) {
			getPromptTags(promptID, promptDetail);
		}
		return promptDetail;
	}

	/**
	 * Retrieves a specific prompt by ID with access control. Returns the prompt
	 * only if it is global or created by the requesting user. The returned map
	 * includes: - Basic prompt information (ID, TITLE, CONTEXT, VERSION, INTENT,
	 * CREATED_BY, DATE_CREATED, GLOBAL) - tags: List of String values where METAKEY
	 * equals "tag" - metaKeys: Map<String, List<String>> containing all other
	 * metadata organized by metakey
	 * 
	 * @param promptID The ID of the prompt to retrieve
	 * @param user     The user requesting the prompt, used for access control
	 * @return Map containing prompt details, tags, and metadata (empty map if not
	 *         found or no access)
	 */
	public static List<Map<String, Object>> getPromptWithVersioning(String promptID, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();

		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pc : PROMPT_COLUMNS) {
			// Add selector with lowercase alias for consistent API response keys
			qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc, pc.toLowerCase()));
		}

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptID));
		qs.addOrderBy(new QueryColumnOrderBySelector("PROMPT__VERSION", "DESC"));

		// Apply appropriate visibility filters based on user role
		applyPromptVisibilityFilters(user, qs);

		List<Map<String, Object>> promptDetails = QueryExecutionUtility.flushRsToMap(promptDb, qs);

		for (Map<String, Object> promptDetail : promptDetails) {
			// Append Tags
			if (!promptDetails.isEmpty()) {
				getPromptTags(promptID, promptDetail);
			}
		}

		return promptDetails;
	}

	/**
	 * Queries PROMPTMETA for a specific prompt and appends tags and metadata.
	 * Entries with METAKEY="tag" are added to a "tags" list. All other entries are
	 * added to a "metaKeys" map organized by metakey.
	 * 
	 * @param promptID      The ID of the prompt to retrieve metadata for
	 * @param promptDetails Map to append tags and metaKeys to
	 */
	private static void getPromptTags(String promptID, Map<String, Object> promptDetails) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// Add selectors with lowercase aliases for consistent API response keys
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE", "metavalue"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAORDER", "metaorder"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID", "prompt_id"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__PROMPT_ID", "==", promptID));
		qs.addOrderBy("PROMPTMETA__PROMPT_ID");
		qs.addOrderBy("PROMPTMETA__METAORDER");

		List<String> tagList = new ArrayList<>();
		Map<String, List<String>> metaKeys = new HashMap<>();

		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(promptDb, qs);
		for (Map<String, Object> ret : retList) {
			String metaKey = (String) ret.get("metakey");
			String metaValue = (String) ret.get("metavalue");

			if ("tag".equals(metaKey)) {
				// Handle tags
				tagList.add(metaValue);
			} else {
				// Handle other metadata in metaKeys
				List<String> valueList = metaKeys.get(metaKey);
				if (valueList == null) {
					valueList = new ArrayList<>();
					metaKeys.put(metaKey, valueList);
				}
				valueList.add(metaValue);
			}
		}

		promptDetails.put("tags", tagList);
		promptDetails.put("metaKeys", metaKeys);
	}

	/**
	 * Updates specific metadata fields for a prompt. Replaces existing entries for
	 * the specified metakeys with new values. Preserves other metadata fields not
	 * included in the update.
	 * 
	 * @param promptId The ID of the prompt to update metadata for
	 * @param metadata Map of metakeys to values (String, List, or Collection)
	 * @param user     The user object for authorization checks
	 */
	public static void updatePromptMetadata(String promptId, Map<String, Object> metadata, User user) {
		IRDBMSEngine promptDb = SystemEngineRegistry.getPromptDb();
		// Check authorization: user can only update metadata for their own prompts or
		// global prompts unless they're admin
		validatePromptUpdateAuthorization(promptId, user);
		// first do a delete
		String deleteQ = "DELETE FROM PROMPTMETA WHERE METAKEY=? AND PROMPT_ID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = promptDb.getPreparedStatement(deleteQ);
			for (String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, promptId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete existing metadata keys for prompt ID '{}'.", promptId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, deletePs);
		}

		// now we do the new insert with the order of the tags
		String query = promptDb.getQueryUtil().createInsertPreparedStatementString("PROMPTMETA",
				new String[] { "PROMPT_ID", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = promptDb.getPreparedStatement(query);
			for (String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if (val instanceof List) {
					values = (List<Object>) val;
				} else if (val instanceof Collection) {
					values.addAll((Collection<Object>) val);
				} else {
					values.add(val);
				}

				for (int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);

					ps.setString(parameterIndex++, promptId);
					ps.setString(parameterIndex++, field);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, i);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to insert updated metadata values for prompt ID '{}'.", promptId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
		}
	}

}
