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

import java.io.IOException;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class PromptUtils extends AbstractPromptUtils {

	private static Logger classLogger = LogManager.getLogger(PromptUtils.class);
	
	private final static String PROMPT = "PROMPT";

	private final static String promptQuery = "INSERT INTO PROMPT (ID, TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED, IS_LATEST, GLOBAL) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private final static List<String> PROMPT_COLUMNS = Arrays.asList(
			"ID",
			"TITLE",
			"CONTEXT",
			"VERSION",
			"INTENT",
			"CREATED_BY",
			"DATE_CREATED",
			"IS_LATEST",
			"GLOBAL"
			);

	/**
	 * Checks if a prompt with the specified title exists and is accessible to the user.
	 * Queries the PROMPT table and applies user metadata filters to ensure the prompt
	 * is visible to the requesting user based on their permissions.
	 * 
	 * @param promptTitle The title of the prompt to check for existence
	 * @param user The user making the request, used for metadata-based access control
	 * @return true if a prompt with the given title exists and is accessible to the user, false otherwise
	 */
	public static Boolean checkPromptTitle(String promptTitle, User user) {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__TITLE", "==", promptTitle));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));
		
//		Add filters based on user metadata: Get user meta
		addUserMetaFiltersToQs(user, qs);
		
		// Add filter: GLOBAL = true OR CREATED_BY = userId
		qs.addExplicitFilter(createGlobalOrCreatedByFilter(user));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(promptDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}

	/**
	 * Adds user metadata filters to a SelectQueryStruct to ensure prompts are filtered
	 * based on the user's metadata permissions. For each metadata key in the user's profile,
	 * creates a subquery that includes prompts with at least one matching metadata value.
	 * 
	 * @param user The user whose metadata will be used for filtering
	 * @param qs The SelectQueryStruct to add filters to
	 */
	private static void addUserMetaFiltersToQs(User user, SelectQueryStruct qs) {
		Map<String, Collection<String>> userMetaMap = user.getPrimaryLoginToken().getMeta();
		
		// Only do so if user is an admin, otherwise everything can be kept in
		if (!SecurityAdminUtils.userIsAdmin(user)) {
		// Add filters based on user metadata
			if (userMetaMap != null && !userMetaMap.isEmpty()) {
				for (Map.Entry<String, Collection<String>> metaEntry : userMetaMap.entrySet()) {
					String metaKey = metaEntry.getKey();
					Collection<String> metaValues = metaEntry.getValue();
					
					// Create a subquery that finds prompts with this metakey and matching values
					SelectQueryStruct subMetaQs = new SelectQueryStruct();
					subMetaQs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));
					subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAKEY", "==", metaKey));
					subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAVALUE", "==", metaValues));
					subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToColFilter("PROMPTMETA__PROMPT_ID", "==", "PROMPT__ID"));
					
					// Include prompts that have at least one matching metadata value for this key
					qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROMPT__ID", "==", subMetaQs));
				}
			}
		}
	}

	/**
	 * Creates a filter that allows prompts that are either global or created by the user.
	 * Returns an OR filter: GLOBAL = true OR CREATED_BY = userId
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
	 * Retrieves a list of prompts accessible to the user, filtered by user metadata permissions.
	 * Each prompt is returned as a Map containing:
	 * - Basic prompt information (ID, TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED)
	 * - tags: List of String values where METAKEY equals "tag"
	 * - metaKeys: Map<String, List<String>> containing all other metadata organized by metakey
	 * 
	 * The method applies user metadata filtering to ensure only prompts with matching metadata
	 * values are returned.
	 * 
	 * @param user The user requesting prompts, used for metadata-based filtering
	 * @param filters Optional GenRowFilters for additional filtering criteria
	 * @param promptMetadataFilter Optional map of specific metadata key-value pairs to filter by
	 * @param limit Optional limit on the number of results returned
	 * @param offset Optional offset for pagination
	 * @return List of prompt maps, each containing prompt details, tags, and metadata
	 */
	public static List<Map<String, Object>> getPrompts(User user, GenRowFilters filters, Map<String, Object> promptMetadataFilter, String limit, String offset) {
		List<Map<String, Object>> promptDetails = appendPromptInfo(user, filters, promptMetadataFilter, limit, offset);
		Map<String, Integer> listIndexPromptMapping = new HashMap<>();
		List<String> promptIdList = new ArrayList<>();
		Integer i = 0;
		for(Map<String, Object> prompt: promptDetails) {
			String promptId = (String) prompt.get("ID");
			promptIdList.add(promptId);
			listIndexPromptMapping.put(promptId, i++);
		}

		appendPromptTags(promptDetails, listIndexPromptMapping, promptIdList);
		return promptDetails;

	}
	
	/**
	 * Checks if one metadata map is a subset of another.
	 * For map 'a' to be a subset of map 'b', every key in 'a' must exist in 'b',
	 * and the collection of values in 'b' must contain all values from 'a' for that key.
	 * 
	 * @param a The metadata map to check if it's a subset
	 * @param b The metadata map to check against
	 * @return true if 'a' is a subset of 'b', false otherwise
	 */
	private static boolean metaKeysIsSubset(Map<String, Collection<String>> a, Map<String, Collection<String>> b) {
		return a.entrySet().parallelStream().allMatch(entry -> {
			Collection<String> bValues = b.get(entry.getKey());
			return bValues != null && bValues.containsAll(entry.getValue());
		});
	}


	/**
	 * Creates a new prompt with the provided details.
	 * Validates all input data, ensures user has permission to use selected metadata,
	 * and inserts records into PROMPT and PROMPTMETA tables. Also ensures referenced
	 * metakeys exist in PROMPTMETAKEYS table.
	 * 
	 * Expected promptDetails map keys:
	 * - title: String (required)
	 * - context: String (required)
	 * - intent: String (optional)
	 * - tags: List<String> (optional)
	 * - metaMap: Map<String, Collection<String>> (optional)
	 * 
	 * @param promptDetails Map containing all prompt information
	 * @param user The user creating the prompt, used for validation
	 * @param userId The ID of the user creating the prompt
	 * @throws IllegalArgumentException if validation fails or user lacks permissions
	 */
	public static void addPrompt(Map<String, Object> promptDetails, User user, String userId) {
		boolean allowClob = promptDb.getQueryUtil().allowClobJavaObject();

		List<String> tags = (List<String>) promptDetails.get("tags");
		Map<String, Collection<String>> userSelectedMeta = (Map<String, Collection<String>>) promptDetails.get("metaMap");
		userSelectedMeta.remove("tags"); // shouldn't be passed in the metaMap
		validateSelectedMetadata(user, userSelectedMeta);

		String promptId = UUID.randomUUID().toString();

		promptDetailsValidation(promptDetails);

		insertPrompt(promptDetails, userId, allowClob, promptId);
		insertTagsAndMeta(tags, userSelectedMeta, promptId);
	}

	/**
	 * Updates an existing prompt with new details.
	 * Marks the previous version as not latest, validates all input data,
	 * creates a new version of the prompt, and updates associated metadata.
	 * 
	 * Expected promptDetails map keys:
	 * - id: String (required) - The ID of the prompt to update
	 * - title: String (required)
	 * - context: String (required)
	 * - intent: String (optional)
	 * - tags: List<String> (optional)
	 * - metaMap: Map<String, Collection<String>> (optional)
	 * 
	 * @param promptDetails Map containing updated prompt information, must include "id"
	 * @param userId The ID of the user updating the prompt
	 * @param user The user updating the prompt, used for validation
	 * @throws IllegalArgumentException if validation fails or user lacks permissions
	 */
	public static void editPrompt(Map<String, Object> promptDetails, String userId, User user) {
		boolean allowClob = promptDb.getQueryUtil().allowClobJavaObject();

		List<String> tags = (List<String>) promptDetails.get("tags");
		Map<String, Collection<String>> userSelectedMeta = (Map<String, Collection<String>>) promptDetails.get("metaMap");
		userSelectedMeta.remove("tags"); // shouldn't be passed in the metaMap
		validateSelectedMetadata(user, userSelectedMeta);

		String promptId = (String) promptDetails.get("id");

		promptDetailsValidation(promptDetails);
		updatePrompt(promptId);
		insertPrompt(promptDetails, userId, allowClob, promptId);
		updatePromptTags(promptId, userSelectedMeta, tags);
	}

	/**
	 * Validates that the user has permission to use the selected metadata.
	 * For admin users, validates that all metakeys exist in the system.
	 * For non-admin users, validates that all selected metadata is a subset of
	 * the user's existing metadata permissions.
	 * 
	 * @param user The user to validate permissions for
	 * @param userSelectedMetadata The metadata the user wants to assign to the prompt
	 * @throws IllegalArgumentException if validation fails
	 */
	private static void validateSelectedMetadata(User user, Map<String, Collection<String>> userSelectedMetadata) {
		Map<String, Collection<String>> existingMeta = user.getPrimaryLoginToken().getMeta();
		if (SecurityAdminUtils.userIsAdmin(user)) {
//			Admins can add prompts with any existing user metakeys using any metavalue (existent or not)
			List<Map<String, Object>> metakeyOptions = SecurityUserUtils.getMetakeyOptions(null);
			Set<String> userMetaKeys = userSelectedMetadata.keySet();
			Set<String> metaKeys = new HashSet<>();
			for (Map<String, Object> metaEntry : metakeyOptions) {
				String metaKey = (String) metaEntry.get("metakey");
				metaKeys.add(metaKey);
			}
			if (!metaKeys.containsAll(userMetaKeys)) {
				throw new IllegalArgumentException("Meta keys not found");
			}
		} else {
			if (!metaKeysIsSubset(userSelectedMetadata, existingMeta)) {
				throw new IllegalArgumentException("Meta filters not found");
			}
		}
	}

	/**
	 * Marks an existing prompt as no longer the latest version.
	 * Sets IS_LATEST flag to false for the specified prompt ID.
	 * This is called before inserting a new version of the prompt.
	 * 
	 * @param promptId The ID of the prompt to mark as not latest
	 */
	private static void updatePrompt(String promptId) {
		String[] colToUpdate = {"IS_LATEST"};
		String[] whereCol = {"ID"};
		String promptPermissionQuery = promptDb.getQueryUtil().createUpdatePreparedStatementString("PROMPT", colToUpdate, whereCol);

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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
		}
	}

	/**
	 * Updates prompt metadata by replacing all existing metadata entries.
	 * Deletes all existing PROMPTMETA entries for the prompt, then performs
	 * a bulk insert of the new tags and metadata values.
	 * 
	 * @param promptId The ID of the prompt to update metadata for
	 * @param userSelectedMeta Map of metadata keys to collections of values
	 * @param tags List of tag values to associate with the prompt
	 */
	public static void updatePromptTags(String promptId, Map<String, Collection<String>> userSelectedMeta, List<String> tags) {
		// first do a delete
		String deleteQ = "DELETE FROM PROMPTMETA WHERE PROMPT_ID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = promptDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, promptId);
			deletePs.execute();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, deletePs);
		}
		if(tags != null && !tags.isEmpty()) {
			insertTagsAndMeta(tags, userSelectedMeta, promptId);
		}
	}

	/**
	 * Queries PROMPTMETA table and appends tags and metadata to prompt details.
	 * Separates metadata entries into two categories:
	 * - Entries with METAKEY="tag" are added to a "tags" list
	 * - All other entries are added to a "metaKeys" map organized by metakey
	 * 
	 * Only processes metadata for prompts in the listIndexPromptMapping to avoid
	 * null pointer errors when metadata exists for filtered-out prompts.
	 * 
	 * @param promptDetails List of prompt detail maps to append metadata to
	 * @param listIndexPromptMapping Map of prompt IDs to their index in promptDetails list
	 * @param promptIdList List of prompt IDs to query metadata for
	 */
	private static void appendPromptTags(List<Map<String, Object>> promptDetails,
			Map<String, Integer> listIndexPromptMapping, List<String> promptIdList) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAORDER"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));

		if (promptIdList != null && !promptIdList.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__PROMPT_ID", "==", promptIdList));
		}
		qs.addOrderBy("PROMPTMETA__PROMPT_ID");
		qs.addOrderBy("PROMPTMETA__METAORDER");
		
		IQueryInterpreter interp = promptDb.getQueryInterpreter();
		interp.setQueryStruct(qs);
		System.out.println(interp.composeQuery());

		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(promptDb, qs);
		for(Map<String, Object> ret: retList) {
			String promptId = (String) ret.get("PROMPT_ID");
			String metaKey = (String) ret.get("METAKEY");
			String metaValue = (String) ret.get("METAVALUE");
			Integer loc = listIndexPromptMapping.get(promptId);
			// Skip if this prompt is not in our filtered result set
			if(loc == null) {
				continue;
			}
			
			Map<String, Object> promptDetail = promptDetails.get(loc);
			
			if("tag".equals(metaKey)) {
				// Handle tags
				List<String> tagList = (List<String>) promptDetail.get("tags");
				if(tagList == null) {
					tagList = new ArrayList<>();
					promptDetail.put("tags", tagList);
				}
				tagList.add(metaValue);
			} else {
				// Handle other metadata in metaKeys
				Map<String, List<String>> metaKeys = (Map<String, List<String>>) promptDetail.get("metaKeys");
				if(metaKeys == null) {
					metaKeys = new HashMap<>();
					promptDetail.put("metaKeys", metaKeys);
				}
				List<String> valueList = metaKeys.get(metaKey);
				if(valueList == null) {
					valueList = new ArrayList<>();
					metaKeys.put(metaKey, valueList);
				}
				valueList.add(metaValue);
			}
		}
	}

	/**
	 * Queries the PROMPT table and returns basic prompt information.
	 * Applies multiple layers of filtering:
	 * - User metadata filters (ensures user has permission to see the prompt)
	 * - Optional specific metadata filters
	 * - Optional generic filters
	 * - IS_LATEST = true (only returns current versions)
	 * - Optional limit and offset for pagination
	 * 
	 * @param user The user requesting prompts, used for metadata-based access control
	 * @param filters Optional additional filters to apply
	 * @param promptMetadataFilter Optional map of specific metadata key-value pairs to filter by
	 * @param limit Optional limit on number of results (as string)
	 * @param offset Optional offset for pagination (as string)
	 * @return List of maps containing prompt information (ID, TITLE, CONTEXT, etc.)
	 */
	private static List<Map<String, Object>> appendPromptInfo(User user, GenRowFilters filters, Map<String, Object> promptMetadataFilter, String limit, String offset) {
		// QUERY PROMPT get ID, TITLE, CONTEXT, IS Public, other small thigngs 
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pc : PROMPT_COLUMNS) {
			if(pc != "IS_LATEST") {
				qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc));
			}
		}

		if(promptMetadataFilter != null && !promptMetadataFilter.isEmpty()) {
			for(String k: promptMetadataFilter.keySet()) {
				SelectQueryStruct subMetaQs = new SelectQueryStruct();
				subMetaQs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));
				subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAKEY", "==", k));
				subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAVALUE", "==", promptMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROMPT__ID", "==", subMetaQs));
			}
		}
		
//		Add filters based on user metadata
		addUserMetaFiltersToQs(user, qs);
		
		// Add filter: GLOBAL = true OR CREATED_BY = userId
		qs.addExplicitFilter(createGlobalOrCreatedByFilter(user));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));

		if(filters != null && !filters.isEmpty()) {
			qs.mergeExplicitFilters(filters);
		}
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
			qs.setLimit(long_limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
			qs.setOffSet(long_offset);
		}
		
		IQueryInterpreter interp = promptDb.getQueryInterpreter();
		interp.setQueryStruct(qs);
		System.out.println(interp.composeQuery());

		List<Map<String, Object>> promptDetails = QueryExecutionUtility.flushRsToMap(promptDb, qs);
		return promptDetails;
	}

	/**
	 * Validates all prompt details before insertion.
	 * Checks required base details (title, context) and validates tags if present.
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
		for(String tag: tags) {
			if(tag == null || tag.isEmpty()) {
				throw new IllegalArgumentException("Tag must be string and not empty");
			}
		}
	}

	/**
	 * Validates the base required fields of a prompt.
	 * Ensures title and context are present and non-empty.
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
	 * Checks if the value is null (when not nullable) and if it's empty (when not allowed).
	 * 
	 * @param promptDetails Map containing the field to validate
	 * @param mapKey The key of the field to validate
	 * @param nullable Whether null values are allowed
	 * @param allowEmpty Whether empty strings are allowed
	 * @throws IllegalArgumentException if validation fails
	 */
	private static void validateString(Map<String, Object> promptDetails, String mapKey, boolean nullable, boolean allowEmpty) {
		String value = null;
		try {
			value = (String) promptDetails.get(mapKey);
			value = value != null ? value.trim(): value;
			if(value == null && !nullable) {
				throw new IllegalArgumentException(mapKey + " cannot be null when adding a new prompt.");
			}
			if(value != null && value.isEmpty() && !allowEmpty) {
				throw new IllegalArgumentException(mapKey + " cannot be null when adding a new prompt.");
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	/**
	 * Inserts tags and metadata entries into the PROMPTMETA table.
	 * First ensures all referenced metakeys exist in PROMPTMETAKEYS table.
	 * Tags are inserted with METAKEY="tag", other metadata uses the actual metakey.
	 * Each set of values maintains an order index (METAORDER).
	 * 
	 * @param tags List of tag values to insert (stored with METAKEY="tag")
	 * @param userSelectedMeta Map of metadata keys to collections of values
	 * @param promptId The ID of the prompt to associate metadata with
	 */
	private static void insertTagsAndMeta(List<String> tags, Map<String, Collection<String>> userSelectedMeta, String promptId) {
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
//			Now add for every meta value
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
		}
	}

	/**
	 * Ensures that a metakey exists in PROMPTMETAKEYS table.
	 * If it doesn't exist, copies it from security.USERMETAKEYS table.
	 * This allows prompts to use the same metadata structure as users.
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
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPTMETAKEYS__METAKEY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETAKEYS__METAKEY", "==", metaKey));
		
		try {
			List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(promptDb, qs);
			return !results.isEmpty();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
	}

	/**
	 * Copies a metakey entry from security.USERMETAKEYS to PROMPTMETAKEYS.
	 * Copies METAKEY, SINGLEMULTI, DISPLAYOPTIONS, and DEFAULTVALUES columns.
	 * Excludes DISPLAYORDER column during the copy.
	 * 
	 * @param metaKey The metakey to copy from USERMETAKEYS to PROMPTMETAKEYS
	 */
	private static void copyMetaKeyFromUserMetaKeys(String metaKey) {
		// Get the security database
		IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
		
		// Query USERMETAKEYS for the metakey
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__SINGLEMULTI"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYOPTIONS"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DEFAULTVALUES"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETAKEYS__METAKEY", "==", metaKey));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Inserts basic prompt details into the PROMPT table.
	 * Inserts: ID, TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED, IS_LATEST.
	 * Uses CLOB for CONTEXT field if database supports it, otherwise uses String.
	 * Automatically retrieves and increments version number for the prompt.
	 * 
	 * @param promptDetails Map containing prompt information (title, context, intent)
	 * @param userId The ID of the user creating/updating the prompt
	 * @param allowClob Whether the database supports CLOB objects for the context field
	 * @param promptId The UUID for the prompt (generated for new prompts, reused for updates)
	 * @throws IllegalArgumentException if insertion fails
	 */
	private static void insertPrompt(Map<String, Object> promptDetails, String userId, boolean allowClob, String promptId) {
		PreparedStatement promptPS = null;
		try {
			promptPS = promptDb.getPreparedStatement(promptQuery);
			int index = 1;
			promptPS.setString(index++, promptId);
			promptPS.setString(index++, String.valueOf(promptDetails.get("title")));
			if(allowClob) {
				Clob toclob = promptDb.getConnection().createClob();
				toclob.setString(1,  String.valueOf(promptDetails.get("context")));
				promptPS.setClob(index++, toclob);
			} else {
				promptPS.setString(index++, String.valueOf(promptDetails.get("context")));
			}
			// Get version of existing prompt
			Integer version = getVersionNumber(promptId);
			promptPS.setInt(index++, version);
			promptPS.setString(index++, String.valueOf(promptDetails.get("intent")));
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
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, null, promptPS, null);
		}
	}

	/**
	 * Retrieves the next version number for a prompt.
	 * Queries for the most recent version of the prompt by ID and DATE_CREATED,
	 * then increments it by 1. Returns 0 for new prompts with no existing versions.
	 * 
	 * @param promptId The ID of the prompt to get the version number for
	 * @return The next version number (existing version + 1, or 0 if no versions exist)
	 */
	private static Integer getVersionNumber(String promptId) {
		Integer version = 0;
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__VERSION"));
		qs.addSelector(new QueryColumnSelector("PROMPT__DATE_CREATED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptId));
		qs.addOrderBy("PROMPT__DATE_CREATED", "desc");
		qs.setLimit(1);

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(promptDb, qs);
			if(wrapper.hasNext()) {
				version = (Integer) wrapper.next().getValues()[0];
				version+=1;
				return version;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return version;
	}

	/**
	 * Deletes a prompt and all its associated metadata.
	 * Removes entries from PROMPT and PROMPTMETA tables.
	 * 
	 * @param promptId The ID of the prompt to delete
	 */
	public static void deletePrompt(String promptId) {
		List<String> deletes = new ArrayList<>();
		deletes.add("DELETE FROM PROMPT WHERE ID=?");
		deletes.add("DELETE FROM PROMPTMETA WHERE PROMPT_ID=?");

		for(String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = promptDb.getPreparedStatement(deleteQuery);
				ps.setString(1, promptId);
				ps.execute();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
			}
		}
	}

	/**
	 * Retrieves all available metadata values for specified metakeys along with usage counts.
	 * Returns METAKEY, METAVALUE, and a count of how many prompts use each value.
	 * Results are grouped by METAKEY and METAVALUE.
	 * 
	 * @param metaKeys List of metakeys to retrieve values for
	 * @return List of maps containing METAKEY, METAVALUE, and count for each metadata entry
	 */
	public static List<Map<String, Object>> getAvailableMetaValues(List<String> metaKeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE"));
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
	 * Retrieves a specific prompt by ID with user metadata filtering.
	 * Returns the prompt only if the user has permission to access it based on their metadata.
	 * The returned map includes:
	 * - Basic prompt information (ID, TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED)
	 * - tags: List of String values where METAKEY equals "tag"
	 * - metaKeys: Map<String, List<String>> containing all other metadata organized by metakey
	 * 
	 * @param promptID The ID of the prompt to retrieve
	 * @param user The user requesting the prompt, used for metadata-based access control
	 * @return Map containing prompt details, tags, and metadata
	 * @throws IndexOutOfBoundsException if no prompt is found or user lacks access
	 */
	public static Map<String, Object> getPrompt(String promptID, User user) {
		
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pc : PROMPT_COLUMNS) {
			if(pc != "IS_LATEST") {
				qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc));
			}
		}

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptID));

//		Add filters based on user metadata: Get user meta
		addUserMetaFiltersToQs(user, qs);
		
		// Add filter: GLOBAL = true OR CREATED_BY = userId
		qs.addExplicitFilter(createGlobalOrCreatedByFilter(user));

		Map<String, Object> promptDetails = QueryExecutionUtility.flushRsToMap(promptDb, qs).get(0);

		//Append Tags
		getPromptTags(promptID, promptDetails);
		return promptDetails;
	}

	/**
	 * Queries PROMPTMETA for a specific prompt and appends tags and metadata to the prompt details.
	 * Separates metadata entries into two categories:
	 * - Entries with METAKEY="tag" are added to a "tags" list
	 * - All other entries are added to a "metaKeys" map organized by metakey
	 * 
	 * @param promptID The ID of the prompt to retrieve metadata for
	 * @param promptDetails Map to append tags and metaKeys to
	 */
	private static void getPromptTags(String promptID, Map<String, Object> promptDetails) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAORDER"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__PROMPT_ID", "==", promptID));
		qs.addOrderBy("PROMPTMETA__PROMPT_ID");
		qs.addOrderBy("PROMPTMETA__METAORDER");
		
		List<String> tagList = new ArrayList<>();
		Map<String, List<String>> metaKeys = new HashMap<>();
		
		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(promptDb, qs);
		for(Map<String, Object> ret: retList) {
			String metaKey = (String) ret.get("METAKEY");
			String metaValue = (String) ret.get("METAVALUE");
			
			if("tag".equals(metaKey)) {
				// Handle tags
				tagList.add(metaValue);
			} else {
				// Handle other metadata in metaKeys
				List<String> valueList = metaKeys.get(metaKey);
				if(valueList == null) {
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
	 * Updates specific metadata fields for a prompt.
	 * Deletes existing entries for the specified metakeys, then inserts new values.
	 * Preserves other metadata fields not included in the update.
	 * Handles both single values and collections (List/Collection) for metadata values.
	 * 
	 * @param promptId The ID of the prompt to update metadata for
	 * @param metadata Map of metakeys to values (can be String, List, or Collection)
	 */
	public static void updatePromptMetadata(String promptId, Map<String, Object> metadata) {
		// first do a delete
		String deleteQ = "DELETE FROM PROMPTMETA WHERE METAKEY=? AND PROMPT_ID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = promptDb.getPreparedStatement(deleteQ);
			for(String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, promptId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, deletePs);
		}

		// now we do the new insert with the order of the tags
		String query = promptDb.getQueryUtil().createInsertPreparedStatementString("PROMPTMETA", new String[]{"PROMPT_ID", "METAKEY", "METAVALUE", "METAORDER"});
		PreparedStatement ps = null;
		try {
			ps = promptDb.getPreparedStatement(query);
			for(String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if(val instanceof List) {
					values = (List<Object>) val;
				} else if(val instanceof Collection) {
					values.addAll( (Collection<Object>) val);
				} else {
					values.add(val);
				}

				for(int i = 0; i < values.size(); i++) {
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
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(promptDb, ps);
		}
	}


}
