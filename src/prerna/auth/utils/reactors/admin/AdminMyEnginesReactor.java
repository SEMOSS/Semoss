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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserCatalogVoteUtils;

public class AdminMyEnginesReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminMyEnginesReactor.class);

	private static final String PERMISSION_USER = "permissionUser";
	private static final String EFFECTIVE_PERMISSIONS = "effectivePermissions";
	private static final String CREATED_BY = "createdBy";
	private static final String CREATED_BY_ME = "createdByMe";

	public AdminMyEnginesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.ENGINE_TYPE.getKey(), ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
				ReactorKeysEnum.NO_META.getKey(), ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey(),
				ReactorKeysEnum.SORT.getKey(), PERMISSION_USER, EFFECTIVE_PERMISSIONS, CREATED_BY, CREATED_BY_ME };
	}

	@Override
	public NounMetadata execute() {
		// add creator, upvotes, total views
		// sort by name, date created, views, upvotes, trending
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();

		String searchTerm = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);
		List<String> engineTypes = getEngineTypeFilters();
		List<String> engineIdFilters = getEngineIdFilters();
		Map<String, String> sortFields = getMap(ReactorKeysEnum.SORT.getKey());
		Boolean noMeta = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.NO_META.getKey()));
		Boolean includeUserT = Boolean
				.parseBoolean(this.keyValue.get(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey()));
		Map<String, Object> engineMetadataFilter = getMetaMap();
		String permissionUser = getString(PERMISSION_USER);
		List<Integer> effectivePermissions = getListInteger(EFFECTIVE_PERMISSIONS);
		GenRowStruct createdByGrs = getGenRowStruct(CREATED_BY);
		List<Pair<String, String>> createdBy = AbstractSecurityUtils
				.getCreatorPairs(createdByGrs == null ? null : createdByGrs.getAllValues());
		if (getBoolean(CREATED_BY_ME, false)) {
			createdBy.addAll(User.getUserIdAndType(user));
		}

		List<Map<String, Object>> engineInfo = adminUtils.getAllEngineSettings(engineIdFilters, engineTypes,
				engineMetadataFilter, searchTerm, limit, offset, sortFields, permissionUser, effectivePermissions,
				createdBy);

		if (!engineInfo.isEmpty() && (!noMeta || includeUserT)) {
			Map<String, Integer> index = new HashMap<>(engineInfo.size());
			int size = engineInfo.size();
			for (int i = 0; i < size; i++) {
				Map<String, Object> engine = engineInfo.get(i);
				String engineId = engine.get("database_id").toString();
				// keep list of database ids to get the index
				index.put(engineId, Integer.valueOf(i));
			}

			if (!noMeta) {
				try (IRawSelectWrapper wrapper = SecurityEngineUtils.getEngineMetadataWrapper(index.keySet(),
						getMetaKeys(), true)) {
					while (wrapper.hasNext()) {
						Object[] data = wrapper.next().getValues();
						String databaseId = (String) data[0];

						String metaKey = (String) data[1];
						String metaValue = (String) data[2];
						if (metaValue == null) {
							continue;
						}

						int indexToFind = index.get(databaseId);
						Map<String, Object> res = engineInfo.get(indexToFind);
						// whatever it is, if it is single send a single value, if it is multi send as
						// array
						if (res.containsKey(metaKey)) {
							Object obj = res.get(metaKey);
							if (obj instanceof List) {
								((List) obj).add(metaValue);
							} else {
								List<Object> newList = new ArrayList<>();
								newList.add(obj);
								newList.add(metaValue);
								res.put(metaKey, newList);
							}
						} else {
							res.put(metaKey, metaValue);
						}
					}
				} catch (Exception e) {
					classLogger.error("Failed to attach engine metadata values to the admin engine list response", e);
				}
			}
			if (includeUserT) {
				try (IRawSelectWrapper wrapper = UserCatalogVoteUtils.getAllVotesWrapper(index.keySet())) {
					while (wrapper.hasNext()) {
						Object[] data = wrapper.next().getValues();
						String databaseId = (String) data[0];
						int upvotes = ((Number) data[1]).intValue();

						int indexToFind = index.get(databaseId);
						Map<String, Object> res = engineInfo.get(indexToFind);
						res.put("upvotes", upvotes);
					}
				} catch (Exception e) {
					classLogger.error("Failed to attach vote totals to the admin engine list response", e);
				}

				Map<String, Boolean> voted = UserCatalogVoteUtils
						.userEngineVotes(User.getUserIdAndType(this.insight.getUser()), index.keySet());
				for (String ks : index.keySet()) {
					int indexToFind = index.get(ks);
					Boolean hasUpvoted = voted.get(ks);
					if (hasUpvoted == null) {
						hasUpvoted = false;
					}
					engineInfo.get(indexToFind).put("hasUpvoted", hasUpvoted);
				}
			}
		}

		return new NounMetadata(engineInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

	/**
	 * 
	 * @return
	 */
	private List<String> getEngineIdFilters() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.ENGINE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

	/**
	 * 
	 * @return
	 */
	private List<String> getEngineTypeFilters() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.ENGINE_TYPE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

	/**
	 * 
	 * @return
	 */
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.META_FILTERS.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Admin Only: Returns every engine on the server, filtered by id, type, search text, metadata, creator or a user's permission level, and sorted by name unless sort says otherwise. Each engine carries its metadata unless noMeta is true, and its upvote count and whether the caller upvoted it when userT is true.")
				.key(ReactorKeysEnum.ENGINE_TYPE, key -> key.list(String.class))
				.key(ReactorKeysEnum.LIMIT, key -> key.type(Integer.class))
				.key(ReactorKeysEnum.META_FILTERS, key -> key.type(Map.class))
				.key(ReactorKeysEnum.META_KEYS, key -> key.list(String.class))
				.key(ReactorKeysEnum.NO_META, key -> key.type(Boolean.class))
				.key(ReactorKeysEnum.OFFSET, key -> key.type(Integer.class))
				.key(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY, key -> key.type(Boolean.class))
				.key(ReactorKeysEnum.ENGINE,
						key -> key.list(String.class).description("This is an optional engine filter"))
				.key(ReactorKeysEnum.SORT, key -> key.type(Map.class).description(
						"The sort is a map with key and direction. Supported keys are 'ENGINENAME' and 'DATECREATED'. Use values like 'ASC' or 'DESC'. 'ENGINENAME' sorting is case-insensitive."))
				.key(PERMISSION_USER, "The id of the user whose permission effectivePermissions checks.")
				.key(EFFECTIVE_PERMISSIONS, key -> key.list(Integer.class).description(
						"""
								Keep only the resources on which permissionUser holds one of these levels: 1 (owner), 2 (edit), \
								or 3 (read only). Counts that user's direct grants and their custom groups' grants; identity \
								provider groups are known only while the user is signed in, so they are not counted."""))
				.key(CREATED_BY, key -> key.list(Map.class).description("""
						Keep only the resources created by one of these creators. Each is a map with the creator's \
						login "id" and login "type", such as {"id": "abc", "type": "MICROSOFT"}; both must match."""))
				.key(CREATED_BY_ME, key -> key.type(Boolean.class).description(
						"When true, keep only the resources the calling user created, under any login in their session."));
	}
}
