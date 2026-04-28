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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * @deprecated use {@link GetWorkspaceReactor}
 */
@Deprecated
public class GetWorkspaceKnowledgeBaseReactor extends AbstractReactor {

	@Deprecated
	public GetWorkspaceKnowledgeBaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Deprecated
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		Map<String, Object> paramMap = getMap();

		User user = this.insight.getUser();

		Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
		if (current == null) {
			throw new IllegalArgumentException("Workspace not found");
		}

		Object currentlyIsActive = current.get("is_active");
		Boolean currentlyActive = (Boolean) currentlyIsActive;

		if (!currentlyActive) {
			throw new IllegalArgumentException("Workspace is disabled by the owner");
		}
		if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}

		List<Map<String, Object>> knowledgeBase = new ArrayList<>();
		List<Map<String, Object>> workspaceKnowledgeEntries = ModelInferenceLogsUtils
				.getWorkspaceResourcesByType(workspaceId, List.of(IEngine.CATALOG_TYPE.VECTOR.toString()));
		for (Map<String, Object> workspaceKnowledgeEntry : workspaceKnowledgeEntries) {
			String knowledgeId = (String) workspaceKnowledgeEntry.get("resource_id");
			if (knowledgeId == null) {
				continue;
			}

			IVectorDatabaseEngine engine = Utility.getVectorDatabase(knowledgeId);
			if (engine == null) {
				continue;
			}

			knowledgeBase.addAll(engine.listDocuments(paramMap));
		}
		return new NounMetadata(knowledgeBase, PixelDataType.MAP);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
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
		return new HashMap<>();
	}
}
