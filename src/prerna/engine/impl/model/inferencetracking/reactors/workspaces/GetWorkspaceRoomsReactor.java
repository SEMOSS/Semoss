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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetWorkspaceRoomsReactor extends AbstractReactor {

	public GetWorkspaceRoomsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.WORKSPACE_ID.getKey(), ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.SORT.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		User user = this.insight.getUser();
		long limit = getLong(ReactorKeysEnum.LIMIT.getKey(), -1L);
		long offset = getLong(ReactorKeysEnum.OFFSET.getKey(), -1L);
		GenRowFilters filters = getFilters();
		List<IQuerySort> sorts = getSorts();

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

		Map<String, Object> rooms = ModelInferenceLogsUtils.getWorkspaceRoomsForUser(workspaceId, user, limit, offset,
				filters, sorts);
		return new NounMetadata(rooms, PixelDataType.MAP);
	}

	private GenRowFilters getFilters() {
		GenRowFilters grf = new GenRowFilters();
		GenRowStruct grs = this.getNounStore().getGenRowStruct(ReactorKeysEnum.FILTERS.getKey());
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				Object val = grs.get(i);
				SelectQueryStruct qs = (SelectQueryStruct) val;
				if (qs != null) {
					grf.merge(qs.getCombinedFilters());
				}
			}
		}

		return grf;
	}

	private List<IQuerySort> getSorts() {
		GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.SORT.getKey());
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			NounMetadata sortNoun = inputsGRS.getNoun(0);
			SelectQueryStruct qs = (SelectQueryStruct) sortNoun.getValue();
			List<IQuerySort> orderBy = qs.getOrderBy();
			return orderBy;
		}
		return null;
	}

}
