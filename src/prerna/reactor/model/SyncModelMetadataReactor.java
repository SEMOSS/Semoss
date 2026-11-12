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
package prerna.reactor.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Re-run the meta/model.json backfill against models that are already in the
 * catalog. The same backfill runs for every model engine on startup, so this
 * exists for the cases a restart does not cover - a refreshed catalog file, or
 * models created before a column existed - without needing to bounce the
 * server.
 * <p>
 * With no engine ids this covers every model engine. That is the intended way
 * to run it: engines whose stored values already match are never written and
 * engines the catalog does not know are only reported, so a full pass is cheap
 * and can be repeated safely.
 */
public class SyncModelMetadataReactor extends AbstractReactor {

	private static final String FORCE_KEY = "force";
	private static final String DRY_RUN_KEY = "dryRun";

	public SyncModelMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), FORCE_KEY, DRY_RUN_KEY };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = requireUser();

		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		organizeKeys();

		List<String> engineIds = getEngineIds();
		boolean force = Boolean.parseBoolean(this.keyValue.get(FORCE_KEY) + "");
		boolean dryRun = Boolean.parseBoolean(this.keyValue.get(DRY_RUN_KEY) + "");

		List<Map<String, Object>> results = SecurityModelMetadataUtils.syncModelMetadataFromCatalog(engineIds, force,
				dryRun);

		Map<String, Object> retMap = new LinkedHashMap<>();
		retMap.put("total", results.size());
		retMap.put("force", force);
		retMap.put("dryRun", dryRun);
		for (String status : new String[] { "UPDATED", "WOULD_UPDATE", "NO_CHANGE", "NO_CATALOG_ENTRY", "NO_MODEL_ID",
				"ERROR" }) {
			retMap.put(status, countByStatus(results, status));
		}
		retMap.put("results", results);
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

	/**
	 * Engines to sync, empty for all of them.
	 */
	private List<String> getEngineIds() {
		List<String> engineIds = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.ENGINE.getKey());
		if (grs != null && !grs.isEmpty()) {
			engineIds.addAll(grs.getAllStrValues());
		} else if (this.curRow != null && !this.curRow.isEmpty()) {
			engineIds.addAll(this.curRow.getAllStrValues());
		}
		return engineIds;
	}

	private static int countByStatus(List<Map<String, Object>> results, String status) {
		int count = 0;
		for (Map<String, Object> result : results) {
			if (status.equals(result.get("status"))) {
				count++;
			}
		}
		return count;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Admin Only: Backfill the model metadata stored for model engines from the meta/model.json catalog")
				.key(ReactorKeysEnum.ENGINE,
						"Optional list of model engine ids to sync. Defaults to every model engine")
				.key(FORCE_KEY,
						"Overwrite values that are already stored instead of only filling in empty ones. Default false")
				.key(DRY_RUN_KEY, "Report what would change without writing anything. Default false");
	}
}
