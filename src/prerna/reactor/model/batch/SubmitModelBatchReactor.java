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
package prerna.reactor.model.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.batch.ModelBatchManager;
import prerna.engine.impl.model.responses.BatchSubmissionResponse;
import prerna.om.ThreadStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Submit a batch of model requests to the provider's native batch API.
 * Returns the provider batch id and initial status.
 */
public class SubmitModelBatchReactor extends AbstractModelBatchReactor {

	public SubmitModelBatchReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.BATCH_REQUESTS.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		List<Map<String, Object>> requests = getRequests();
		if (requests.isEmpty()) {
			throw new IllegalArgumentException("At least one request is required to submit a batch");
		}

		Map<String, Object> params = baseParams();

		IModelEngine engine = ModelBatchManager.resolveEngine(getUser(), engineId);
		BatchSubmissionResponse response = engine.submitBatch(requests, params);
		if (response.getProviderBatchId() != null) {
			ModelBatchManager.recordBatchSubmission(getUser(), engine, response.getProviderBatchId(), requests,
					this.insight.getInsightId(), ThreadStore.getSessionId());
		}
		return new NounMetadata(response.toMap(), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getRequests() {
		List<Map<String, Object>> requests = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.BATCH_REQUESTS.getKey());
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				addRequest(requests, grs.get(i));
			}
		}
		return requests;
	}

	@SuppressWarnings("unchecked")
	private void addRequest(List<Map<String, Object>> requests, Object val) {
		if (val == null) {
			return;
		}
		if (val instanceof Map) {
			requests.add((Map<String, Object>) val);
		} else if (val instanceof List) {
			for (Object elem : (List<Object>) val) {
				addRequest(requests, elem);
			}
		} else if (val instanceof String) {
			String s = ((String) val).trim();
			if (s.isEmpty()) {
				return;
			}
			if (s.startsWith("[")) {
				List<Map<String, Object>> parsed = GSON.fromJson(s,
						new TypeToken<List<Map<String, Object>>>() {
						}.getType());
				if (parsed != null) {
					requests.addAll(parsed);
				}
			} else {
				Map<String, Object> parsed = GSON.fromJson(s, new TypeToken<Map<String, Object>>() {
				}.getType());
				if (parsed != null) {
					requests.add(parsed);
				}
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Submit a batch of model requests to the provider's native batch API";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.BATCH_REQUESTS.getKey())) {
			return "List of requests; each entry is a map with 'custom_id' and 'body' (the provider-native per-request payload). May also be a JSON string.";
		}
		return super.getDescriptionForKey(key);
	}
}
