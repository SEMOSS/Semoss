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
package prerna.engine.impl.model.responses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The full set of per-request results for a completed batch.
 */
public class BatchResultsResponse {

	private String providerBatchId;
	private String status;
	private Integer count;
	private List<BatchResultItem> items = new ArrayList<>();
	private String rawJsonl;

	public String getProviderBatchId() {
		return providerBatchId;
	}

	/**
	 * Lets a caller rewrite the id after the fact -- e.g. ModelBatchManager
	 * restoring the caller-facing composite Vertex batch id after asking the
	 * engine with just the decoded bare job id.
	 */
	public void setProviderBatchId(String providerBatchId) {
		this.providerBatchId = providerBatchId;
	}

	public String getStatus() {
		return status;
	}

	public Integer getCount() {
		return count;
	}

	public List<BatchResultItem> getItems() {
		return items;
	}

	public String getRawJsonl() {
		return rawJsonl;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> out = new HashMap<>();
		out.put("batchId", providerBatchId);
		out.put("status", status);
		out.put("count", count);
		List<Map<String, Object>> itemMaps = new ArrayList<>();
		for (BatchResultItem item : items) {
			itemMaps.add(item.toMap());
		}
		out.put("results", itemMaps);
		return out;
	}

	@SuppressWarnings("unchecked")
	public static BatchResultsResponse fromObject(Object responseObject) {
		Map<String, Object> map = BatchModelEngineResponseUtil.asMap(responseObject);
		BatchResultsResponse r = new BatchResultsResponse();
		r.providerBatchId = BatchModelEngineResponseUtil.getString(map, "provider_batch_id");
		r.status = BatchModelEngineResponseUtil.getString(map, "status");
		r.count = BatchModelEngineResponseUtil.getInteger(map.get("count"));
		r.rawJsonl = BatchModelEngineResponseUtil.getString(map, "raw_jsonl");
		Object resultsObj = map.get("results");
		if (resultsObj instanceof List) {
			for (Object itemObj : (List<Object>) resultsObj) {
				if (itemObj instanceof Map) {
					r.items.add(BatchResultItem.fromMap((Map<String, Object>) itemObj));
				}
			}
		}
		return r;
	}
}
