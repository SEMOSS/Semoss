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

import java.util.HashMap;
import java.util.Map;

/**
 * Live status of a batch as reported by the provider.
 */
public class BatchStatusResponse {

	private String providerBatchId;
	private String status;
	private Map<String, Object> counts;
	private Map<String, Object> raw;

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

	public Map<String, Object> getCounts() {
		return counts;
	}

	public Map<String, Object> getRaw() {
		return raw;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> out = new HashMap<>();
		out.put("batchId", providerBatchId);
		out.put("status", status);
		if (counts != null) {
			out.put("counts", counts);
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	public static BatchStatusResponse fromObject(Object responseObject) {
		Map<String, Object> map = BatchModelEngineResponseUtil.asMap(responseObject);
		BatchStatusResponse r = new BatchStatusResponse();
		r.providerBatchId = BatchModelEngineResponseUtil.getString(map, "provider_batch_id");
		r.status = BatchModelEngineResponseUtil.getString(map, "status");
		Object countsObj = map.get("counts");
		if (countsObj instanceof Map) {
			r.counts = (Map<String, Object>) countsObj;
		}
		Object rawObj = map.get("raw");
		if (rawObj instanceof Map) {
			r.raw = (Map<String, Object>) rawObj;
		}
		return r;
	}
}
