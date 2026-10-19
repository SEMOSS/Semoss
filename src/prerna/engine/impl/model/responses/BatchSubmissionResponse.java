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
 * Result of submitting a batch to a provider. Holds the SEMOSS-normalized fields
 * plus the raw provider object (used by the compatible REST endpoints to echo the
 * provider wire shape back to the SDK client).
 */
public class BatchSubmissionResponse {

	private String providerBatchId;
	private String status;
	private Integer requestCount;
	private Map<String, Object> raw;

	public String getProviderBatchId() {
		return providerBatchId;
	}

	/**
	 * Lets a caller rewrite the id after the fact -- e.g. ModelBatchManager
	 * prepending a storage-engine locator onto a Vertex job name once it has
	 * uploaded the batch's input file through it.
	 */
	public void setProviderBatchId(String providerBatchId) {
		this.providerBatchId = providerBatchId;
	}

	public String getStatus() {
		return status;
	}

	public Integer getRequestCount() {
		return requestCount;
	}

	public Map<String, Object> getRaw() {
		return raw;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> out = new HashMap<>();
		out.put("batchId", providerBatchId);
		out.put("status", status);
		if (requestCount != null) {
			out.put("requestCount", requestCount);
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	public static BatchSubmissionResponse fromObject(Object responseObject) {
		Map<String, Object> map = BatchModelEngineResponseUtil.asMap(responseObject);
		BatchSubmissionResponse r = new BatchSubmissionResponse();
		r.providerBatchId = BatchModelEngineResponseUtil.getString(map, "provider_batch_id");
		r.status = BatchModelEngineResponseUtil.getString(map, "status");
		r.requestCount = BatchModelEngineResponseUtil.getInteger(map.get("request_count"));
		Object rawObj = map.get("raw");
		if (rawObj instanceof Map) {
			r.raw = (Map<String, Object>) rawObj;
		}
		return r;
	}
}
