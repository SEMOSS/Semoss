/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model.responses;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractModelEngineResponse<T> implements Serializable {

	/** */
	private static final long serialVersionUID = -5721069401496221919L;

	public static final String RESPONSE = "response";
	public static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	public static final String NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse";
	public static final String USAGE_RESTRICTION_KEY = "usageRestriction";
	public static final String USAGE_RESTRICTION_MODE = "restrictedBy";
	public static final String USAGE_RESTRICTION_CURRENT_VALUE = "currentValue";
	public static final String USAGE_RESTRICTION_MAX_VALUE = "maxValue";

	protected T response;
	protected Integer numberOfTokensInPrompt;
	protected Integer numberOfTokensInResponse;
	protected Map<String, Object> usageRestriction = null;

	public AbstractModelEngineResponse(T response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
		this.response = response;
		this.numberOfTokensInPrompt = numberOfTokensInPrompt;
		this.numberOfTokensInResponse = numberOfTokensInResponse;
	}

	public T getResponse() {
		return response;
	}

	public void setResponse(T response) {
		this.response = response;
	}

	public Integer getNumberOfTokensInPrompt() {
		return numberOfTokensInPrompt;
	}

	public void setNumberOfTokensInPrompt(Integer numberOfTokensInPrompt) {
		this.numberOfTokensInPrompt = numberOfTokensInPrompt;
	}

	public Integer getNumberOfTokensInResponse() {
		return numberOfTokensInResponse;
	}

	public void setNumberOfTokensInResponse(Integer numberOfTokensInResponse) {
		this.numberOfTokensInResponse = numberOfTokensInResponse;
	}

	public Map<String, Object> getUsageRestriction() {
		return usageRestriction;
	}

	public void setUsageRestriction(Map<String, Object> usageRestriction) {
		this.usageRestriction = usageRestriction;
	}

	public Map<String, Object> toMap() {
		Map<String, Object> responseMap = new HashMap<>();
		responseMap.put(RESPONSE, this.response);
		responseMap.put(NUMBER_OF_TOKENS_IN_PROMPT, this.numberOfTokensInPrompt);
		responseMap.put(NUMBER_OF_TOKENS_IN_RESPONSE, this.numberOfTokensInResponse);
		if (this.usageRestriction != null) {
			responseMap.put(USAGE_RESTRICTION_KEY, this.usageRestriction);
		}
		return responseMap;
	}

	protected static Integer getTokens(Object numTokens) {
		if (numTokens instanceof Integer) {
			return (Integer) numTokens;
		} else if (numTokens instanceof Long) {
			return ((Long) numTokens).intValue();
		} else if (numTokens instanceof Double) {
			return ((Double) numTokens).intValue();
		} else if (numTokens instanceof Number) {
			return ((Number) numTokens).intValue();
		} else if (numTokens instanceof String) {
			return Integer.valueOf((String) numTokens);
		} else {
			return null;
		}
	}
}
