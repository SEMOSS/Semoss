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
package prerna.sablecc2.om.nounmeta;

import java.util.HashMap;
import java.util.Map;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class GuardrailNounMetadata extends NounMetadata {

	public static final String PASS_KEY = "pass";
	public static final String RETURN_PROMPT_KEY = "returnPrompt";
	public static final String FULL_DETAILS_KEY = "fullDetails";

	/** Default constructor for preset nouns */
	public GuardrailNounMetadata(boolean pass, String returnPrompt, Object details) {
		Map<String, Object> guardrailMap = new HashMap<>();
		guardrailMap.put(PASS_KEY, pass);
		guardrailMap.put(RETURN_PROMPT_KEY, returnPrompt);
		guardrailMap.put(FULL_DETAILS_KEY, details);
		this.value = guardrailMap;
		this.noun = PixelDataType.MAP;
		this.opType.add(PixelOperationType.GUARDRAIL);
	}

	/**
	 * @return
	 */
	public boolean isPass() {
		return (boolean) ((Map<String, Object>) this.value).get(PASS_KEY);
	}

	/**
	 * @return
	 */
	public String getReturnPrompt() {
		return (String) ((Map<String, Object>) this.value).get(RETURN_PROMPT_KEY);
	}

	/** */
	public Object getFullDetails() {
		return ((Map<String, Object>) this.value).get(FULL_DETAILS_KEY);
	}
}
