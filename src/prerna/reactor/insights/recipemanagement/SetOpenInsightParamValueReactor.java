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
package prerna.reactor.insights.recipemanagement;

import java.util.Map;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetOpenInsightParamValueReactor extends AbstractInsightParameterReactor {

	private static final String PARAM_MAP = "paramMap";

	public SetOpenInsightParamValueReactor() {
		this.keysToGet = new String[]{PARAM_MAP};
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> paramValues = null;
		GenRowStruct grs = this.store.getNoun(PARAM_MAP);
		if (grs != null && !grs.isEmpty()) {
			paramValues = (Map<String, Object>) grs.get(0);
		}

		if (paramValues == null) {
			if (!this.curRow.isEmpty()) {
				paramValues = (Map<String, Object>) this.curRow.get(0);
			}
		}

		return new NounMetadata(paramValues, PixelDataType.PARAM_VALUES_MAP);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PARAM_MAP)) {
			return "The input map of param name to param value";
		}
		return super.getDescriptionForKey(key);
	}
}
