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
package prerna.reactor.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEngineMetaValuesReactor extends AbstractReactor {

	public GetEngineMetaValuesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE_TYPE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		List<String> eTypes = getListValues(ReactorKeysEnum.ENGINE_TYPE.getKey());
		List<String> engineList = SecurityEngineUtils.getUserEngineIdList(this.insight.getUser(), eTypes, true, false,
				true);
		if (engineList != null && engineList.isEmpty()) {
			return new NounMetadata(new ArrayList<>(), PixelDataType.CUSTOM_DATA_STRUCTURE);
		}
		List<Map<String, Object>> ret = SecurityEngineUtils.getAvailableMetaValues(engineList,
				getListValues(ReactorKeysEnum.META_KEYS.getKey()));
		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private List<String> getListValues(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return this.curRow.getAllStrValues();
	}
}
