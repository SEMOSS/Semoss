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

import java.util.List;
import java.util.Map;
import prerna.auth.utils.SecurityUserUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserMetakeyOptionsReactor extends AbstractReactor {

	public GetUserMetakeyOptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		List<Map<String, Object>> ret = SecurityUserUtils.getMetakeyOptions(getMetaKeys());
		NounMetadata noun = new NounMetadata(ret, PixelDataType.PIXEL_OBJECT);
		return noun;
	}

	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve information about possible metadata keys applicable to users";
	}
}
