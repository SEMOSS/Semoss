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
package prerna.reactor;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EchoReactor extends AbstractReactor {

	public EchoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			return grs.getNoun(0);
		}
		grs = this.store.getNoun(ALL_NOUN_STORE);
		if (grs != null && !grs.isEmpty()) {
			return grs.getNoun(0);
		}

		return new NounMetadata("", PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VALUE.getKey())) {
			return "The value to echo back";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns back the input the user enters";
	}
}
