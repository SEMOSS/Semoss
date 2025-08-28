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
package prerna.reactor.legacy.playsheets;

import java.util.Hashtable;
import java.util.Map;
import prerna.om.OldInsight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RunPlaysheetMethodReactor extends AbstractReactor {

	public RunPlaysheetMethodReactor() {
		this.keysToGet = new String[]{"method", ReactorKeysEnum.PARAM_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String method = getMethod();
		Map<String, Object> hash = getParamMap();
		if (this.insight instanceof OldInsight) {
			Object ret = ((OldInsight) this.insight).getPlaySheet().doMethod(method, hash);
			return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
		} else {
			throw new IllegalArgumentException("This is a legacy pixel that should only be used for old insights");
		}
	}

	/**
	 * Get the method name to invoke via reflection
	 *
	 * @return
	 */
	private String getMethod() {
		GenRowStruct mapGrs = this.store.getNoun(this.keysToGet[0]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			return (String) mapGrs.get(0);
		}

		if (!curRow.isEmpty()) {
			return (String) curRow.get(0);
		}

		throw new IllegalArgumentException("Need to pass in method name");
	}

	/**
	 * Get the params for the method
	 *
	 * @return
	 */
	private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getNoun(this.keysToGet[1]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			return (Map<String, Object>) mapGrs.get(0);
		}

		if (!curRow.isEmpty()) {
			return (Map<String, Object>) curRow.get(1);
		}

		return new Hashtable<String, Object>();
	}
}
