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
package prerna.cluster.util.clients;

import java.util.Map;
import prerna.util.Utility;

public class AppCloudClientProperties {

	private Map<String, String> env = null;

	public AppCloudClientProperties() {
		this.env = System.getenv();
	}

	/**
	 * This method is used to first try and pull the value from the env if it is not
	 * found or is empty then try to pull from DIHelper else return null
	 *
	 * @param key
	 * @return
	 */
	public String get(String key) {
		String val = this.env.get(key);
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		// give benefit of the doubt..
		val = this.env.get(key.toUpperCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		val = this.env.get(key.toLowerCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}

		val = Utility.getDIHelperProperty(key);
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		// give benefit of the doubt..
		val = Utility.getDIHelperProperty(key.toUpperCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}
		val = Utility.getDIHelperProperty(key.toLowerCase());
		if (val != null && !(val = val.trim()).isEmpty()) {
			return val;
		}

		// no luck...
		return null;
	}
}
