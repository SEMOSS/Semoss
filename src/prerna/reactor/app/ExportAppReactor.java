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
package prerna.reactor.app;

import prerna.reactor.utils.ExportProjectAppReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

public class ExportAppReactor extends ExportProjectAppReactor {

	/**
	 * @param projectNameAndId
	 * @return
	 */
	@Override
	protected String getFileName(String projectNameAndId) {
		return projectNameAndId + "_app.smss-app";
	}

	@Override
	public String getReactorDescription() {
		return "Export an app as a single .smss-app file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is a required value containing the id of the app that is being exported";
		}
		return super.getDescriptionForKey(key);
	}
}
