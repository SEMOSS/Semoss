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
package prerna.reactor.task.lambda.map;

import java.util.List;
import java.util.Map;
import prerna.auth.User;
import prerna.engine.api.IHeadersDataRow;

public interface IMapLambda {

	/**
	 * Process one row and output the row again Cannot modify the headers
	 *
	 * @param row
	 * @return
	 */
	IHeadersDataRow process(IHeadersDataRow row);

	/**
	 * Modify the header information if necessary for the new transformation
	 *
	 * @return
	 */
	List<Map<String, Object>> getModifiedHeaderInfo();

	/**
	 * Initialize the transformation by defining the columns being used
	 *
	 * @param headerInfo
	 * @param columns
	 */
	void init(List<Map<String, Object>> headerInfo, List<String> columns);

	/** Set the user within the transformation */
	void setUser(User user);

	/** Sets other params to be utilized for twitter etc. */
	void setParams(Map params);
}
