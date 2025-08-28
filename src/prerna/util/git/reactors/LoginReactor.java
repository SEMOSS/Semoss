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
package prerna.util.git.reactors;

import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GitHub;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitUtils;

public class LoginReactor extends AbstractReactor {

	public LoginReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Logging In...");
		String username = this.keyValue.get(this.keysToGet[0]);
		String password = this.keyValue.get(this.keysToGet[1]);
		GitHub ret = GitUtils.login(username, password);
		if (ret == null) {
			throw new IllegalArgumentException("Could not properly login using credentials");
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}
}
