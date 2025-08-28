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

import java.util.HashMap;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class GitBaseReactor extends AbstractReactor {

	public String getToken() {
		User user = insight.getUser();
		String gitProvider = Utility.getDIHelperProperty(Constants.GIT_PROVIDER);
		AccessToken gitAccess = null;
		if (gitProvider != null && !(gitProvider.isEmpty())
				&& gitProvider.toLowerCase().equals(AuthProvider.GITLAB.toString().toLowerCase())) {
			gitAccess = user.getAccessToken(AuthProvider.GITLAB);
		} else {
			gitAccess = user.getAccessToken(AuthProvider.GITHUB);
		}

		if (gitAccess == null) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "git");
			retMap.put("message", "Please login to your Git account");
			throwLoginError(retMap);
		}

		return gitAccess.getAccess_token();
	}
}
