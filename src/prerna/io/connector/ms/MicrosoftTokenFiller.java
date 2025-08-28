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
package prerna.io.connector.ms;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class MicrosoftTokenFiller implements IAccessTokenFiller {

	public static final String MS_GRAPH_BASE_API = "https://graph.microsoft.com";
	private static final String USER_INFO_URL = MS_GRAPH_BASE_API + "/v1.0/me/";
	private static String[] beanProps = {"name", "id", "email"};
	private static String jsonPattern = "[displayName,id,mail]";

	@Override
	public void fillAccessToken(AccessToken msAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		if (userInfoUrl == null || (userInfoUrl = userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if (jsonPattern == null || (jsonPattern = jsonPattern.trim()).isEmpty()) {
			jsonPattern = MicrosoftTokenFiller.jsonPattern;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = MicrosoftTokenFiller.beanProps;
		}

		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = msAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, msAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
}
