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
package prerna.io.connector.okta;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class OktaTokenFiller implements IAccessTokenFiller {

	private static String jsonPattern = "[sub,name,email,phone_number]";
	private static String[] beanProps = {"id", "name", "email", "phone"};

	@Override
	public void fillAccessToken(AccessToken oktaAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		if (params == null) {
			params = new HashMap<>();
		}
		if (jsonPattern == null || (jsonPattern = jsonPattern.trim()).isEmpty()) {
			jsonPattern = OktaTokenFiller.jsonPattern;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = OktaTokenFiller.beanProps;
		}

		String accessToken = oktaAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, oktaAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
}
