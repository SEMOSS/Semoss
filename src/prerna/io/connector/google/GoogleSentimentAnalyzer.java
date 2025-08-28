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
package prerna.io.connector.google;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.SentimentAnalysis;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleSentimentAnalyzer implements IConnectorIOp {

	String url = "https://language.googleapis.com/v1/documents:analyzeSentiment";

	String[] beanProps = {"sentence", "magnitude", "score"};
	String jsonPattern = "sentences[].{sentence: text.content, magnitude: sentiment.magnitude, score:sentiment.score}";

	@Override
	public Object execute(User user, Map<String, Object> params) {
		// if no input, unsure what you will get...
		if (params == null) {
			params = new HashMap<>();
		}

		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
		String accessToken = googToken.getAccess_token();

		// make the API call
		String jsonString = HttpHelperUtility.makePostCall(url, accessToken, params, true);
		// System.out.println("Output >>>>> " + jsonString);

		// // fill the bean with the return
		Object returnObj = BeanFiller.fillFromJson(jsonString, jsonPattern, beanProps, new SentimentAnalysis());
		return returnObj;
	}
}
