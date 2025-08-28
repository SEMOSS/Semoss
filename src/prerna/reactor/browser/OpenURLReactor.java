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
package prerna.reactor.browser;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenURLReactor extends AbstractReactor {

	private static final String REACTOR_DESCRIPTION = "Open the URL of the Browser App rendered on the server.";
	private static final String URL_KEY_DESCRIPTION = "A URL address to open on the Browser App rendered on the server.";

	public OpenURLReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey()};
		this.keyRequired = new int[]{1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		BrowserUtils.ensureUserLoggedIn(user);

		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}

		String url = this.keyValue.get(this.keysToGet[0]);

		String domain = null;
		try {
			URI uri = new URI(url);
			if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
				throw new IllegalArgumentException("URL is not http or https.");
			}
			domain = uri.getHost();
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("URL is improperly formatted.", e);
		}

		Map<String, Object> actions = new HashMap<>();
		actions.put("actor", "system");
		actions.put("action", "navigate");
		actions.put("website", url);

		String json = BrowserUtils.mapToJsonString(actions);

		JSONObject jo = new JSONObject(json);

		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			pbu = new PlaywrightBrowserUtil();
			this.insight.setPlaywrightUtil(pbu);
		}

		pbu.open(jo);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.URL.getKey())) {
			return URL_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
