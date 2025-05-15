package prerna.reactor.browser;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenURLReactor extends AbstractReactor {
	
	private final static String REACTOR_DESCRIPTION = "Open the URL of the Browser App rendered on the server.";
	private final static String URL_KEY_DESCRIPTION = "A URL address to open on the Browser App rendered on the server.";
	
	public OpenURLReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.URL.getKey()};
		this.keyRequired = new int[] { 1 };
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
