package prerna.reactor.browser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.reactor.browser.PlaywrightBrowserUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class HoverXYReactor extends AbstractReactor {

	private final static String REACTOR_DESCRIPTION = "Hover over the x, y coordinate of the Browser App rendered on the server.";
	private final static String X_KEY_DESCRIPTION = "The X coordiante of the Browser App rendered on the server.";
	private final static String Y_KEY_DESCRIPTION = "The Y coordinate of the Browser App rendered on the server.";

	public HoverXYReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.X.getKey(), ReactorKeysEnum.Y.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		BrowserUtils.ensureUserLoggedIn(user);

		if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
			throwAnonymousUserError();
		}

		int x = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
		int y = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));

		// We would map the x,y coordinate that hover in the UI to the browser being
		// rendered locally.
		Map<String, Object> actions = new HashMap<>();
		actions.put("actor", "system");
		actions.put("action", "hoverXY");
		actions.put("event", "hover");
		
		List<Integer> params = new ArrayList<>();
		params.add(x);
		params.add(y);
		actions.put("params", params);
		
		
		String json = BrowserUtils.mapToJsonString(actions);
		
		JSONObject jo = new JSONObject(json);
		
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			throw new IllegalArgumentException("There is no Playwright Browser currently open for this insight.");
		}
		pbu.mouse_xy(jo, "hoverXY");
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.X.getKey())) {
			return X_KEY_DESCRIPTION;
		} else if (key.equals(ReactorKeysEnum.Y.getKey())) {
			return Y_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
