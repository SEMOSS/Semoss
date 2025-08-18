package prerna.reactor.browser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PressKeyReactor extends AbstractReactor {

	private final static String REACTOR_DESCRIPTION = "Key to press on the Browser App rendered on the server.";
	private final static String INPUT_KEY_DESCRIPTION = "The the key to press of the browser app rendered on the server.";
	
	public PressKeyReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.INPUT.getKey()};
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
		
		String input = this.keyValue.get(this.keysToGet[0]);
		
		if (input == null || input.isEmpty()) {
			throw new IllegalArgumentException("Input could not be null or empty");
		}
		
		input = input.trim();
		
		// Ideally, previous call would have been ClickXY and the form to be input would already
		// Be selected. That way, the next action would be. FillInputForm.
		
		Map<String, Object> actions = new HashMap<>();
		
		List<String> inputs = new ArrayList<>();
		inputs.add(input);
		
		actions.put("actor", "system");
		actions.put("action", "keypress");
		actions.put("event", "press");
		actions.put("params", inputs);
		
		String json = BrowserUtils.mapToJsonString(actions);
		
		JSONObject jo = new JSONObject(json);
		PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
		if (pbu == null) {
			throw new IllegalArgumentException("There is no Playwright Browser currently open for this insight.");
		}	
		pbu.keyboardPress(input);
		// pbu.fill(jo);
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.INPUT.getKey())) {
			return INPUT_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
