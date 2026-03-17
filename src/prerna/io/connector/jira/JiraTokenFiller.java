package prerna.io.connector.jira;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class JiraTokenFiller implements IAccessTokenFiller {

	private static final String USER_INFO_URL = "https://api.atlassian.com/me";

	private static final String[] BEAN_PROPS = { "name", "email", "locale" };
	private static final String JSON_PATTERN = "[name, email, locale]";

	@Override
	public void fillAccessToken(AccessToken jiraAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		if (userInfoUrl == null || (userInfoUrl = userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if (jsonPattern == null || (jsonPattern = jsonPattern.trim()).isEmpty()) {
			jsonPattern = JiraTokenFiller.JSON_PATTERN;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = JiraTokenFiller.BEAN_PROPS;
		}
		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = jiraAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		if (output == null || output.trim().isEmpty()) {
			throw new IllegalArgumentException("Jira user info response is empty");
		}
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, jiraAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
}