package prerna.io.connector.serviceNow;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.SocialPropertiesUtil;

public class ServiceNowTokenFiller implements IAccessTokenFiller {

	private static final SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
	private static final String PREFIX = "servicenow";
	private static final String INSTANCE_URL = socialData.getProperty(PREFIX + "_instance_url");
	private static final String USER_INFO_URL_PROP = socialData.getProperty(PREFIX + "_userinfo_url");

	// Updated for array-based response
	private static final String[] BEAN_PROPS = { "user_name", "email", "id", "name" };
	private static final String JSON_PATTERN = "[result.user_name, result.email, result.sys_id, result.name]";

	@Override
	public void fillAccessToken(AccessToken snAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		// Use defaults if parameters are not provided
		if (userInfoUrl == null || userInfoUrl.trim().isEmpty()) {
			userInfoUrl = USER_INFO_URL_PROP;
		}
		if (jsonPattern == null || jsonPattern.trim().isEmpty()) {
			jsonPattern = JSON_PATTERN;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = BEAN_PROPS;
		}
		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = snAccessToken.getAccess_token();

		// Debug logging
		System.out.println("servicenow_userinfo_url: [" + USER_INFO_URL_PROP + "]");
		System.out.println("Final userInfoUrl used for GET: [" + userInfoUrl + "]");

		// Make GET call to ServiceNow user info endpoint
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, null, true);

		// Debug: print the user info response
		System.out.println("ServiceNow user info response: " + output);

		// Fill the bean with the returned JSON
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, snAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// Delegate to main method (no special handling for sanitizeResponse)
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
}
