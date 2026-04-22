package prerna.io.connector.servicenow;

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
	private static final String USER_INFO_URL_PROP = socialData.getProperty(PREFIX + "_userinfo_url");

	// Updated for array-based response
	private static final String[] DEFAULT_BEAN_PROPS = { "name", "email", "id" };
	private static final String DEFAULT_JSON_PATTERN = "[result.name, result.email, result.sys_id]";

	@Override
	public void fillAccessToken(AccessToken snAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params) {
		// Use defaults if parameters are not provided
		if (userInfoUrl == null || userInfoUrl.trim().isEmpty()) {
			userInfoUrl = USER_INFO_URL_PROP;
		}
		if (jsonPattern == null || jsonPattern.trim().isEmpty()) {
			jsonPattern = DEFAULT_JSON_PATTERN;
		}
		if (beanProps == null || beanProps.length == 0) {
			beanProps = DEFAULT_BEAN_PROPS;
		}
		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = snAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, null, true);

		// Fill the bean with the returned JSON
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, snAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		// ServiceNow payload is controlled and immediately mapped into AccessToken
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
}
