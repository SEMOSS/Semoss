package prerna.io.connector.salesforce;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class SalesforceTokenFiller implements IAccessTokenFiller {

	private static final String USER_INFO_URL = "https://login.salesforce.com/services/oauth2/userinfo";
	private static String [] beanProps = {"username", "email", "id"};
	private static String jsonPattern = "[name, email, user_id]";
	
	@Override
	public void fillAccessToken(AccessToken salesforceAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params) {
		if(userInfoUrl == null || (userInfoUrl=userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if(jsonPattern == null || (jsonPattern=jsonPattern.trim()).isEmpty()) {
			jsonPattern = SalesforceTokenFiller.jsonPattern;
		}
		if(beanProps == null || beanProps.length == 0) {
			beanProps = SalesforceTokenFiller.beanProps;
		}
		
		if(params == null) {
			params = new HashMap<>();
		}
		
		String accessToken = salesforceAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, salesforceAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
		
	}

}
