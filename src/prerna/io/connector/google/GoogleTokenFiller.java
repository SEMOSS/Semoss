package prerna.io.connector.google;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleTokenFiller implements IAccessTokenFiller {

	private static final String USER_INFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo";
	private static String [] beanProps = {"name", "gender", "locale", "email", "id"}; // add is done when you have a list
	private static String jsonPattern = "[name, gender, locale, email, sub]";
	
	@Override
	public void fillAccessToken(AccessToken googleAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params) {
		if(userInfoUrl == null || (userInfoUrl=userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if(jsonPattern == null || (jsonPattern=jsonPattern.trim()).isEmpty()) {
			jsonPattern = GoogleTokenFiller.jsonPattern;
		}
		if(beanProps == null || beanProps.length == 0) {
			beanProps = GoogleTokenFiller.beanProps;
		}
		
		if(params == null) {
			params = new HashMap<>();
		}
		
		String accessToken = googleAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, googleAccessToken);
	}
	
	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
	
}
