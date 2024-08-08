package prerna.io.connector.okta;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class OktaTokenFiller implements IAccessTokenFiller {

	private static String jsonPattern = "[sub,name,email,phone_number]";
	private static String[] beanProps = {"id","name","email","phone"};
	
	@Override
	public void fillAccessToken(AccessToken oktaAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params) {
		if(params == null) {
			params = new HashMap<>();
		}
		if(jsonPattern == null || (jsonPattern=jsonPattern.trim()).isEmpty()) {
			jsonPattern = OktaTokenFiller.jsonPattern;
		}
		if(beanProps == null || beanProps.length == 0) {
			beanProps = OktaTokenFiller.beanProps;
		}
		
		String accessToken = oktaAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, oktaAccessToken);
	}

	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
	
}
