package prerna.io.connector.ms;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.io.connector.IAccessTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class MicrosoftTokenFiller implements IAccessTokenFiller {

	private static final String USER_INFO_URL = "https://graph.microsoft.com/v1.0/me/";
	private static String [] beanProps = {"name","id","email"}; 
	private static String jsonPattern = "[displayName,id,mail]";
	
	@Override
	public void fillAccessToken(AccessToken msAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params) {
		if(userInfoUrl == null || (userInfoUrl=userInfoUrl.trim()).isEmpty()) {
			userInfoUrl = USER_INFO_URL;
		}
		if(jsonPattern == null || (jsonPattern=jsonPattern.trim()).isEmpty()) {
			jsonPattern = MicrosoftTokenFiller.jsonPattern;
		}
		if(beanProps == null || beanProps.length == 0) {
			beanProps = MicrosoftTokenFiller.beanProps;
		}
		
		if(params == null) {
			params = new HashMap<>();
		}
		
		String accessToken = msAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, msAccessToken);
	}
	
	@Override
	public void fillAccessToken(AccessToken accessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params, boolean sanitizeResponse) {
		// dont need to sanitize
		fillAccessToken(accessToken, userInfoUrl, jsonPattern, beanProps, params);
	}
	
}