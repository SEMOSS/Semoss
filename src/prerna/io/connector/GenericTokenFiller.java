package prerna.io.connector;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GenericTokenFiller implements IAccessTokenFiller {

	@Override
	public void fillAccessToken(AccessToken genericAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps, Map<String, Object> params) {
		fillAccessToken(genericAccessToken, userInfoUrl, jsonPattern, beanProps, params, false);
	}

	@Override
	public void fillAccessToken(AccessToken genericAccessToken, String userInfoUrl, String jsonPattern, String[] beanProps,
			Map<String, Object> params, boolean sanitizeResponse) {
		if(params == null) {
			params = new HashMap<>();
		}
		
		String accessToken = genericAccessToken.getAccess_token();
		String output = HttpHelperUtility.makeGetCall(userInfoUrl, accessToken, params, true);
		
		if(sanitizeResponse) {
			output = output.replace("\\", "\\\\");
			// add more replacements as need be in the future
		}
		// fill the bean with the return
		BeanFiller.fillFromJson(output, jsonPattern, beanProps, genericAccessToken);
	}

}
