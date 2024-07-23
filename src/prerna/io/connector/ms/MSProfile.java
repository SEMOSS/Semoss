package prerna.io.connector.ms;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class MSProfile implements IConnectorIOp{

	private static String url = "https://graph.microsoft.com/v1.0/me/";
	private static String [] beanProps = {"name","id","email"}; // add is done when you have a list
	private static String jsonPattern = "[displayName,id,mail]";
	
	@Override
	public String execute(User user, Map<String, Object> params) {
		AccessToken acToken = user.getAccessToken(AuthProvider.MS);
		return fillAccessToken(acToken, params);
	}
	
	public static String fillAccessToken(AccessToken acToken, Map<String, Object> params) {
		if(params == null) {
			params = new HashMap<>();
		}
		
		String accessToken = acToken.getAccess_token();
		
		// you fill what you want to send on the API call
		params.put("Bearer", accessToken);
		params.put("alt", "json");
		
		// make the API call
		String output = HttpHelperUtility.makeGetCall(url, accessToken, null, true);

		//String output = HttpHelperUtility.makeGetCall(url, accessToken, params, false);
		
		// fill the bean with the return
		acToken = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanProps, acToken);
		
		return output;
	}

}
