package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleProfile implements IConnectorIOp{

	private static String url = "https://www.googleapis.com/oauth2/v3/userinfo";
	private static String [] beanProps = {"name", "gender", "locale", "email", "id"}; // add is done when you have a list
	private static String jsonPattern = "[name, gender, locale, email, sub]";
	
	@Override
	public String execute(User user, Hashtable params) {
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
		return fillAccessToken(googToken, params);
	}
	
	public static String fillAccessToken(AccessToken googToken, Hashtable params) {
		if(params == null) {
			params = new Hashtable();
		}
		
		String accessToken = googToken.getAccess_token();
		
		// you fill what you want to send on the API call
		params.put("access_token", accessToken);
		params.put("alt", "json");
		
		// make the API call
		String output = HttpHelperUtility.makeGetCall(url, accessToken, params, false);
		
		// fill the bean with the return
		googToken = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanProps, googToken);
		
		return output;
	}

}
