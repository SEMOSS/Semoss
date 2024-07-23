package prerna.io.connector.surveymonkey;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class MonkeyProfile implements IConnectorIOp{

	private static String url = "https://api.surveymonkey.com/v3/users/me";
	private static String [] beanProps = {"id","email","username","name"};
	// need to join the first name and last name together
	private static String jsonPattern = "{id: id, email: email, username: username, first_name: first_name, last_name: last_name}.[id, email, username, join(' ', [first_name, last_name])]";
	
	@Override
	public String execute(User user, Map<String, Object> params) {
		AccessToken acToken = user.getAccessToken(AuthProvider.SURVEYMONKEY);
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

		// fill the bean with the return
		acToken = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanProps, acToken);
		
		return output;
	}
	
}
