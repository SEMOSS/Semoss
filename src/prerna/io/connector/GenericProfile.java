package prerna.io.connector;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GenericProfile implements IConnectorIOp{


//	private static String url = "https://graph.microsoft.com/v1.0/me/";
//	private static String [] beanProps = {"name","id","email"}; // add is done when you have a list
//	private static String jsonPattern = "[displayName,id,mail]";
//	
	@Override
	public String execute(User user, Hashtable params) {
		//never called anywhere...
		AccessToken acToken = user.getAccessToken(AuthProvider.GENERIC);
		//return fillAccessToken(acToken, params);
		return null;
	}
	
	public static String fillAccessToken(AccessToken acToken, String userInfoURL, String beanProps, String jsonPattern, Hashtable params, Boolean sanitizeResponse) {
		if(params == null) {
			params = new Hashtable();
		}
		
		String[] beanPropsArr = beanProps.split(",", -1);

		String accessToken = acToken.getAccess_token();
		
		// you fill what you want to send on the API call
		params.put("Bearer", accessToken);
		params.put("alt", "json");
		
		// make the API call
		String output = HttpHelperUtility.makeGetCall(userInfoURL, accessToken, null, true);

		if(sanitizeResponse) {
			output = output.replace("\\", "\\\\");
			// add more replacements as need be in the future
		}

		//String output = HttpHelperUtility.makeGetCall(url, accessToken, params, false);
		
		
		acToken = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanPropsArr, acToken);
		
		return output;
	}

}
