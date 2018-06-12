package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class GoogleProfile implements IConnectorIOp{

	String url = "https://www.googleapis.com/oauth2/v3/userinfo";
	
	// name of the object to return
	String objectName = "prerna.auth.User2"; // it will fill this object and return the data
	String [] beanProps = {"name", "gender", "locale"}; // add is done when you have a list
	String jsonPattern = "[name, gender, locale]";
	
	@Override
	public String execute(User user, Hashtable params) 
	{
		if(params == null)
			params = new Hashtable();
		// TODO Auto-generated method stub
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
				
		String accessToken = googToken.getAccess_token();
		
		// you fill what you want to send on the API call
		params.put("access_token", accessToken);
		params.put("alt", "json");
		
		// make the API call
		String output = AbstractHttpHelper.makeGetCall(url, accessToken, params, false);
		
		//System.out.println("Output >>>>> " + output);
		
		// fill the bean with the return
		googToken = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanProps, googToken);
		
		user.setAccessToken(googToken);
		
		return output;
	}

}
