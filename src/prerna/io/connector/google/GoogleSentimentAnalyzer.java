package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.io.connector.IConnectorIOp;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class GoogleSentimentAnalyzer implements IConnectorIOp {

	String url = "https://language.googleapis.com/v1/documents:analyzeSentiment";
	
	// name of the object to return
	String objectName = "prerna.auth.User2"; // it will fill this object and return the data
	String [] beanProps = {"name", "gender", "locale"}; // add is done when you have a list
	String jsonPattern = "[name, gender, locale]";
	
	@Override
	public String execute(User2 user, Hashtable params) 
	{
		if(params == null)
			params = new Hashtable();
		// TODO Auto-generated method stub
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE.name());
				
		String accessToken = googToken.getAccess_token();
		
		// you fill what you want to send on the API call
		Object input = params.remove("input");
		
		// make the API call
		String output = AbstractHttpHelper.makePostCall(url, accessToken, input , true);
		
		//System.out.println("Output >>>>> " + output);
		
		// fill the bean with the return
		googToken = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanProps, googToken);
		
		user.setAccessToken(googToken);
		
		return output;
	}

}
