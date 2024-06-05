package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.SentimentAnalysis;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleSentimentAnalyzer implements IConnectorIOp {

	String url = "https://language.googleapis.com/v1/documents:analyzeSentiment";
	
	String [] beanProps = {"sentence", "magnitude", "score"};
	String jsonPattern = "sentences[].{sentence: text.content, magnitude: sentiment.magnitude, score:sentiment.score}";
	
	@Override
	public Object execute(User user, Hashtable params) {
		// if no input, unsure what you will get...
		if(params == null) {
			params = new Hashtable();
		}
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
		String accessToken = googToken.getAccess_token();
		
		// make the API call
		String jsonString = HttpHelperUtility.makePostCall(url, accessToken, params , true);
//		System.out.println("Output >>>>> " + jsonString);
		
//		// fill the bean with the return
		Object returnObj = BeanFiller.fillFromJson(jsonString, jsonPattern, beanProps, new SentimentAnalysis());
		return returnObj;
	}

}
