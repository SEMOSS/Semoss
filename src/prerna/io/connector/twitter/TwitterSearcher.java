package prerna.io.connector.twitter;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.Viewpoint;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class TwitterSearcher implements IConnectorIOp{

	String url = "https://api.twitter.com/1.1/search/tweets.json" ;//?key=***REMOVED***;
	
	// name of the object to return
	String objectName = "prerna.om.Viewpoint"; // it will fill this object and return the data
	String [] beanProps = {"review", "author", "authorId", "location", "repeatCount", "favCount", "followerCount"}; // add is done when you have a list
	String jsonPattern = "statuses[].{review: text, author: id, authorId:user.screen_name, user_name: user.name, location:user.location, repeatCount:retweet_count, favCount: favorite_count, followerCount: user.followers_count }";
	
	// things you can feed into a search
	// https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets
	// q - query string
	// geocode - lat, long, 2mile
	// result_type - mixed, recent, popular
	// count - how many to get - defaults to 10
	// max_id, since_id - low level control in terms of the id to get

	@Override
	public Object execute(User user, Hashtable params) {
		if(params == null) {
			params = new Hashtable();
		}
		
		AccessToken twitToken = user.getAccessToken(AuthProvider.TWITTER);
		String accessToken = twitToken.getAccess_token();
				
		// make the API call
		String output = AbstractHttpHelper.makeGetCall(url, accessToken, params, true);
//		System.out.println(output);
		
		// need a way to convert this into a full review
		Object retObject = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new Viewpoint());
		return retObject;
	}
}
