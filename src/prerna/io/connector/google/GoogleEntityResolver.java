package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.EntityResolution;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleEntityResolver implements IConnectorIOp {

	String url = "https://language.googleapis.com/v1/documents:analyzeEntities";
	
	String [] beanProps = {"entity_name", "entity_type", "wiki_url", "content", "content_subtype"}; 
	String jsonPattern = "entities[].{entity_name : name, entity_type : type, wiki_url : metadata.wikipedia_url, content : mentions[].text.content, content_subtype : mentions[].type}";
	
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
		
		EntityResolution entity = new EntityResolution();
//		// fill the bean with the return
		Object returnObj = BeanFiller.fillFromJson(jsonString, jsonPattern, beanProps, entity);
		return returnObj;
	}

}
