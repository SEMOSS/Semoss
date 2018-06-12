package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.GeoLocation;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class GoogleLatLongGetter implements IConnectorIOp{

	String url = "https://maps.googleapis.com/maps/api/geocode/json" ;//?key=***REMOVED***;
	
	// name of the object to return
	String objectName = "prerna.auth.User2"; // it will fill this object and return the data
	String [] beanProps = {"latitude", "longitude"}; // add is done when you have a list
	String jsonPattern = "results[*].geometry.location.[lat, lng][]";
	
	@Override
	public Object execute(User user, Hashtable params) 
	{
		if(params == null) {
			params = new Hashtable();
		}

		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE_MAP);
		String accessToken = googToken.getAccess_token();
		// you fill what you want to send on the API call
		// the other thing it needs is an address
		params.put("key", accessToken);

		// make the API call
		String output = AbstractHttpHelper.makeGetCall(url, accessToken, params, false);
		//System.out.println("Output >>>>> " + output);
		
		// fill the bean with the return
		GeoLocation retLocation = (GeoLocation)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new GeoLocation());
		return retLocation;
	}

}
