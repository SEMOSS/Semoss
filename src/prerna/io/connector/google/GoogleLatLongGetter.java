package prerna.io.connector.google;

import java.util.Hashtable;

import prerna.auth.AccessToken;
import prerna.auth.AppTokens;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.GeoLocation;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleLatLongGetter implements IConnectorIOp{

	String url = "https://maps.googleapis.com/maps/api/geocode/json" ;
	
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

		AccessToken googToken = null;
		if(user != null) {
			googToken = user.getAccessToken(AuthProvider.GOOGLE_MAP);
		}
		if(googToken == null) {
			googToken = AppTokens.getInstance().getAccessToken(AuthProvider.GOOGLE_MAP);
		}
		
		if(googToken == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires login to google", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		String accessToken = googToken.getAccess_token();
		// you fill what you want to send on the API call
		// the other thing it needs is an address
		params.put("key", accessToken);

		// make the API call
		String output = HttpHelperUtility.makeGetCall(url, accessToken, params, false);
		//System.out.println("Output >>>>> " + output);
		
		// fill the bean with the return
		GeoLocation retLocation = (GeoLocation)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new GeoLocation());
		return retLocation;
	}

}
