package prerna.io.connector.google;

import java.util.Hashtable;
import java.util.List;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.RemoteItem;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleListFiles implements IConnectorIOp{

	// lists the various files for this user
	// if the 
	// name of the object to return
	String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
	String [] beanProps = {"id", "name", "type"}; // add is done when you have a list
	String jsonPattern = "files[].{id:id, name:name, type:mimeType}";

	// possible properties that can be passed
	//https://developers.google.com/drive/v2/web/search-parameters
	// https://developers.google.com/drive/v3/web/mime-types
	// CSV = text/csv
	// spreadsheet - application/x-vnd.oasis.opendocument.spreadsheet
	// ms excel - application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
	// tsv = text/tab-separated-values
	// spreadsheet -application/vnd.google-apps.spreadsheet
	// mimeType="text/csv" or mimeType="application/vnd.google-apps.spreadsheet"
	
	String url_str = "https://www.googleapis.com/drive/v3/files";

	@Override
	public Object execute(User user, Hashtable params) {
		
		if(params == null)
			params = new Hashtable();
		
		// TODO Auto-generated method stub
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
				
		String accessToken = googToken.getAccess_token();
		
		// you fill what you want to send on the API call
		params.put("access_token", accessToken);
		
		String output = HttpHelperUtility.makeGetCall(url_str, accessToken, params, false);
		
		// fill the bean with the return
		List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());

		
		// TODO Auto-generated method stub
		return fileList;
	}

}
