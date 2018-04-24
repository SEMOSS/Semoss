
package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.io.connector.IConnectorIOp;
import prerna.om.RemoteItem;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class DropBoxListFilesReactor extends AbstractReactor{

	public DropBoxListFilesReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {

		List<HashMap<String, Object>> masterList = new ArrayList<HashMap<String, Object>>();

		// lists the various files for this user

		String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
		String [] beanProps = {"name", "path"}; // add is done when you have a list
		String jsonPattern = "matches[].[metadata.name, metadata.path_lower]";

		//api string
		String url_str = "https://api.dropboxapi.com/2/files/search";

		//get access token
		String accessToken = null;
		User2 user = this.insight.getUser2();
		try{
			if(user==null){
				SemossPixelException exception = new SemossPixelException();
				exception.setContinueThreadOfExecution(false);
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "dropbox");
				retMap.put("message", "Please login to your DropBox account");
				exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
				throw exception;
			}
			else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.DROPBOX.name());
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			SemossPixelException exception = new SemossPixelException();
			exception.setContinueThreadOfExecution(false);
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "dropbox");
			retMap.put("message", "Please login to your DropBox account");
			exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
			throw exception;
		}


		// you fill what you want to send on the API call
		Hashtable params = new Hashtable();
		params.put("path","");
		params.put("query", ".csv");
		params.put("start", 0);
		params.put("max_results", 200);
		params.put("mode", "filename");


		String output = AbstractHttpHelper.makePostCall(url_str, accessToken, params, true);

		// fill the bean with the return
		List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		for(RemoteItem entry : fileList){
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", entry.getName());
			tempMap.put("path", entry.getPath());
			masterList.add(tempMap);

		}

		return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.GOOGLE_DRIVE_LIST);




	}




}