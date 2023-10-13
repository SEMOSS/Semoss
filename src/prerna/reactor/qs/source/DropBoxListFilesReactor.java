
package prerna.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
		String jsonPattern = "matches[].{name:metadata.name, path:metadata.path_lower}";

		//api string
		String url_str = "https://api.dropboxapi.com/2/files/search";

		//get access token
		String accessToken = null;
		User user = this.insight.getUser();
		try{
			if(user==null){
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "dropbox");
				retMap.put("message", "Please login to your DropBox account");
				throwLoginError(retMap);
			}
			else if (user != null) {
				AccessToken dropToken = user.getAccessToken(AuthProvider.DROPBOX);
				accessToken=dropToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "dropbox");
			retMap.put("message", "Please login to your DropBox account");
			throwLoginError(retMap);
		}


		// you fill what you want to send on the API call
		Hashtable params = new Hashtable();
		params.put("path","");
		params.put("query", ".csv");
		params.put("start", 0);
		params.put("max_results", 1000);
		params.put("mode", "filename");


		String output = AbstractHttpHelper.makePostCall(url_str, accessToken, params, true);

		// fill the bean with the return
		Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		System.out.println(C.getClass().getName());
		if(C instanceof RemoteItem){
			RemoteItem fileList= (RemoteItem) C;
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", fileList.getName());
			tempMap.put("path", fileList.getPath());
			masterList.add(tempMap);
		}
		else{
			List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
			for(RemoteItem entry : fileList){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("path", entry.getPath());
				masterList.add(tempMap);
			}
		}

		return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.CLOUD_FILE_LIST);

	}

}