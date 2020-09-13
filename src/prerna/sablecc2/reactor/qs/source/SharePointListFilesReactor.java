
package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;

public class SharePointListFilesReactor extends AbstractReactor{
	private static final String CLASS_NAME = SharePointListFilesReactor.class.getName();


	public SharePointListFilesReactor() {
		this.keysToGet = new String[] { "driveId" };
	}

	@Override
	public NounMetadata execute() {
		
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String driveId = this.keyValue.get(this.keysToGet[0]);
		if (driveId == null || driveId.length() <= 0) {
			throw new IllegalArgumentException("Need to specify the SharePoint Drive id");
		}
		

		List<HashMap<String, Object>> masterList = new ArrayList<HashMap<String, Object>>();
		// lists the various files for this user
		// if the 
		// name of the object to return
		String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
		String [] beanProps = {"name", "id"}; // add is done when you have a list
		String jsonPattern = "value[].{name:name,id:id}";

		//get access token
		String accessToken=null;
		User user = this.insight.getUser();

		try{
			if(user==null){
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "microsoft");
				retMap.put("message", "Please login to your Microsoft account");
				throwLoginError(retMap);
			}
			else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.MS);
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "microsoft");
			retMap.put("message", "Please login to your Microsoft account");
			throwLoginError(retMap);
		}

		//add in params for the get call
		Hashtable params = new Hashtable();
		params.put("select", "name,id");
		String url_str = "https://graph.microsoft.com/v1.0/drives/"+driveId+"/root/search(q='.csv')";
		String output = AbstractHttpHelper.makeGetCall(url_str, accessToken, params, true);

		// fill the bean with the return
		//fill an object
		Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		//check if the object if a remote item or a vector
		//if its a remote item add it to the master list
		System.out.println(C.getClass().getName());
		if(C instanceof RemoteItem){
			RemoteItem fileList= (RemoteItem) C;
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", fileList.getName());
			tempMap.put("id", fileList.getId());
			masterList.add(tempMap);
		}
		//if its a list, iterate through it and add it to the master list
		else{
			List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
			for(RemoteItem entry : fileList){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				masterList.add(tempMap);
			}
		}

		return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.CLOUD_FILE_LIST);

	}


}