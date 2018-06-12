package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

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

public class GoogleListFilesReactor extends AbstractReactor{


	public GoogleListFilesReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		String url_str = "https://www.googleapis.com/drive/v3/files";
		List<HashMap<String, Object>> masterList = new ArrayList<HashMap<String, Object>>();


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

		//get access token
		String accessToken=null;
		User user = this.insight.getUser();
		try{
			if(user==null){
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			}
			else if (user != null) {
				AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken=googleToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}


		// you fill what you want to send on the API call
		//text/csv call
		Hashtable params = new Hashtable();
		params.put("access_token", accessToken);
		params.put("pageSize", "1000");
		params.put("q=mimeType", "'text/csv'");
	

		String output = AbstractHttpHelper.makeGetCall(url_str, accessToken, params, false);
		
		Hashtable params2 = new Hashtable();
		params.put("access_token", accessToken);
		params.put("pageSize", "1000");
		params.put("q=mimeType", "'application/vnd.google-apps.spreadsheet'");
		
		String output2 = AbstractHttpHelper.makeGetCall(url_str, accessToken, params, false);


		// fill the bean with the return
		Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		System.out.println(C.getClass().getName());
		if(C instanceof RemoteItem){
			RemoteItem fileList= (RemoteItem) C;
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", fileList.getName());
			tempMap.put("id", fileList.getId());
			tempMap.put("type", fileList.getType());
			masterList.add(tempMap);
		}
		else{
			List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
			for(RemoteItem entry : fileList){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				tempMap.put("type", entry.getType());
				masterList.add(tempMap);
			}
		}
		
		Object D = BeanFiller.fillFromJson(output2, jsonPattern, beanProps, new RemoteItem());
		System.out.println(D.getClass().getName());
		if(D instanceof RemoteItem){
			RemoteItem fileList2= (RemoteItem) D;
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", fileList2.getName());
			tempMap.put("id", fileList2.getId());
			tempMap.put("type", fileList2.getType());
			masterList.add(tempMap);
		}
		else{
			List <RemoteItem> fileList2 = (List)BeanFiller.fillFromJson(output2, jsonPattern, beanProps, new RemoteItem());
			for(RemoteItem entry : fileList2){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				tempMap.put("type", entry.getType());
				masterList.add(tempMap);
			}
		}
/*
		List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		List <RemoteItem> fileList2 = (List)BeanFiller.fillFromJson(output2, jsonPattern, beanProps, new RemoteItem());

		for(RemoteItem entry : fileList){
			if(((entry.getType().toString().equalsIgnoreCase("text/csv")&&(entry.getName().toString().contains(".csv"))))){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				tempMap.put("type", entry.getType());
				masterList.add(tempMap);
			}

		}
		for(RemoteItem entry : fileList2){
			HashMap<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", entry.getName());
			tempMap.put("id", entry.getId());
			tempMap.put("type", entry.getType());
			masterList.add(tempMap);		
			}
			*/

		return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.CLOUD_FILE_LIST);	}

}
