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
		List<Map<String, Object>> masterList = new ArrayList<Map<String, Object>>();

		// possible properties that can be passed
		// https://developers.google.com/drive/v2/web/search-parameters
		// https://developers.google.com/drive/v3/web/mime-types
		// CSV = text/csv
		// spreadsheet - application/x-vnd.oasis.opendocument.spreadsheet
		// ms excel - application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
		// tsv = text/tab-separated-values
		// spreadsheet -application/vnd.google-apps.spreadsheet
		// mimeType="text/csv" or mimeType="application/vnd.google-apps.spreadsheet"

		//get access token
		String accessToken = null;
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

		//text/csv call
		Hashtable csvParams = new Hashtable();
		csvParams.put("access_token", accessToken);
		csvParams.put("pageSize", "1000");
		csvParams.put("q=mimeType", "'text/csv'");
		// this makes the call with the params and pushes the results into masterList
		gatherResults(accessToken, csvParams, masterList);

		// spreadsheet call
		Hashtable spreashseetParams = new Hashtable();
		spreashseetParams.put("access_token", accessToken);
		spreashseetParams.put("pageSize", "1000");
		spreashseetParams.put("q=mimeType", "'application/vnd.google-apps.spreadsheet'");
		// this makes the call with the params and pushes the results into masterList
		gatherResults(accessToken, spreashseetParams, masterList);

		return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CLOUD_FILE_LIST);
	}
	
	/**
	 * This makes the call to the google api and populates the results into the master list
	 * @param accessToken
	 * @param params
	 * @param masterList
	 */
	private void gatherResults(String accessToken, Hashtable params, List<Map<String, Object>> masterList) {
		String url = "https://www.googleapis.com/drive/v3/files";
		String [] beanProps = {"id", "name", "type"};
		String jsonPattern = "files[].{id:id, name:name, type:mimeType}";
		
		String output = AbstractHttpHelper.makeGetCall(url, accessToken, params, false);
		
		// loop through and aggregate results
		Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		if(C instanceof RemoteItem){
			RemoteItem fileList2= (RemoteItem) C;
			Map<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put("name", fileList2.getName());
			tempMap.put("id", fileList2.getId());
			tempMap.put("type", fileList2.getType());
			masterList.add(tempMap);
		}
		else{
			List <RemoteItem> fileList2 = (List<RemoteItem>) C;
			for(RemoteItem entry : fileList2){
				Map<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				tempMap.put("type", entry.getType());
				masterList.add(tempMap);
			}
		}
	}
}
