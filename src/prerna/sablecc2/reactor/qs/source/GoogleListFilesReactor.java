package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

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
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
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
		String jsonPattern = "files[].[id, name, mimeType]";

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
		User2 user = this.insight.getUser2();
		try{
			if(user==null){
				SemossPixelException exception = new SemossPixelException();
				exception.setContinueThreadOfExecution(false);
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
				throw exception;
			}
			else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE.name());
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			SemossPixelException exception = new SemossPixelException();
			exception.setContinueThreadOfExecution(false);
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			exception.setAdditionalReturn(new NounMetadata(retMap, PixelDataType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR, PixelOperationType.ERROR));
			throw exception;
		}


		// you fill what you want to send on the API call
		Hashtable params = new Hashtable();
		params.put("access_token", accessToken);

		String output = AbstractHttpHelper.makeGetCall(url_str, accessToken, params, false);

		// fill the bean with the return
		List <RemoteItem> fileList = (List)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		for(RemoteItem entry : fileList){
			if((entry.getType().toString().contains("google-apps.spreadsheet"))||((entry.getType().toString().equalsIgnoreCase("text/csv")&&(entry.getName().toString().contains(".csv"))))){
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				tempMap.put("type", entry.getType());
				masterList.add(tempMap);
			}

		}

		return new NounMetadata(masterList, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.CLOUD_FILE_LIST);	}

}
