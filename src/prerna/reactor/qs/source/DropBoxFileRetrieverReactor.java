
package prerna.reactor.qs.source;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DropBoxFileRetrieverReactor extends AbstractQueryStructReactor{

	//private String[] keysToGet;
	private static final String CLASS_NAME = DropBoxFileRetrieverReactor.class.getName();


	public DropBoxFileRetrieverReactor() {
		this.keysToGet = new String[] { "path" };
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {

		//get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String dropboxPath = this.keyValue.get(this.keysToGet[0]);
		if (dropboxPath == null || dropboxPath.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file path");
		}



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
				AccessToken msToken = user.getAccessToken(AuthProvider.DROPBOX);
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "dropbox");
			retMap.put("message", "Please login to your DropBox account");
			throwLoginError(retMap);
		}

		//

		// lists the various files for this user
		// if the 
		// name of the object to return
		String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
		String [] beanProps = {"name","id","url"}; // add is done when you have a list
		String jsonPattern = "[metadata.name,metadata.id,link]";

		// you fill what you want to send on the API call
		String url_str = "https://api.dropboxapi.com/2/files/get_temporary_link";
		Hashtable params = new Hashtable();
		params.put("path", dropboxPath);

		String output = AbstractHttpHelper.makePostCall(url_str, accessToken, params, true);

		// fill the bean with the return. This return will have a url to download the file from which is done below
		RemoteItem link = (RemoteItem) BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");
		try {
			URL urlDownload = new URL(link.getUrl());
			File destination = new File(filePath);
			FileUtils.copyURLToFile(urlDownload, destination);
		} catch (MalformedURLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// get datatypes
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(filePath);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
		Map<String, String> dataTypes = predictionMaps[0];
		Map<String, String> additionalDataTypes = predictionMaps[1];
		CsvQueryStruct qs = new CsvQueryStruct();
		for (String key : dataTypes.keySet()) {
			qs.addSelector("DND", key);
		}
		helper.clear();
		qs.merge(this.qs);
		qs.setFilePath(filePath);
		qs.setDelimiter(',');
		qs.setColumnTypes(dataTypes);
		qs.setAdditionalTypes(additionalDataTypes);
		return qs;


	}


}