package prerna.sablecc2.reactor.qs.source;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.poi.main.MetaModelCreator;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GoogleFileRetrieverReactor extends AbstractQueryStructReactor{

	private static final String CLASS_NAME = GoogleFileRetrieverReactor.class.getName();

	
	public GoogleFileRetrieverReactor() {
		this.keysToGet = new String[] { "id", "type"};
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {

		//get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String fileID = this.keyValue.get(this.keysToGet[0]);
		if (fileID == null || fileID.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file id");
		}
		String type= this.keyValue.get(this.keysToGet[1]);
		if (type == null || type.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file type");
		}


		//get access token
		String accessToken=null;
		User2 user = this.insight.getUser2();
		try {
			if(user==null){
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			}
			else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE.name());
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}

		//Initialize variables
		Hashtable params = new Hashtable();
		CsvQueryStruct qs = new CsvQueryStruct();

		//filepath for the download/export
		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath += "\\" + Utility.getRandomString(10) + ".csv";
		filePath = filePath.replace("\\", "/");
		String url_str = null;
		if(type.contains("google-apps.spreadsheet")){
			url_str = "https://www.googleapis.com/drive/v3/files/"+fileID+"/export"; 
			params = new Hashtable();
			params.put("mimeType", "text/csv");
		}
		else if(type.contains("text/csv")){
			url_str = "https://www.googleapis.com/drive/v3/files/"+fileID; 
			params = new Hashtable();
			params.put("alt", "media");
		}
		else{
			throw new IllegalArgumentException("Illegal file type");
		}
		try{
			BufferedReader br = AbstractHttpHelper.getHttpStream(url_str, accessToken, params, true);

			// create a file

			File outputFile = new File(filePath);

			BufferedWriter target = new BufferedWriter(new FileWriter(outputFile));
			String data = null;


			while((data = br.readLine()) != null)
			{
				target.write(data);
				target.write("\n");
				target.flush();
			}
			// get datatypes
			CSVFileHelper helper = new CSVFileHelper();
			helper.setDelimiter(',');
			helper.parse(filePath);
			MetaModelCreator predictor = new MetaModelCreator(helper, null);
			Map<String, String> dataTypes = predictor.getDataTypeMap();
			for (String key : dataTypes.keySet()) {
				qs.addSelector("DND", key);
			}
			helper.clear();
			qs.merge(this.qs);
			qs.setFilePath(filePath);
			qs.setDelimiter(',');
			qs.setColumnTypes(dataTypes);
			return qs;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return qs;
	}
}
