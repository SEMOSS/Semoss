package prerna.sablecc2.reactor.qs.source;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User2;
import prerna.io.connector.IConnectorIOp;
import prerna.poi.main.MetaModelCreator;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GoogleFileRetrieverReactor extends AbstractQueryStructReactor{

	public GoogleFileRetrieverReactor() {
		this.keysToGet = new String[] { "name", "id", "type"};
	}

	@Override
	protected QueryStruct2 createQueryStruct() {

		//get keys
		String fileName = this.curRow.get(0).toString();
		if (fileName == null || fileName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file name");
		}
		String fileID = this.curRow.get(1).toString();
		if (fileID == null || fileID.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file id");
		}
		String type = this.curRow.get(1).toString();
		if (type == null || type.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file type");
		}


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
			qs.setCsvFilePath(filePath);
			qs.setDelimiter(',');
			qs.setColumnTypes(dataTypes);
			return qs;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return qs;
	}
}
