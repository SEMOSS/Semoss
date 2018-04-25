
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
import prerna.poi.main.MetaModelCreator;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class OneDriveFileRetrieverReactor extends AbstractQueryStructReactor{

	public OneDriveFileRetrieverReactor() {
		this.keysToGet = new String[] { "name", "id" };
	}



	@Override
	protected QueryStruct2 createQueryStruct() {
		String fileName = this.curRow.get(0).toString();
		if (fileName == null || fileName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file name");
		}
		String msID = this.curRow.get(1).toString();
		if (msID == null || msID.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file path");
		}

		//get access token
		String accessToken=null;
		User2 user = this.insight.getUser2();

		try{
			if(user==null){
				Map<String, Object> retMap = new HashMap<String, Object>();
				retMap.put("type", "microsoft");
				retMap.put("message", "Please login to your Microsoft account");
				throwLoginError(retMap);
			}
			else if (user != null) {
				AccessToken msToken = user.getAccessToken(AuthProvider.AZURE_GRAPH.name());
				accessToken=msToken.getAccess_token();
			}
		}
		catch (Exception e) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("type", "microsoft");
			retMap.put("message", "Please login to your Microsoft account");
			throwLoginError(retMap);
		}

		
		Hashtable params = new Hashtable();
		CsvQueryStruct qs = new CsvQueryStruct();

		try {
			String url_str = "https://graph.microsoft.com/v1.0/me/drive/items/"+msID+"/content";
			BufferedReader br = AbstractHttpHelper.getHttpStream(url_str, accessToken, params, true);

			// create a file
			String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
					+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
			filePath += "\\" + Utility.getRandomString(10) + ".csv";
			filePath = filePath.replace("\\", "/");
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