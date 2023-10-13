
package prerna.reactor.qs.source;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class OneDriveFileRetrieverReactor extends AbstractQueryStructReactor{

	private static final String CLASS_NAME = OneDriveFileRetrieverReactor.class.getName();

	public OneDriveFileRetrieverReactor() {
		this.keysToGet = new String[] { "id" };
	}



	@Override
	protected SelectQueryStruct createQueryStruct() {
		//get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String msID = this.keyValue.get(this.keysToGet[0]);
		if (msID == null || msID.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file id");
		}

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


		Hashtable params = new Hashtable();
		CsvQueryStruct qs = new CsvQueryStruct();
		BufferedWriter target = null;
		try {
			String url_str = "https://graph.microsoft.com/v1.0/me/drive/items/"+msID+"/content";
			BufferedReader br = AbstractHttpHelper.getHttpStream(url_str, accessToken, params, true);

			// create a file
			String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
					+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
			filePath += "\\" + Utility.getRandomString(10) + ".csv";
			filePath = filePath.replace("\\", "/");
			File outputFile = new File(filePath);

			target = new BufferedWriter(new FileWriter(outputFile));
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
			Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
			Map<String, String> dataTypes = predictionMaps[0];
			Map<String, String> additionalDataTypes = predictionMaps[1];
			for (String key : dataTypes.keySet()) {
				qs.addSelector("DND", key);
			}
			helper.clear();
			qs.merge(this.qs);
			qs.setFilePath(filePath);
			qs.setDelimiter(',');
			qs.setColumnTypes(dataTypes);
			qs.setAdditionalTypes(additionalDataTypes);
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if(target != null) {
		          try {
		        	  target.flush();
		        	  target.close();
		          } catch(IOException e) {
		            logger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
		return qs;
	}




}