package prerna.io.connector.jira.reactor;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraHelper;
import prerna.util.Utility;

public class JiraInsertReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(JiraInsertReactor.class);
	
	static IRDBMSEngine userTrackingDb;

	public JiraInsertReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.API_KEY.getKey(),
				ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.ALIAS.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String userId = this.keyValue.get(this.keysToGet[0]);
		String apiToken = this.keyValue.get(this.keysToGet[1]);
		String url = this.keyValue.get(this.keysToGet[2]);
		String alias = this.keyValue.get(this.keysToGet[3]);
		long dateCreated = System.currentTimeMillis();
		long lastUsed = System.currentTimeMillis();
		HashMap<String, Object> map=new HashMap<>();
		Timestamp date = new Timestamp(dateCreated);
		Timestamp lused = new Timestamp(lastUsed);
		HashMap<Object, Object> responseMap = new HashMap<Object, Object>();
		User user = this.insight.getUser();
		String insightusername = user.getPrimaryLoginToken().getUsername();
		int profileId = 0;
		String msg=null;
		try {
			IDatabaseEngine database = Utility.getDatabase("bcdb0a92-2a3b-4c73-bb79-5f5116bd6832");
			map = insertData(database, userId, apiToken, url, date, lused, insightusername, alias);
			Object object = map.get("Data inserted successfully");
			if(Boolean.FALSE.equals(map.get("Data inserted successfully"))) {
				msg=(String) map.get("Error");
				responseMap.put("Data inserted successfully", object);
				responseMap.put("Error",msg);
			}
			if (Boolean.TRUE.equals(map.get("Data inserted successfully"))) {
				profileId = readData(database, userId, apiToken, url, date, lused, alias);
			}
			if (profileId != 0) {
				responseMap.put("Primary key from Table", profileId);
				responseMap.put("Success", map.get("Data inserted successfully"));
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in executing the reactor";
			msg=e.getMessage();
			return new NounMetadata(error + ":" + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private Integer readData(IDatabaseEngine database, String userId, String apiKey, String url, Timestamp date,
			Timestamp lused, String alias) {
		int profileKey = 0;
		try {
			String tableName = null;
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String query = " select JIRAPROFILE_UNIQUE_ROW_ID from " + tableName + " where API_KEY='" + apiKey
					+ "' and URL='" + url + "'and DATE_CREATED='" + date + "' and DATE_LAST_USED='" + lused
					+ "' and ALIAS='" + alias + "'";
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					profileKey = rs.getInt("JIRAPROFILE_UNIQUE_ROW_ID");
				}
			}
			return profileKey;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return profileKey;

	}

	private static HashMap<String, Object> insertData(IDatabaseEngine database, String userId, String apiKey, String url,
			Timestamp dateCreated, Timestamp lastUsed, String insightusername, String alias) {
		String tableName = null;
		String msg=null;
		HashMap<String, Object> map=new HashMap<>();
		boolean flag=false;
		try {
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String insertQuery = " INSERT INTO " + tableName
					+ "(API_KEY,USER_ID,URL,DATE_CREATED,DATE_LAST_USED,NAME,CREATED_BY,ALIAS) " + "VALUES ('" + apiKey
					+ "','" + userId + "','" + url + "','" + dateCreated + "','" + lastUsed + "','" + insightusername
					+ "','" + insightusername + "','" + alias + "')";
			database.insertData(insertQuery);
			flag=true;
			map.put("Data inserted successfully", flag);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			msg=e.getMessage();
			map.put("Data inserted successfully", flag);
			map.put("Error", msg);
		
		}
		return map;
	}

}
