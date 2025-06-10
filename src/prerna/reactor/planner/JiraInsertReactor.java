package prerna.reactor;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class JiraInsertReactor extends AbstractReactor {

	static IRDBMSEngine userTrackingDb;

	public JiraInsertReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.API_KEY.getKey(),
				ReactorKeysEnum.URL.getKey() };
		this.keyRequired = new int[] { 1, 1, 1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String userId = this.keyValue.get(this.keysToGet[0]);
		String apiToken = this.keyValue.get(this.keysToGet[1]);
		String url = this.keyValue.get(this.keysToGet[2]);
		long dateCreated = System.currentTimeMillis();
		long lastUsed = System.currentTimeMillis();
		Boolean insertData = false;
		Timestamp date=new Timestamp(dateCreated);
		Timestamp lused=new Timestamp(lastUsed);
		HashMap<Object, Object> map = new HashMap<Object, Object>();
		int profileId = 0;
		try {
			IDatabaseEngine database = Utility.getDatabase("c44b138d-aa8e-42cc-a925-6c2ac855df64");
			insertData = insertData(database, userId, apiToken,url,date,lused);
			if (insertData == true) {
				profileId = readData(database, userId, apiToken,url,date,lused);
			}
			if (profileId != 0) {
				map.put("Primary key from Table", profileId);
				map.put("Success", insertData);
			}
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in executing the reactor";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private Integer readData(IDatabaseEngine database, String userId, String apiKey,String url,Timestamp date,Timestamp lused) {
		int profileKey = 0;
		try {
			String tableName = null;
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			String query=" select JIRAPROFILE_UNIQUE_ROW_ID from "+tableName+ " where APIKEY='"+apiKey+"' and URL='"+url+"'and DATE_CREATED='"+date+"' and LAST_USED='"+lused+"'";
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
			e.printStackTrace();
		}
		return profileKey;

	}

	private static Boolean insertData(IDatabaseEngine database, String userId, String apiKey, String url, Timestamp dateCreated, Timestamp lastUsed) {
		String tableName = null;
		try {
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
				tableName = element;
			}
			String insertQuery=" INSERT INTO "+tableName+ "(Apikey,UserId,URL,DATE_CREATED,LAST_USED) " + "VALUES ('" +apiKey + "','"+userId +"','"+url+"','"+dateCreated+"','"+lastUsed+"')";
			database.insertData(insertQuery);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

}
