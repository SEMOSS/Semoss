package prerna.io.connector.docs;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SaveDocsProfileReactor extends AbstractReactor {
	
	private static final String EngineId = "26a0d483-a005-4885-8420-46c685c5ee52";

	public SaveDocsProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.DOCNAME.getKey(),
				ReactorKeysEnum.JSON.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String name = this.keyValue.get(this.keysToGet[0]);
		String DocName = this.keyValue.get(this.keysToGet[1]);
		String serviceJson = this.keyValue.get(this.keysToGet[2]);
		Boolean insertData = false;
		Timestamp date = new Timestamp(System.currentTimeMillis());
		Timestamp lused = new Timestamp(System.currentTimeMillis());
		User user = this.insight.getUser();
		String insight_username = user.getPrimaryLoginToken().getUsername();
		String insight_usermailid = user.getPrimaryLoginToken().getEmail();
		HashMap<Object, Object> map = new HashMap<Object, Object>();
		int profileId = 0;
		String ID = "id";
		String SUCCESS = "Success";
		try {
			IDatabaseEngine database = Utility.getDatabase(EngineId);
			insertData = insertData(database, insight_username, insight_usermailid, name, date, lused, serviceJson,
					DocName);
			if (insertData) {
				profileId = readData(database, insight_username, date, lused, serviceJson);
			}
			if (profileId != 0) {
				map.put(ID, profileId);
				map.put(SUCCESS, insertData);
			}
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in executing the reactor";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private static Boolean insertData(IDatabaseEngine database, String InsightName, String InsightEmail, String Name,
			Timestamp dateCreated, Timestamp lastUsed, String serviceJson, String DocName) {
		String tableName = null;
		try {
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String insertQuery = " INSERT INTO " + tableName
					+ "(insight_username,insight_usermailid,name,docname,datecreated,lastupdateddate,servicejson) "
					+ "VALUES ('" + InsightName + "','" + InsightEmail + "','" + Name + "','" + DocName + "','"
					+ dateCreated + "','" + lastUsed + "','" + serviceJson + "')";
			database.insertData(insertQuery);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private Integer readData(IDatabaseEngine database, String InsightName, Timestamp dateCreated, Timestamp lastUsed,
			String serviceJson) {
		int profileKey = 0;
		try {
			String tableName = null;
			String ResultSetObj = "RESULTSET_OBJECT";
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String query = " select id from " + tableName + " where insight_username='" + InsightName
					+ "' and servicejson='" + serviceJson + "'";
			@SuppressWarnings("unchecked")
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get(ResultSetObj);
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					profileKey = rs.getInt("id");
				}
			}
			return profileKey;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return profileKey;

	}

	@Override
	public String getReactorDescription() {
		return "This reactor insert the data into the googledocsprofile database and also return the id.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "Name " + ReactorKeysEnum.NAME.getKey();
		} else if (key.equals(ReactorKeysEnum.DOCNAME.getKey())) {
			return "Name of the Document " + ReactorKeysEnum.DOCNAME.getKey();
		} else if (key.equals(ReactorKeysEnum.JSON.getKey())) {
			return "ServiceJson " + ReactorKeysEnum.JSON.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}