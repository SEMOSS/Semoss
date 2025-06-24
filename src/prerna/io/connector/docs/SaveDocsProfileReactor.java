package prerna.io.connector.docs;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SaveDocsProfileReactor extends AbstractReactor {

	static IRDBMSEngine userTrackingDb;

	public SaveDocsProfileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATE_CREATED.getKey(), ReactorKeysEnum.LAST_USED.getKey(),
				ReactorKeysEnum.JSON.getKey() };
		this.keyRequired = new int[] {1, 1, 1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String dateCreated = this.keyValue.get(this.keysToGet[0]);
		String lastUsed = this.keyValue.get(this.keysToGet[1]);
		String serviceJson = this.keyValue.get(this.keysToGet[2]);
		Boolean insertData = false;
		Timestamp date = Timestamp.valueOf(dateCreated);
		Timestamp lused = Timestamp.valueOf(lastUsed);
		User user = this.insight.getUser();
		String userName = user.getPrimaryLoginToken().getUsername();
		String userEmail = user.getPrimaryLoginToken().getEmail();
		HashMap<Object, Object> map = new HashMap<Object, Object>();
		int profileId = 0;
		try {
			IDatabaseEngine database = Utility.getDatabase("9be6565f-550f-4be0-8758-c25232973cb1");
			insertData = insertData(database, userName, userEmail, date, lused, serviceJson);
			if(insertData) {
				profileId = readData(database, userName, date, lused, serviceJson);
			}
			if (profileId != 0) {
				map.put("id", profileId);
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

	private static Boolean insertData(IDatabaseEngine database, String Name, String email, Timestamp dateCreated, Timestamp lastUsed, String serviceJson) {
		String tableName = null;
		try {
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String insertQuery = " INSERT INTO " + tableName + "(name,useremail,datecreated,lastupdateddate,servicejson) "
					+ "VALUES ('" + Name + "','" + email + "','" + dateCreated + "','" + lastUsed
					+ "','" + serviceJson + "')";
			database.insertData(insertQuery);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	private Integer readData(IDatabaseEngine database, String Name, Timestamp dateCreated, Timestamp lastUsed, String serviceJson) {
		int profileKey = 0;
		try {
			String tableName = null;
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String query=" select id from "+tableName+ " where name='"+Name+"' and servicejson='"+serviceJson+"'";
			@SuppressWarnings("unchecked")
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
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

}