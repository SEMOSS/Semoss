package prerna.io.connector.google;

import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class SaveGoogleProfileReactor extends AbstractReactor{
	 
	private static final Logger classLogger = LogManager.getLogger(SaveGoogleProfileReactor.class);
	
	public SaveGoogleProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SHEET_NAME.getKey(), ReactorKeysEnum.SPREADSHETT_ID.getKey(),
				ReactorKeysEnum.SERVICE_JSON.getKey(), ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.USERID.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		String msg=null;
		int profileId = 0;
		try {
			this.organizeKeys();
			LocalDateTime now = LocalDateTime.now();
			HashMap<String, Object> map=new HashMap<String, Object>();
			HashMap<String, Object> responseMap=new HashMap<String, Object>();
			IDatabaseEngine database = Utility.getDatabase("6abf12ab-ae96-4edd-a1af-b56b9a37634d");
			String sheetName = this.keyValue.get(this.keysToGet[0]);
			String spreadSheetId = this.keyValue.get(this.keysToGet[1]);
			String serviceJson = this.keyValue.get(this.keysToGet[2]);
			String name = this.keyValue.get(this.keysToGet[3]);
			String userId = this.keyValue.get(this.keysToGet[3]);
			map=insertSpreadSheetData(sheetName,spreadSheetId,serviceJson,database,now,name,userId);
			Object object = map.get("Data inserted successfully");
			if(Boolean.FALSE.equals(map.get("Data inserted successfully"))) {
				msg=(String) map.get("Error");
				responseMap.put("Data inserted successfully", object);
				responseMap.put("Error",msg);
			}
			if (Boolean.TRUE.equals(map.get("Data inserted successfully"))) {
				profileId = readData(name,spreadSheetId,serviceJson,database);
			}
			if (profileId != 0) {
				responseMap.put("Primary key from Table", profileId);
				responseMap.put("Success", map.get("Data inserted successfully"));
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in executing the reactor";
			msg=e.getMessage();
			return new NounMetadata(error + ":" + msg, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private int readData(String name, String spreadSheetId, String serviceJson, IDatabaseEngine database) {
		int profileKey = 0;
		try {
			String tableName = null;
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String query = " select id from " + tableName + " where spreadsheet_id='" + spreadSheetId + "'";
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					profileKey = rs.getInt("id");
				}
			}
			return profileKey;
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return profileKey;
	}

	private HashMap<String, Object> insertSpreadSheetData(String sheetName, String spreadSheetId, String serviceJson, IDatabaseEngine database, LocalDateTime now, String name, String userId) {
		String tableName = null;
		String msg=null;
		HashMap<String, Object> map=new HashMap<>();
		boolean flag=false;
		try {
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String insertQuery =" INSERT INTO " + tableName
					+ "(sheet_name,credentials,spreadsheet_id,update_at,created_at,user_id,name) " + "VALUES ('" + sheetName +"','"+ serviceJson +"','"+ spreadSheetId + "','"+ now + "','"+ now + "','"+ userId + "','"+ name + "')";
			database.insertData(insertQuery);
			flag=true;
			map.put("Data inserted successfully", flag);
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			msg=e.getMessage();
			map.put("Data inserted successfully", flag);
			map.put("Error", msg);
		}
		return map;
	}

}
