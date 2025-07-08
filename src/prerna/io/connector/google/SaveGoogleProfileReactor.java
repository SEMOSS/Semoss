package prerna.io.connector.google;

import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

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
	
	private static final String table="Google_USERDB";
	 
	private static final Logger classLogger = LogManager.getLogger(SaveGoogleProfileReactor.class);
	
	public SaveGoogleProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey(), ReactorKeysEnum.USERID.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public String getReactorDescription() {
		return "This reactor will save user details in DB";
	}

	@Override
	public NounMetadata execute() {
		String msg=null;
		String profileId = null;
		try {
			this.organizeKeys();
			LocalDateTime now = LocalDateTime.now();
			HashMap<String, Object> map=new HashMap<String, Object>();
			HashMap<String, Object> responseMap=new HashMap<String, Object>();
			IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
			String name = this.keyValue.get(this.keysToGet[0]);
			String userId = this.keyValue.get(this.keysToGet[1]);
			User user = this.insight.getUser();
			String insightusername = user.getPrimaryLoginToken().getUsername();
			String type="Google";
			map=insertSpreadSheetData(securityDb,now,name,userId,insightusername,type);
			Object object = map.get("Data inserted successfully");
			if(Boolean.FALSE.equals(map.get("Data inserted successfully"))) {
				msg=(String) map.get("Error");
				responseMap.put("Data inserted successfully", object);
				responseMap.put("Error",msg);
			}
			if (Boolean.TRUE.equals(map.get("Data inserted successfully"))) {
				profileId = readData(name,securityDb);
			}
			if (!profileId.isEmpty()) {
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

	/**
	 * To readdata of db
	 * @param titleSheetName
	 * @param accessToken
	 * @return 
	 */
	private String readData(String name, IDatabaseEngine securityDb) {
		String tableName = null;
		String profileKey=null;
		try {
			List<String> tables = securityDb.getPixelConcepts();
			for(String tbl:tables) {
				if(table.equals(tbl)) {
					tableName=tbl;
					break;
				}
			}
			String query = " select id from " + tableName + " where name='" + name + "'";
			HashMap<String, String> hashmap = (HashMap<String, String>) securityDb.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string; 
				while (rs.next()) {
					profileKey = rs.getString("id");
				}
			}
			return profileKey;
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return profileKey;
	}

	/**
	 * To insert user data in db
	 * @param securityDb
	 * @param now
	 * @param name
	 * @param userId
	 * @param insightusername
	 * @param type
	 * @return 
	 */
	private HashMap<String, Object> insertSpreadSheetData(IDatabaseEngine securityDb, LocalDateTime now, String name, String userId, String insightusername, String type) {
		String tableName = null;
		String msg=null;
		HashMap<String, Object> map=new HashMap<>();
		boolean flag=false;
		try {
			String id = UUID.randomUUID().toString();
			List<String> tables = securityDb.getPixelConcepts();
			for(String tbl:tables) {
				if(table.equals(tbl)) {
					tableName=tbl;
					break;
				}
			}
			String insertQuery =" INSERT INTO " + tableName
					+ "(datecreated,userid,name,username,type,id) " + "VALUES ('" + now + "','"+ userId + "','"+ name + "','"+ insightusername + "','"+ type + "','"+ id + "')";
			securityDb.insertData(insertQuery);
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
