package prerna.io.connector.salesforce;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class SalesforceInsertConnectionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SalesforceInsertConnectionsReactor.class);
	
	private static final String TABLE = "SALESFORCE_CONNECTIONS";
    private static final String SALESFORCE_UNIQUE_ID = "ID";
    
	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String ALIAS = "alias";
	
	public SalesforceInsertConnectionsReactor() {
		this.keysToGet = new String[] { CLIENT_ID, CLIENT_SECRET, ALIAS };
		this.keyRequired = new int[] { 1, 1, 1 };
	}
	
	// Centralized table name lookup
    private String getTableName(IDatabaseEngine database) {
        try {
            List<String> tables = database.getPixelConcepts();
            for (String tableName : tables) {
                if (TABLE.equals(tableName)) {
                    return tableName;
                }
            }
        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
        }
        return null;
    }
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String clientId = this.keyValue.get(this.keysToGet[0]);
		String clientSecret = this.keyValue.get(this.keysToGet[1]);
		String alias = this.keyValue.get(this.keysToGet[2]);
		
		HashMap<Object, Object> responseMap = new HashMap<>();
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				responseMap.put("status", false);
				responseMap.put("error", "SALESFORCE_CONNECTIONS table not found in database.");
				return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			
			IRDBMSEngine salesforceDB = (RDBMSNativeEngine) database;
			
			UUID id = UUID.randomUUID();
			HashMap<String, Object> insertResult = insertData(salesforceDB, tableName, id.toString(),
						clientId, clientSecret, alias);
			
			Boolean inserted = (Boolean) insertResult.get("status");
			if (!inserted) {
				String msg = (String) insertResult.get("error");
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(msg));
			}
			
			String profileId = readData(salesforceDB, tableName, clientId, clientSecret);
			if (profileId != null && !profileId.isEmpty()) {
				responseMap.put("id", profileId);
				responseMap.put("success", true);
			} else {
				responseMap.put("id", null);
                responseMap.put("success", false);
			}
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
            String error = "Error in executing the reactor due to: " + e.getMessage();
            throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}
	
	private String readData(IRDBMSEngine salesforceDB, String tableName, String clientId, String clientSecret) {
		if (!(TABLE.equals(tableName))) {
            classLogger.error("Invalid table name");
            throw new IllegalArgumentException("Invalid table name");
        }
		
		String profileKey = null;
		String query = "SELECT " + SALESFORCE_UNIQUE_ID + " FROM " + tableName
	            + " WHERE CLIENTID=? AND CLIENTSECRET=?";
		
		try (Connection conn = salesforceDB.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, clientId);
            pstmt.setString(2, clientSecret);
            
            try (ResultSet rs = pstmt.executeQuery()) {
            	if (rs.next()) {
                    profileKey = rs.getString(SALESFORCE_UNIQUE_ID);
                }
            }
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
            throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return profileKey;    
	}
	
	private HashMap<String, Object> insertData(IRDBMSEngine salesforceDB, String tableName, String id,
			String clientId, String clientSecret, String alias) {
		HashMap<String, Object> map = new HashMap<>();
        boolean flag = false;

        if (!(TABLE.equals(tableName))) {
        	map.put("status", false);
            map.put("error", "Invalid table name");
            return map;
        }
        
        // check if CLIENTID or ALIAS already exists
        String checkQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE CLIENTID=? OR ALIAS=?";
        try (Connection conn = salesforceDB.getConnection();
             PreparedStatement checkStmt = conn.prepareStatement(checkQuery)) {

			checkStmt.setString(1, clientId);
			checkStmt.setString(2, alias);

            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    String msg = "Error: ALIAS '" + alias + "' already exists for this user.";
                    map.put("status", false);
                    map.put("error", msg);
                    return map;
                }
            }
            
            // doing the insert
         	String insertQuery = "INSERT INTO " + tableName
         			+ " (ID, ALIAS, CLIENTID, CLIENTSECRET) VALUES (?, ?, ?, ?)";
         	
         	try (PreparedStatement insertStmt = conn.prepareStatement(insertQuery)) {
         		insertStmt.setString(1, id);
				insertStmt.setString(2, alias);
				insertStmt.setString(3, clientId);
				insertStmt.setString(4, clientSecret);

                int rowsInserted = insertStmt.executeUpdate();
                flag = rowsInserted > 0;
                map.put("status", flag);
         	}
	    } catch (Exception e) {
	    	classLogger.error(Constants.STACKTRACE, e);
            map.put("status", false);
            map.put("error", e.getMessage());
            // Don't throw here, just return the map with error info
	    }
        return map;
	}
	
	@Override
    public String getReactorDescription() {
        return "This reactor is used for inserting Salesforce Connections";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(CLIENT_ID)) {
            return "Client Id used for Salesforce API authentication.";
        } else if (key.equals(CLIENT_SECRET)) {
            return "Client Secret used for Salesforce API authentication.";
        } else if (key.equals(ALIAS)) {
            return "Unique key name to identify this Salesforce connection entry.";
        }
        return super.getDescriptionForKey(key);
    }
}
