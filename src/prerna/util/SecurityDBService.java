package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.model.OAuthConfig;

public class SecurityDBService {

	private static final String SERVICENOW_UNIQUE_ID = "ID";
	private static final String TABLE = "SERVICENOW";
	
	public static OAuthConfig getServiceNowOAuthConfigById(String id) {
		OAuthConfig config = null;
        try {
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = "SERVICENOW"; // You could use a lookup if needed

            IRDBMSEngine serviceNowDB = (RDBMSNativeEngine) database;
            String query = "SELECT * FROM " + tableName + " WHERE " + SERVICENOW_UNIQUE_ID + " = ?";
            try (Connection conn = serviceNowDB.makeConnection();
                 PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, id);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        config = new OAuthConfig();
                        config.setClientId(rs.getString("CLIENT_ID"));
                        config.setClientSecret(rs.getString("CLIENT_SECRET"));
                        config.setRedirectUri(rs.getString("REDIRECT_URL"));
                        config.setInstanceUrl(rs.getString("INSTANCE_URL"));
                        config.setScope(rs.getString("SCOPE"));
                        config.setCodeChallengeMethod(rs.getString("CODE_CHALLENGE_METHOD"));
                        config.setUserinfoUrl(rs.getString("USER_INFO_URL"));
                        config.setBeanProps(rs.getString("BEANPROPS"));
                        config.setJsonPattern(rs.getString("JSONPATTERN"));
                        config.setAutoAdd(rs.getString("AUTO_ADD"));
                        config.setLoginAllowed(rs.getString("LOGIN_APPLICABLE"));
                    }
                }
            }
        } catch (Exception e) {
            // Log error, handle as needed
            e.printStackTrace();
        }
        return config;
    }
	
	public boolean isServiceNowLoginAllowed(String id) {
        boolean allowed = false;
        try {
            IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
            String tableName = TABLE;

            IRDBMSEngine serviceNowDB = (RDBMSNativeEngine) database;
            String query = "SELECT LOGIN_ALLOWED FROM " + tableName + " WHERE " + SERVICENOW_UNIQUE_ID + " = ?";
            try (Connection conn = serviceNowDB.makeConnection();
                 PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, id);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        allowed = rs.getBoolean("LOGIN_ALLOWED");
                    }
                }
            }
        } catch (Exception e) {
            // Log error, handle as needed
            e.printStackTrace();
        }
        return allowed;
    }

}
