package prerna.io.connector.salesforce;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class SalesforceUtils {

	private static final Logger classLogger = LogManager.getLogger(SalesforceUtils.class);

	public static String getSalesforceAccessToken(User user) throws Exception {
		String accessToken = null;

        if (user == null) {
        	classLogger.error("User not found in session.");
            throw new SemossPixelException("User not found in session.");
        }

        AccessToken salesforceToken = user.getAccessToken(AuthProvider.SALESFORCE);

        if (salesforceToken == null) {
        	classLogger.error("No Salesforce Access Token fetched for user");
            throw new SemossPixelException("No Salesforce Access Token fetched.");
        }

        accessToken = salesforceToken.getAccess_token();
        return accessToken;
	}
	
	public static String getSalesforceInstanceUrl(User user) throws Exception {
		String instanceUrl = null;

        if (user == null) {
        	classLogger.error("User not found in session.");
            throw new SemossPixelException("User not found in session.");
        }

        AccessToken salesforceToken = user.getAccessToken(AuthProvider.SALESFORCE);

        if (salesforceToken == null) {
        	classLogger.error("No Salesforce Access Token fetched for user");
            throw new SemossPixelException("No Salesforce Access Token fetched.");
        }

        instanceUrl = salesforceToken.getInstance_url();
        
        if (instanceUrl == null) {
        	classLogger.error("Instance URL is missing.");
            throw new SemossPixelException("Instance URL is missing.");
        }
        
        return instanceUrl;
	}
}
