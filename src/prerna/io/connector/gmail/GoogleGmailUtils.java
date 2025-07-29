package prerna.io.connector.gmail;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class GoogleGmailUtils {

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailUtils.class);
	
	public static String getGoogleAccessToken(User user) throws Exception {
        String accessToken = null;

        if (user == null) {
        	classLogger.error("User not found in session.");
            throw new SemossPixelException("User not found in session.");
        }

        AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);

        if (googleToken == null) {
        	classLogger.error("No Google Access Token fetched for user");
            throw new SemossPixelException("No Google Access Token fetched.");
        }

        accessToken = googleToken.getAccess_token();
        return accessToken;
    }
}
