package prerna.io.connector.gmail;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGmailUtils {
	
	public static String getGoogleAccessToken(User user) throws Exception {

		String accessToken = null;

		try {
			if (user == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);
			} else {
				AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken = googleToken.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		return accessToken;
	}
	
	public static void throwLoginError(Map<String, Object> details) {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
}
