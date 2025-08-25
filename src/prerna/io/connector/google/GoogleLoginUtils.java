package prerna.io.connector.google;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public final class GoogleLoginUtils {
	
	private GoogleLoginUtils() {
		
	}
	
	/**
	 * 
	 * @param user
	 * @return
	 * @throws Exception
	 */
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
	
	/**
	 * 
	 * @param details
	 * @throws SemossPixelException
	 */
	public static void throwLoginError(Map<String, Object> details) throws SemossPixelException {
		SemossPixelException exception = new SemossPixelException(NounMetadata.getErrorNounMessage(details, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}
}
