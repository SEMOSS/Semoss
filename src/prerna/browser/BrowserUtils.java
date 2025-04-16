package prerna.browser;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrowserUtils {
	
	public static void ensureUserLoggedIn(User user) {
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User is not logged in. Cannot open URL.", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
	}
	
	public static boolean anonymousEnabledAndUserAnonymous(User user) {
		return AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous();
	}

	
	public static String getNonNullString(Map<String, String> keyValue, String key) {
		String res = keyValue.get(key);
		if (res == null) {
			String error = "KeyValue for <" + key + "> cannot be null.";
			throw new IllegalArgumentException(error);
		}
		return res;
	}
	
	public static int getNonNullInt(Map<String, String> keyValue, String key) {
		String val = getNonNullString(keyValue, key);
		return Integer.parseInt(val);
	}
	
}
