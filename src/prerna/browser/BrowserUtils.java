package prerna.browser;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrowserUtils {
	
	private static final Logger classLogger = LogManager.getLogger(BrowserUtils.class);
	
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
	
	public static String mapToJsonString(Map<String, String> input) {
		ObjectMapper om = new ObjectMapper();
		try {
			return om.writeValueAsString(input);
		} catch (JsonProcessingException e) {
			classLogger.error("Could not parse map with inputs: {}", input.toString());
			throw new IllegalArgumentException("Could not process input and make it json string", e);
		}
	}
	
}
