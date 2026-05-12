package prerna.auth.utils.reactors.admin;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityRoomTokenUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminSetRoomTokenLimitReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";
	private static final String MAX_TOKENS_KEY = "maxTokens";
	private static final String MAX_INPUT_TOKENS_KEY = "maxInputTokens";
	private static final String MAX_OUTPUT_TOKENS_KEY = "maxOutputTokens";
	private static final String IS_ACTIVE_KEY = "isActive";

	public AdminSetRoomTokenLimitReactor() {
		this.keysToGet = new String[] { USER_ID_KEY, MAX_TOKENS_KEY, MAX_INPUT_TOKENS_KEY, MAX_OUTPUT_TOKENS_KEY, IS_ACTIVE_KEY };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		// For demo: allow any user. For production: uncomment admin check.
		// SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		// if (adminUtils == null) {
		// 	throw new IllegalArgumentException("User must be an admin to perform this function");
		// }

		organizeKeys();
		String userId = this.keyValue.get(USER_ID_KEY);
		if (userId != null && userId.trim().isEmpty()) {
			userId = null;
		}

		long maxTokens = getLongValue(MAX_TOKENS_KEY, -1);
		long maxInputTokens = getLongValue(MAX_INPUT_TOKENS_KEY, -1);
		long maxOutputTokens = getLongValue(MAX_OUTPUT_TOKENS_KEY, -1);
		boolean isActive = getBooleanValue(IS_ACTIVE_KEY, true);

		String createdBy = user.getAccessToken(user.getLogins().get(0)).getId();

		if (userId == null) {
			SecurityRoomTokenUtils.setDefaultRoomTokenLimit(maxTokens, maxInputTokens, maxOutputTokens, isActive, createdBy);
		} else {
			SecurityRoomTokenUtils.setUserRoomTokenLimit(userId, maxTokens, maxInputTokens, maxOutputTokens, isActive, createdBy);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("success", true);
		result.put("userId", userId);
		result.put("maxTokens", maxTokens);
		result.put("maxInputTokens", maxInputTokens);
		result.put("maxOutputTokens", maxOutputTokens);
		result.put("isActive", isActive);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	private long getLongValue(String key, long defaultVal) {
		String val = this.keyValue.get(key);
		if (val == null || val.trim().isEmpty()) {
			return defaultVal;
		}
		try {
			return Long.parseLong(val.trim());
		} catch (NumberFormatException e) {
			return defaultVal;
		}
	}

	private boolean getBooleanValue(String key, boolean defaultVal) {
		String val = this.keyValue.get(key);
		if (val == null || val.trim().isEmpty()) {
			return defaultVal;
		}
		return Boolean.parseBoolean(val.trim());
	}
}
