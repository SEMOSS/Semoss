/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.auth.utils.reactors.admin;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryRateLimitUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminSetQueryRateLimitReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";
	private static final String USAGE_FREQUENCY_KEY = "usageFrequency";
	private static final String MAX_REQUESTS_KEY = "maxRequests";
	private static final String IS_ACTIVE_KEY = "isActive";

	public AdminSetQueryRateLimitReactor() {
		this.keysToGet = new String[] { USER_ID_KEY, USAGE_FREQUENCY_KEY, MAX_REQUESTS_KEY, IS_ACTIVE_KEY };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String userId = this.keyValue.get(USER_ID_KEY);
		if (userId != null && userId.trim().isEmpty()) {
			userId = null;
		}

		String usageFrequency = this.keyValue.get(USAGE_FREQUENCY_KEY);
		long maxRequests = getLongValue(MAX_REQUESTS_KEY, -1);
		boolean isActive = getBooleanValue(IS_ACTIVE_KEY, true);
		String createdBy = user.getAccessToken(user.getLogins().get(0)).getId();

		if (userId == null) {
			SecurityQueryRateLimitUtils.setDefaultQueryRateLimit(usageFrequency, maxRequests, isActive, createdBy);
		} else {
			SecurityQueryRateLimitUtils.setUserQueryRateLimit(userId, usageFrequency, maxRequests, isActive, createdBy);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("success", true);
		result.put("userId", userId);
		result.put("usageFrequency", usageFrequency);
		result.put("maxRequests", maxRequests);
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
