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

public class AdminRemoveQueryRateLimitReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";
	private static final String USAGE_FREQUENCY_KEY = "usageFrequency";

	public AdminRemoveQueryRateLimitReactor() {
		this.keysToGet = new String[] { USER_ID_KEY, USAGE_FREQUENCY_KEY };
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
		String usageFrequency = this.keyValue.get(USAGE_FREQUENCY_KEY);
		if (usageFrequency == null || usageFrequency.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a usageFrequency to remove the query rate limit");
		}

		if (userId == null || userId.trim().isEmpty()) {
			SecurityQueryRateLimitUtils.removeDefaultQueryRateLimit(usageFrequency);
			userId = null;
		} else {
			SecurityQueryRateLimitUtils.removeUserQueryRateLimit(userId, usageFrequency);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("success", true);
		result.put("userId", userId);
		result.put("usageFrequency", usageFrequency);
		return new NounMetadata(result, PixelDataType.MAP);
	}
}
