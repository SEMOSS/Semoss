/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.auth.utils.reactors.admin;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryRateLimitUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetQueryRateLimitsReactor extends AbstractReactor {

	public AdminGetQueryRateLimitsReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		List<Map<String, Object>> limits = SecurityQueryRateLimitUtils.getAllQueryRateLimits();
		return new NounMetadata(limits, PixelDataType.FORMATTED_DATA_SET);
	}
}
