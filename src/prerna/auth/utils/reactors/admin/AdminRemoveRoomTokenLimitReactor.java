package prerna.auth.utils.reactors.admin;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityRoomTokenUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminRemoveRoomTokenLimitReactor extends AbstractReactor {

	private static final String USER_ID_KEY = "userId";

	public AdminRemoveRoomTokenLimitReactor() {
		this.keysToGet = new String[] { USER_ID_KEY };
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
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide a userId to remove the room token limit override");
		}

		SecurityRoomTokenUtils.removeUserRoomTokenLimit(userId);

		Map<String, Object> result = new HashMap<>();
		result.put("success", true);
		result.put("userId", userId);
		return new NounMetadata(result, PixelDataType.MAP);
	}
}
