package prerna.reactor.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GetProjectUsersListReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetProjectUsersListReactor.class);

	public GetProjectUsersListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.LIMIT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);

		if (projectId == null) {
			throw new IllegalArgumentException("Please define the project id.");
		}
		// check user permission for the database
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user) && !SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"User does not have permission to view access requests for this project");
		}

		Map<String, Object> ret = new HashMap<String, Object>();
		try {
			long totalMembers = SecurityProjectUtils.getProjectUsersCount(user, projectId, null, null);
			long limitMembers = 0L;
			if (limit == null) {
				limitMembers = totalMembers;
			} else {
				limitMembers = Long.parseLong(limit);
			}

			List<Map<String, Object>> members = SecurityProjectUtils.getProjectUsers(user, projectId, null, null,
					limitMembers, 0);

			ret.put("totalMembers", totalMembers);
			ret.put("members", members);
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

}
