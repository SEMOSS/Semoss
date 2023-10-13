package prerna.reactor.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityGroupProjectUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RequestableProjectsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// check security
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to get requestable projects",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
			
		Map<String, Object> projects = new HashMap<>();
		// get the projects the user has access to
		Set<String> allUserProjects = SecurityProjectUtils.getProjectsUserHasExplicitAccess(user);
		// get group projects
		List<String> userGroupProjects = SecurityGroupProjectUtils.getAllUserGroupProjects(user);
		allUserProjects.addAll(userGroupProjects);
		// get info for all Projects the user has access to
		List<Map<String, Object>> projectAccessInfo = SecurityProjectUtils.getProjectInfo(allUserProjects);
		projects.put("HAS_PERMISSION", projectAccessInfo);
		// get the projects that the user does not have access to but can request access
		List<Map<String, Object>> requestableProjects = SecurityProjectUtils.getUserRequestableProjects(allUserProjects);
		projects.put("CAN_REQUEST", requestableProjects);
		return new NounMetadata(projects, PixelDataType.MAP);
	}

}
