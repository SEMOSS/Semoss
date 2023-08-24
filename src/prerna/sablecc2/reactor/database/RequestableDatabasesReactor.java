package prerna.sablecc2.reactor.database;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityGroupEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class RequestableDatabasesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// check security
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to get requestable apps",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		Map<String, Object> dbs = new HashMap<>();
		// get the dbs the user has access to
		Set<String> allUserDbs = SecurityEngineUtils.getDatabasesUserHasExplicitAccess(user);
		// get group dbs
		List<String> userGroupDbs = SecurityGroupEngineUtils.getAllUserGroupDatabases(user);
		allUserDbs.addAll(userGroupDbs);
		// get info for all dbs the user has access to
		List<Map<String, Object>> dbAccessInfo = SecurityEngineUtils.getEngineInfo(allUserDbs);
		dbs.put("HAS_PERMISSION", dbAccessInfo);
		// get the dbs that the user does not have access to but can request access
		List<Map<String, Object>> requestableDbs = SecurityEngineUtils.getUserRequestableEngines(allUserDbs);
		dbs.put("CAN_REQUEST", requestableDbs);
		return new NounMetadata(dbs, PixelDataType.MAP);
	}

}
