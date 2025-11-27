package prerna.reactor.agent.mcp;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;

public abstract class AbstractBaseMCPReactor extends AbstractReactor {

	/**
	 * 
	 * @param engine
	 * @param engineId
	 * @param user
	 */
	protected void checkSecurity(IEngine engine, String engineId, User user) {
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		if (engine instanceof IProject) {
			if (!SecurityProjectUtils.userCanViewProject(user, engineId)) {
				throw new IllegalArgumentException(
						"Project " + engineId + " does not exist or user does not have access");
			}
		} else {
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Engine " + engineId + " does not exist or user does not have access");
			}
		}
	}

}
