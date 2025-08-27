package prerna.reactor.project.notification;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PollingNotificationsReactor extends AbstractReactor {
	// TODO: CAN ADD LOGGER
	@Override
	public NounMetadata execute() {
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		String userId = this.insight.getUserId();
		int newNotificationCount = SecurityProjectUtils.getNewNotificationCountForLoggedInUser(userId);
		return new NounMetadata(newNotificationCount, PixelDataType.CONST_INT);
	}
	
	@Override
	public String getReactorDescription() {
		return "Deletes all the notifications for the logged-in user";
	}

}
