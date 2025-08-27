package prerna.reactor.project.notification;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateReadNotificationsReactor extends AbstractReactor {
	// TODO: CAN ADD LOGGER
	public UpdateReadNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NOTIFICATION_ID.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		String notificationId = keyValue.get(ReactorKeysEnum.NOTIFICATION_ID.getKey());
		
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
     
		SecurityProjectUtils.updateReadNotificationForUser(notificationId);
		
		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Updates the notification as read by user";
	}
}
