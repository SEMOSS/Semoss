package prerna.reactor.project.notification;

import java.sql.Timestamp;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityNotificationUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class UpdateReadNotificationsReactor extends AbstractReactor {

	public UpdateReadNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NOTIFICATION_ID.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String notificationId = this.keyValue.get(this.keysToGet[0]);
	
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		Timestamp readAt = Utility.getCurrentSqlTimestampUTC();
		SecurityNotificationUtils.updateReadNotifications(notificationId, readAt);
		
		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Updates the notification as read by user";
	}
}
