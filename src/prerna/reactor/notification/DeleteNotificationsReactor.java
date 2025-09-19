package prerna.reactor.notification;


import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityNotificationUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteNotificationsReactor extends AbstractReactor {

	public DeleteNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NOTIFICATION_ID.getKey()};
		this.keyRequired = new int[] {0};
	}
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String notificationId = this.keyValue.get(this.keysToGet[0]);
		String memberId = this.insight.getUserId();
		
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		int deleteCount;
		if(notificationId != null) {
			deleteCount = SecurityNotificationUtils.removeNotifications(null, notificationId);
		}else {
			deleteCount = SecurityNotificationUtils.removeNotifications(memberId, null);
		}
		return new NounMetadata(deleteCount, PixelDataType.CONST_INT);
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor deletes single or multiple notifications for the logged-in user and returns number of deleted notifications";
	}
}
