package prerna.reactor.notification;

import java.util.List;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteNotificationReactor extends AbstractReactor {

	public DeleteNotificationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NOTIFICATION_ID.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
			throwAnonymousUserError();
		}

		organizeKeys();
		String notificationId = this.keyValue.get(this.keysToGet[0]);

		List<Pair<String, String>> userIdAndTypeList = User.getUserIdAndType(user);
		if (userIdAndTypeList == null || userIdAndTypeList.isEmpty()) {
			throw new SemossPixelException(new NounMetadata("Unable to determine user type for deletion",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR));
		}

		String recipientId = userIdAndTypeList.get(0).getValue0();
		String recipientType = userIdAndTypeList.get(0).getValue1();
		int deleteCount;
		if (notificationId != null) {
			deleteCount = NotificationDbUtils.deleteNotification(null, null, notificationId);
		} else {
			deleteCount = NotificationDbUtils.deleteNotification(recipientId, recipientType, null);
		}
		return new NounMetadata(deleteCount, PixelDataType.CONST_INT);
	}

	@Override
	public String getReactorDescription() {
		return "Deletes a user's notification. Takes in a notificatioinId for a single notification or no value for all notifications";
	}
}
