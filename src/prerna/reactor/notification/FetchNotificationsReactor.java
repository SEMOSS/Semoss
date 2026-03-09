package prerna.reactor.notification;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FetchNotificationsReactor extends AbstractReactor {

	public FetchNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String limit = this.keyValue.get(ReactorKeysEnum.LIMIT.getKey());
		String offset = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account to retrieve the function engine files",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
			throwAnonymousUserError();
		}

		List<Map<String, Object>> allNotifications = null;
		allNotifications = NotificationDbUtils.fetchAllNotifications(user, limit, offset);
		if (!allNotifications.isEmpty()) {
			NotificationDbUtils.resetNotificationActionType(user);
		}

		return new NounMetadata(allNotifications, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Fetch all user notifications";
	}
}
