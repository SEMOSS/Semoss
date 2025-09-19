package prerna.reactor.notification;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityNotificationUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetNotificationsReactor extends AbstractReactor{

	public GetNotificationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey()};
		this.keyRequired = new int[] {0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String recipient = this.insight.getUserId();
		String limit = this.keyValue.get( ReactorKeysEnum.LIMIT.getKey());
		String offset = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		List<Map<String, Object>> allNotifications = null;
		   allNotifications = SecurityNotificationUtils.getAllNotifications(recipient, limit, offset);
        if(!allNotifications.isEmpty()){ 
        	SecurityNotificationUtils.updateActiontypeForUserNotifications(recipient);
        }
		
		return new NounMetadata(allNotifications, PixelDataType.MAP);
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches all the notifications for logged-in user";
	}
}
