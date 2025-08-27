package prerna.reactor.project.notification;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetNotificationsReactor extends AbstractReactor{

	@Override
	public NounMetadata execute() {
		organizeKeys(); //
		//User user = this.insight.getUser();
		String recipient = this.insight.getUserId();
		
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		List<Map<String, Object>> allNotifications = null;
		   allNotifications = SecurityProjectUtils.getAllNotifications(recipient);
        if(allNotifications != null) {
		   SecurityProjectUtils.updateActiontypeForUserNotifications(recipient);
        }
		
		return new NounMetadata(allNotifications, PixelDataType.MAP);
	}
	
	@Override
	public String getReactorDescription() {
		return "Fetches all the notifications for logged-in user";
	}

}
