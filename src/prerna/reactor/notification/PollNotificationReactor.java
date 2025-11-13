package prerna.reactor.notification;

import java.util.List;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.notifications.NotificationDbUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PollNotificationReactor extends AbstractReactor {
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();

        if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
            throwAnonymousUserError();
        }

        List<Pair<String, String>> userIdAndTypeList = User.getUserIdAndType(user);
        if (userIdAndTypeList == null || userIdAndTypeList.isEmpty()) {
            throw new SemossPixelException(
                new NounMetadata("Unable to determine user type for deletion", 
                PixelDataType.CONST_STRING, 
                PixelOperationType.ERROR, 
                PixelOperationType.LOGGIN_REQUIRED_ERROR)
            );
        }

        String recipientId = userIdAndTypeList.get(0).getValue0();
        String recipientType = userIdAndTypeList.get(0).getValue1();
		int newNotificationCount = NotificationDbUtils.fetchNewNotificationCount(recipientId, recipientType);
		return new NounMetadata(newNotificationCount, PixelDataType.CONST_INT);
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor returns number of new notifications for logged-in";
	}
}
