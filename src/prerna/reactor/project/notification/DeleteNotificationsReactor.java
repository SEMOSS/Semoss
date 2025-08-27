package prerna.reactor.project.notification;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteNotificationsReactor extends AbstractReactor {
  //TODO: CAN ADD LOGGER
	@Override
	public NounMetadata execute() {
		String memberId = this.insight.getUserId();
		
		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		SecurityProjectUtils.removeAllNotificationsForLoggedInUser(memberId);
		NounMetadata retNoun = NounMetadata.getSuccessNounMessage("Success!");
		return retNoun;
	}
	
	@Override
	public String getReactorDescription() {
		return "Deletes all the notifications for the logged-in user";
	}

}
