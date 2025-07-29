package prerna.io.connector.gmail;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGmailProfileByIdReactor extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailProfileByIdReactor.class);

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			return GoogleGmailHelper.getGmailProfileById(accessToken);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the GmailProfile details of the user";
	}

}
