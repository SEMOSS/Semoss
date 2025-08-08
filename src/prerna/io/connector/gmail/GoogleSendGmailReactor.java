package prerna.io.connector.gmail;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSendGmailReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleSendGmailReactor.class);

	public GoogleSendGmailReactor() {
		this.keysToGet = new String[] { "toemail", "subject", "body" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		this.organizeKeys();
		String toemail = this.keyValue.get(this.keysToGet[0]);
		String subject = this.keyValue.get(this.keysToGet[1]);
		String body = this.keyValue.get(this.keysToGet[2]);

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			return GoogleGmailHelper.sendEmail(accessToken, subject, body, toemail);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}

	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to send email.";
	}
	
}
