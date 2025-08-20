package prerna.io.connector.gmail;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleGmailProfileByIdReactor extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailProfileByIdReactor.class);

	public GoogleGmailProfileByIdReactor() {
		this.keysToGet = new String[] {};
	}
	
	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			Map<String, Object> retMap = GoogleGmailHelper.getGmailProfileById(accessToken);
	        return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred getting the user profile details. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Get the user profile details";
	}
	
}
