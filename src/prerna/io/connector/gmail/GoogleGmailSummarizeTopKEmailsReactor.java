package prerna.io.connector.gmail;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleGmailSummarizeTopKEmailsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailSummarizeTopKEmailsReactor.class);
	
	public GoogleGmailSummarizeTopKEmailsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String limitStr = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			int limit = Integer.parseInt(limitStr);
			List<Map<String, Object>> result = GoogleGmailHelper.summarizeTopKEmails(accessToken, limit);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred summarizing the emails. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Summarize the top k emails";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The limit for the number of emails to summarize";
		}
		return super.getDescriptionForKey(key);
	}
}
