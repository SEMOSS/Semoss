package prerna.io.connector.gmail;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSummarizeTopKEmailsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleSummarizeTopKEmailsReactor.class);
	
	public GoogleSummarizeTopKEmailsReactor() {
		this.keysToGet = new String[] { "number" };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String number = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			int num = Integer.parseInt(number);
			List<Map<String, Object>> result = GoogleGmailHelper.summarizeTopKEmails(accessToken, num);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to summarize the top k email";
	}

}
