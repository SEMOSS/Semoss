package prerna.io.connector.google.docs;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleDocsListReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleDocsListReactor.class);
	
	public GoogleDocsListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String limitStr = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			int limit = Integer.parseInt(limitStr);
			List<Map<String, Object>> result = GoogleDocsHelper.getDocsList(accessToken, limit);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of document ids. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve Google Docs document ids for User";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The limit for the maximum number of docs";
		}
		return super.getDescriptionForKey(key);
	}
}