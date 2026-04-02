package prerna.io.connector.jira;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminExternalConnectorsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminJiraInsertConnectionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminJiraInsertConnectionsReactor.class);

	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String SCOPE = "scope";
	private static final String USER_PROFILE_URL = "userProfileUrl";

	public AdminJiraInsertConnectionsReactor() {
		this.keysToGet = new String[] { CLIENT_ID, CLIENT_SECRET, SCOPE, USER_PROFILE_URL };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminExternalConnectorsUtils adminUtils = SecurityAdminExternalConnectorsUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		this.organizeKeys();

		String clientId = this.keyValue.get(this.keysToGet[0]);
		String clientSecret = this.keyValue.get(this.keysToGet[1]);
		String scope = this.keyValue.get(this.keysToGet[2]);
		String userProfileUrl = this.keyValue.get(this.keysToGet[3]);

		Map<Object, Object> responseMap = new HashMap<>();
		try {
			String connectionId = adminUtils.insertJiraConnection(clientId, clientSecret, scope, userProfileUrl);
			responseMap.put("id", connectionId);
			responseMap.put("success", connectionId != null && !connectionId.isEmpty());
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to insert Jira connection.", e);
			String error = "Error inserting Jira connection: " + e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}

	@Override
	public String getReactorDescription() {
		return "Creates and stores a Jira connection entry (client id, client secret, scope, and user profile URL).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (CLIENT_ID.equals(key)) {
			return "Required Jira connected-app client id.";
		} else if (CLIENT_SECRET.equals(key)) {
			return "Required Jira connected-app client secret.";
		} else if (SCOPE.equals(key)) {
			return "Required OAuth scope for the Jira connection.";
		} else if (USER_PROFILE_URL.equals(key)) {
			return "Required URL used to retrieve the Jira user profile.";
		}
		return super.getDescriptionForKey(key);
	}
}
