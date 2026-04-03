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

	private static final String ALIAS = "alias";
	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String SCOPE = "scope";
	private static final String USER_PROFILE_URL = "userProfileUrl";

	public AdminJiraInsertConnectionsReactor() {
		this.keysToGet = new String[] { ALIAS, CLIENT_ID, CLIENT_SECRET, SCOPE, USER_PROFILE_URL };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminExternalConnectorsUtils adminUtils = SecurityAdminExternalConnectorsUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		this.organizeKeys();

		String alias = this.keyValue.get(this.keysToGet[0]);
		String clientId = this.keyValue.get(this.keysToGet[1]);
		String clientSecret = this.keyValue.get(this.keysToGet[2]);
		String scope = this.keyValue.get(this.keysToGet[3]);
		String userProfileUrl = this.keyValue.get(this.keysToGet[4]);

		Map<Object, Object> responseMap = new HashMap<>();
		try {
			String connectionId = adminUtils.insertJiraConnection(alias, clientId, clientSecret, scope, userProfileUrl);
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
		return "Creates a Jira OAuth connection definition in the SEMOSS security database. Use this only when an administrator is registering a new Jira connector configuration; do not use it to authenticate a user or to fetch Jira projects or issues. Returns a map with id and success. Preconditions: the caller must be a SEMOSS admin and must provide valid Jira app configuration values.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ALIAS.equals(key)) {
			return "Required. Human-readable unique alias for this stored Jira connection, for example 'Corporate Jira Cloud'. This value is stored in SEMOSS and later returned by GetJiraConnectionsReactor. If the alias is missing or duplicates an existing Jira connection alias, the insert fails.";
		} else if (CLIENT_ID.equals(key)) {
			return "Required. Jira OAuth client ID exactly as issued by the Atlassian app configuration. This is not a project key, not a user token, and not a SEMOSS identifier. If it is wrong or missing, later Jira authentication flows for this connection will fail.";
		} else if (CLIENT_SECRET.equals(key)) {
			return "Required. Jira OAuth client secret exactly as issued by the Atlassian app configuration. Keep the value exactly as configured in Atlassian. If it is wrong or missing, token exchange for this connection will fail.";
		} else if (SCOPE.equals(key)) {
			return "Required. Space-delimited Atlassian OAuth scope string, for example 'read:jira-user read:jira-work write:jira-work offline_access'. Copy this from the Jira app configuration. If required scopes are omitted or wrong, downstream Jira project or issue reactors can fail with authorization errors.";
		} else if (USER_PROFILE_URL.equals(key)) {
			return "Required. Full HTTPS URL used by this Jira connector to retrieve the authenticated user's profile. Copy the exact endpoint from the connector setup or Atlassian app configuration. If this URL is wrong or missing, profile lookup and authentication flows can fail.";
		}
		return super.getDescriptionForKey(key);
	}
}
