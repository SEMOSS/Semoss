package prerna.graph;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.util.SocialPropertiesUtil;

public class MSGraphAPICall {

	private static final Logger classLogger = LogManager.getLogger(MSGraphAPICall.class);
	private static AccessToken systemAccessToken = null;
	private static long tokenExpirationTime = 0;

	/**
	 *This method use to get users data from graph api with next link having subsequent userdata
	 *@param accessToken, searchTerm, nextLink
	 *@return String
	 */
	public String getUserDetails(AccessToken accessToken, String groupId, String searchTerm, String nextLink) throws Exception {
		classLogger.info("getUserDetails based on Graph Api");

		if (accessToken == null) {
			classLogger.info("Graph call for Access Token is null, will attempt to use system credentials");
			if (systemAccessToken == null || isTokenExpired()) {
				systemAccessToken = refreshSystemAccessToken();
			}
			accessToken = systemAccessToken;
		}

		String uri = "";
		if(nextLink == null) {
			String searchParam = "";
			if(searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty()) {
				searchParam = "$search=" + 
						URLEncoder.encode("\"displayName:" + searchTerm 
								+ "\" OR \"mail:" + searchTerm
								+ "\" OR \"userPrincipalName:" + searchTerm + "\"", 
								java.nio.charset.StandardCharsets.UTF_8.toString())
				+ "&";
			}
			if (groupId == null || groupId.isEmpty()) {
				uri = MicrosoftTokenFiller.MS_GRAPH_BASE_API  + "/v1.0/users?" 
						+ searchParam 
						+ "$orderby=displayName&"
						+ "$top=999&"
						+ "$count=true&"
						+ "$select=displayName,id,mail,userType,givenName,surname&"
						+ "$filter=(userType eq 'Member')";
			} else {
				uri = MicrosoftTokenFiller.MS_GRAPH_BASE_API  + "/v1.0/groups/" + groupId + "/members/microsoft.graph.user?" 
						+ searchParam 
						+ "$orderby=displayName&"
						+ "$top=999&"
						+ "$count=true";
			}
		} else {
			uri = nextLink;
		}
		
		Map<String, String> headerMap = new HashMap<>();
		headerMap.put("Authorization", "Bearer " + accessToken.getAccess_token());
		headerMap.put("Accept", "application/json");
		headerMap.put("ConsistencyLevel", "eventual");
		
		String jsonResponse = HttpHelperUtility.getRequest(uri, headerMap, null, null, null);
		return jsonResponse;
	}


	/**
	 * This method exchanges application-specific access keys and secret keys for an access token.
	 * 
	 * @return The access token.
	 * @throws IOException If an error occurs during the token exchange.
	 */
	private synchronized AccessToken refreshSystemAccessToken() throws IOException {
		String clientId = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_client_id");
		String clientSecret = SocialPropertiesUtil.getInstance().getProperty("ms_graphapi_secret_key");
		String tenantId = SocialPropertiesUtil.getInstance().getProperty("ms_tenant");

		String tokenEndpoint = "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
		String scope = "https://graph.microsoft.com/.default";

		Map<String, String> params = new HashMap<>();
		params.put("client_id", clientId);
		params.put("scope", scope);
		params.put("grant_type", "client_credentials");
		params.put("client_secret", clientSecret);

		AccessToken newToken = HttpHelperUtility.getAccessToken(tokenEndpoint, params, true, true);
		if (newToken != null) {
			// Set the token expiration time based on the current time and the expires_in value, subtracting 5 minutes (300 seconds)
			tokenExpirationTime = System.currentTimeMillis() + ((newToken.getExpires_in() - 300) * 1000);
		}
		return newToken;
	}

	/**
	 * Checks if the current system access token is expired.
	 * 
	 * @return True if the token is expired, false otherwise.
	 */
	private boolean isTokenExpired() {
		return System.currentTimeMillis() >= tokenExpirationTime;
	}

}