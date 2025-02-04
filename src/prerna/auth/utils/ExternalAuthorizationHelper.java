package prerna.auth.utils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class ExternalAuthorizationHelper {

	private static final Logger classLogger = LogManager.getLogger(ExternalAuthorizationHelper.class);

	/**
	 * 
	 * @param user
	 */
	public static void updateEnginePermissionsBasedOnApiCall(User user) {
		try {
			//get the logged in  user emailId
			String emailId = user.getAccessToken(user.getLogins().get(0)).getEmail();
			classLogger.info("Logged in user email id : " + emailId);

			//Call client API to get api Response
			String apiResponse = getClientApiJsonResponse(emailId);

			//Transform api response
			List<Map<String, Object>> enginePermissions = transformApiResponse(user, apiResponse);

			//update permissions for engine
			SecurityEngineUtils.updateEngineUserPermissions(user, enginePermissions);
			classLogger.info("Engine permissions update for userid = " + User.getSingleLogginName(user));
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating the engine permissions");
		}
	}
	
	/**
	 * 
	 * @param emailId
	 * @return
	 * @throws Exception
	 */
	private static String getClientApiJsonResponse(String emailId) throws Exception {
		String url = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_URL);
		String requestKey = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_REQUEST_KEY);
		
		JSONObject requestBody = new JSONObject();
		requestBody.put(requestKey, emailId);

		String username = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_AUTH_USERNAME);
		String password = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_AUTH_PASSWORD);
		String basicAuth = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());

		Map<String, String> headersMap = new HashMap<>();
		headersMap.put("Authorization", "Basic " + basicAuth);
		headersMap.put("Accept", "application/json");
		headersMap.put("Content-Type", "application/json");

		return HttpHelperUtility.postRequestStringBody(url, headersMap, requestBody.toString(), ContentType.APPLICATION_JSON, 
				null, null, null);
	}

	/**
	 * 
	 * @param user
	 * @param apiResponse
	 * @return
	 */
	private static List<Map<String, Object>> transformApiResponse(User user, String apiResponse) {
		List<Map<String, Object>> enginePermissions = new ArrayList<>();
		try {
			// Parse and Manipulate JSON Response
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(apiResponse);
			Iterator<String> fieldNames = rootNode.fieldNames();
			String firstKey = fieldNames.next();
			JsonNode detailNode = rootNode.path(firstKey);

			for (JsonNode detail : detailNode) {
				Map<String, Object> permissionMap = new HashMap<>();
				String targetSystem = detail.path("targetSystem").asText();
				RdbmsTypeEnum engineSubType = RdbmsTypeEnum.getEnumFromString(targetSystem);
				if (engineSubType != null) {
					permissionMap.put("engineSubType", engineSubType);
				} else {
					classLogger.warn("Engine sub type not found for targetSystem : " + targetSystem 
							+ " which was returned for user " + User.getSingleLogginName(user));
					// ignoring
					continue;
				}
				permissionMap.put("engineId", detail.path("dataCollectionId").asText());
				permissionMap.put("engineName", detail.path("dataCollectionName").asText());

				String defaultPermission = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_DEFAULT_PERMISSION);
				if(defaultPermission == null || (defaultPermission=defaultPermission.trim()).isEmpty()) {
					defaultPermission = AccessPermissionEnum.READ_ONLY.getPermission();
				}
				permissionMap.put("permission", defaultPermission);

				enginePermissions.add(permissionMap);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return enginePermissions;
	}

}
