/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.auth.external;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class ExternalAuthorizationHelper {

	private static final Logger classLogger = LogManager.getLogger(ExternalAuthorizationHelper.class);

	/**
	 * @param user
	 * @throws Exception
	 */
	public static void updateEnginePermissionsBasedOnApiCall(User user) throws Exception {
		try {
			// get the logged in user emailId
			String emailId = user.getAccessToken(user.getLogins().get(0)).getEmail();
			classLogger.info("Logged in user email id : " + emailId);

			// Call client API to get api Response
			String apiResponse = getClientApiJsonResponse(emailId);

			// Transform api response
			List<Map<String, Object>> enginePermissions = transformApiResponse(user, apiResponse);

			// Update permissions for engine
			List<Map<String, Object>> newEngines = SecurityEngineUtils.updateEngineUserPermissions(user,
					enginePermissions);
			classLogger.info("Engine permissions update for userid = " + User.getSingleLogginName(user));

			// Create SMSS for this engine
			for (Map<String, Object> newE : newEngines) {
				String engineId = (String) newE.get("engineId");
				String engineName = (String) newE.get("engineName");
				IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) newE.get("engineType");
				String engineSubType = (String) newE.get("engineSubType");
				Map<String, Object> properties = null;

				// TODO: need to expand on logic for the class to initialize
				String engineClass = null;
				if (engineType == IEngine.CATALOG_TYPE.DATABASE) {
					engineClass = RDBMSNativeEngine.class.getName();

					properties = new HashMap<>();
					properties.put(Constants.RDBMS_TYPE, engineSubType);
					properties.put(Constants.OWL, Constants.DATABASE_FOLDER + "/@ENGINE@/" + engineName + "_OWL.OWL");
				}

				File tempSmss = UploadUtilities.createTemporaryEngineSmss(engineType, engineId, engineName, engineClass,
						properties);
				DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
				File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
				FileUtils.copyFile(tempSmss, smssFile);
				DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, smssFile.getAbsolutePath());
				tempSmss.delete();

				// also make the folder to persist... even if empty
				String engineFolder = EngineUtility.getSpecificEngineBaseFolder(engineType, engineId, engineName);
				File eFolder = new File(engineFolder);
				eFolder.mkdir();

				ClusterUtil.pushEngine(engineId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	/**
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

		return HttpHelperUtility.postRequestStringBody(url, headersMap, requestBody.toString(),
				ContentType.APPLICATION_JSON, null, null, null);
	}

	/**
	 * @param user
	 * @param apiResponse
	 * @return
	 */
	private static List<Map<String, Object>> transformApiResponse(User user, String apiResponse) {
		String defaultPermission = Utility
				.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_DEFAULT_PERMISSION);
		if (defaultPermission == null || (defaultPermission = defaultPermission.trim()).isEmpty()) {
			defaultPermission = AccessPermissionEnum.READ_ONLY.getPermission();
		}
		IEngine.CATALOG_TYPE defaultEType = null;
		String defaultETypeStr = Utility
				.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_DEFAULT_ENGINE_TYPE);
		if (defaultETypeStr != null && !(defaultETypeStr = defaultETypeStr.trim()).isEmpty()) {
			try {
				defaultEType = IEngine.CATALOG_TYPE.valueOf(defaultETypeStr);
			} catch (Exception e) {
				classLogger.warn("Invalid " + Constants.EXTERNAL_PERMISSION_MANAGEMENT_DEFAULT_ENGINE_TYPE + " value = "
						+ defaultEType);
			}
		}

		final String ENGINEID_KEY = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINEID);
		if (ENGINEID_KEY == null || ENGINEID_KEY.isEmpty()) {
			throw new IllegalArgumentException(
					"Must have a valid value for " + Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINEID);
		}
		final String ENGINENAME_KEY = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINENAME);
		if (ENGINENAME_KEY == null || ENGINENAME_KEY.isEmpty()) {
			throw new IllegalArgumentException(
					"Must have a valid value for " + Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINENAME);
		}
		final String JMES_PATH_EXPRESSION = Utility
				.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_RESPONSE_JMES_PATH);
		if (JMES_PATH_EXPRESSION == null || JMES_PATH_EXPRESSION.isEmpty()) {
			throw new IllegalArgumentException(
					"Must have a valid value for " + Constants.EXTERNAL_PERMISSION_MANAGEMENT_RESPONSE_JMES_PATH);
		}
		final String ENGINETYPE_KEY = Utility.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINETYPE);
		final String ENGINESUBTYPE_KEY = Utility
				.getDIHelperProperty(Constants.EXTERNAL_PERMISSION_MANAGEMENT_ENGINESUBTYPE);

		List<Map<String, Object>> enginePermissions = new ArrayList<>();
		try {
			// Parse and Manipulate JSON Response
			JsonNode parsedJsonNode = BeanFiller.getJmesResult(apiResponse, JMES_PATH_EXPRESSION);
			if (parsedJsonNode == null) {
				throw new IllegalArgumentException(
						"Unable to process api response = " + apiResponse + " to determine user permissions");
			}
			for (JsonNode detail : parsedJsonNode) {
				Map<String, Object> permissionMap = new HashMap<>();

				// these are mandatory
				permissionMap.put("engineId", detail.path(ENGINEID_KEY).asText());
				permissionMap.put("engineName", detail.path(ENGINENAME_KEY).asText());

				IEngine.CATALOG_TYPE engineType = null;
				if (ENGINETYPE_KEY != null && !ENGINETYPE_KEY.isEmpty() && detail.has(ENGINETYPE_KEY)) {
					String engineTypeStr = detail.path(ENGINETYPE_KEY).asText();
					try {
						engineType = IEngine.CATALOG_TYPE.valueOf(engineTypeStr);
					} catch (Exception e) {
						classLogger.warn("Engine type not found for value : " + engineTypeStr
								+ " which was returned for user " + User.getSingleLogginName(user));
					}
				}
				if (engineType == null) {
					engineType = defaultEType;
				}
				permissionMap.put("engineType", engineType);

				String engineSubType = null;
				if (ENGINESUBTYPE_KEY != null && !ENGINESUBTYPE_KEY.isEmpty() && detail.has(ENGINESUBTYPE_KEY)) {
					String engineSubTypeStr = detail.path(ENGINESUBTYPE_KEY).asText();
					if (engineType == IEngine.CATALOG_TYPE.DATABASE) {
						RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.getEnumFromString(engineSubTypeStr);
						if (rdbmsType != null) {
							engineSubType = rdbmsType.getLabel();
						} else {
							classLogger.warn("Engine sub type not found for value : " + engineSubTypeStr
									+ " which was returned for user " + User.getSingleLogginName(user));
						}
					} else {
						// TODO: add future validation for other engine types ...
						engineSubType = engineSubTypeStr;
					}
				}
				permissionMap.put("engineSubType", engineSubType);

				permissionMap.put("permission", defaultPermission);
				enginePermissions.add(permissionMap);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return enginePermissions;
	}
}
