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
package prerna.reactor.security;

import java.util.Map;
import org.javatuples.Pair;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateEngineAppLinkReactor extends AbstractReactor {

	public UpdateEngineAppLinkReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		validateInputs(engineId, projectId);

		User user = this.insight.getUser();
		validateUserSession(user);

		String userName = getUserName(user);
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		boolean isAuthor = isUserAuthor(user, engineId);

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if (isAdmin || isAuthor) {
			try {
				hasAccessToAppAndEngine(user, engineId, projectId);
				SecurityEngineUtils.updateEngineToolApp(engineId, projectId);
				noun.addAdditionalReturn(NounMetadata
						.getSuccessNounMessage("Successfully set the new tool_app for the engine " + engineId));
			} catch (Exception e) {
				throw new RuntimeException("Backend failure while setting tool_app", e);
			}
		} else {
			noun.addAdditionalReturn(NounMetadata.getErrorNounMessage(
					"Access Denied to user: " + userName + " - Only Admin or Author can update tool_app"));
		}
		return noun;
	}

	private void validateInputs(String engineId, String projectId) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Engine ID must not be null or empty");
		}
		if (projectId == null || projectId.trim().isEmpty()) {
			throw new IllegalArgumentException("Project ID must not be null or empty");
		}
	}

	private void validateUserSession(User user) {
		if (user == null) {
			throw new IllegalArgumentException("Invalid or expired user session");
		}
	}

	private String getUserName(User user) {
		return User.getLoginNames(user).values().stream().findFirst().orElse("Unknown");
	}

	private void hasAccessToAppAndEngine(User user, String engineId, String projectId) {
		if (!canAccessEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine does not exist or user does not have access to the project");
		}
		if (!canAccessApp(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}
	}

	private boolean canAccessEngine(User user, String engineId) {
		return !SecurityEngineUtils.engineIsDiscoverable(engineId)
				&& SecurityEngineUtils.userCanViewEngine(user, engineId);
	}

	private boolean canAccessApp(User user, String projectId) {
		return !SecurityProjectUtils.projectIsDiscoverable(projectId)
				&& SecurityProjectUtils.userCanViewProject(user, projectId);
	}

	private boolean isUserAuthor(User user, String engineId) {
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		Map<String, Object> permissionMap = SecurityQueryUtils.getEnginePermission(userDetails.getValue0(), engineId);
		if (permissionMap != null) {
			Object permissionValue = permissionMap.get("PERMISSION");
			return permissionValue != null && Integer.parseInt(permissionValue.toString()) == 1;
		}
		return false;
	}
}
