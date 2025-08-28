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
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetRoomWorkspaceReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(SetRoomWorkspaceReactor.class);

	public SetRoomWorkspaceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.WORKSPACE_ID.getKey()};
		this.keyRequired = new int[]{1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		if (workspaceId != null) {
			Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
			if (current == null) {
				throw new IllegalArgumentException("Workspace not found");
			}
			String currentOwner = (String) current.get("owner");

			Object currentlySharingEnabled = current.get("sharing_enabled");
			Boolean currentlyShared = (Boolean) currentlySharingEnabled;

			Object currentlyIsActive = current.get("is_active");
			Boolean currentlyActive = (Boolean) currentlyIsActive;

			boolean hasPermission = false;
			if (currentOwner != null) {
				for (AuthProvider provider : user.getLogins()) {
					if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
						hasPermission = true;
						break;
					}
				}
			}
			if (Boolean.TRUE != currentlyActive || !hasPermission && (Boolean.TRUE != currentlyShared
					|| !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user))) {
				throw new IllegalArgumentException("User unauthorized to perform this operation");
			}
		}

		ModelInferenceLogsUtils.setRoomWorkspaceId(roomId, user.getPrimaryLoginToken().getId(), workspaceId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
