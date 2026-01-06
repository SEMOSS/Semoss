package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetRoomWorkspaceReactor extends AbstractReactor {

	@SuppressWarnings("unused")
	private static final Logger classLogger = LogManager.getLogger(SetRoomWorkspaceReactor.class);

	public SetRoomWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());

		Room room = ModelInferenceLogsUtils.getRoomById(roomId, user.getPrimaryLoginToken().getId());
		if (room == null) {
			throw new IllegalArgumentException("Room not found");
		}

		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		if (workspaceId != null) {
			Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
			if (current == null) {
				throw new IllegalArgumentException("Workspace not found");
			}

			Object currentlyIsActive = current.get("is_active");
			Boolean currentlyActive = (Boolean) currentlyIsActive;

			if (!currentlyActive) {
				throw new IllegalArgumentException("Workspace is disabled by the owner");
			}
			if (!SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
				throw new IllegalArgumentException(
						"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
			}
		}

		ModelInferenceLogsUtils.setRoomWorkspaceId(roomId, user.getPrimaryLoginToken().getId(), workspaceId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}