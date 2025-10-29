package prerna.reactor.livekit;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.livekit.LiveKitController;
import io.livekit.server.AccessToken;
import java.util.HashMap;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;

public class LiveKitJoinRoomReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(LiveKitJoinRoomReactor.class);

	
	public LiveKitJoinRoomReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				"operation",
				"roomId"
		};
		this.keyRequired = new int[] { 1, 1, 0 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String roomId = this.keyValue.get("roomId");
		if (roomId == null || roomId.isEmpty()) {
			roomId = UUID.randomUUID().toString();
		}
		
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String operation = this.keyValue.get("operation");
		
		
		User user = this.insight.getUser();
		String userId = User.getSingleLogginName(user);
		
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		LiveKitController controller = new LiveKitController();
		try {
		AccessToken token = controller.joinRoom(userId, userId, roomId, engineId, operation, this.insight);
		
		String jwt = token.toJwt();
		
		HashMap<String, String> result = new HashMap<>();
		
		result.put("jwt", jwt);
		result.put("room_id", roomId);
		result.put("insightId", this.insight.getInsightId());
		
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		
		} catch (Exception e) {
			String errorMsg = "Failed to list LiveKit rooms: " + e.getMessage();
			classLogger.error(errorMsg, e);
			
			return getError(errorMsg);
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Create or join a LiveKit Room.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("roomId")) {
			return "This is the room ID of an existing room. To join a new room simply do not pass this param or pass as an empty string.";
		}

		return super.getDescriptionForKey(key);
	}
	
	
}
