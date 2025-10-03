package prerna.reactor.livekit;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.livekit.LiveKitController;
import io.livekit.server.AccessToken;
import java.util.HashMap;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import prerna.auth.User;

public class LiveKitJoinRoomReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(LiveKitJoinRoomReactor.class);

	
	public LiveKitJoinRoomReactor() {
		this.keysToGet = new String[] {
				"roomId"
		};
		this.keyRequired = new int[] { 0 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String roomId = this.keyValue.get("roomId");
		if (roomId == null || roomId.isEmpty()) {
			roomId = UUID.randomUUID().toString();
		}
		
		User user = this.insight.getUser();
		String userId = User.getSingleLogginName(user);
		
		LiveKitController controller = LiveKitController.getInstance();
		try {
		AccessToken token = controller.joinRoom(userId, userId, roomId);
		
		String jwt = token.toJwt();
		
		HashMap<String, String> result = new HashMap<>();
		
		result.put("jwt", jwt);
		result.put("room_id", roomId);
		
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		
		} catch (IOException e) {
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
