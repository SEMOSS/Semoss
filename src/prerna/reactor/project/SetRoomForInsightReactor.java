package prerna.reactor.project;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetRoomForInsightReactor extends AbstractReactor {

    public SetRoomForInsightReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.ROOM_ID.getKey()};
        this.keyRequired = new int[]{1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        String userId = user.getPrimaryLoginToken().getId();

        String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
        if (roomId == null || roomId.isEmpty()) {
            throw new IllegalArgumentException("Room ID is required");
        }

        boolean isOwner = !ModelInferenceLogsUtils.getUserActiveRooms(roomId, userId).isEmpty();
        if(!isOwner) {
            throw new IllegalArgumentException("User is not an owner of an active room");
        }
        
        List<Map<String, Object>> activeRooms = ModelInferenceLogsUtils.getUserActiveRooms(roomId, userId);
        if (activeRooms == null || activeRooms.isEmpty()) {
            throw new IllegalArgumentException("User is not the owner of the room or room is not active");
        }

        Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
        if (room == null) {
            throw new IllegalArgumentException("Room not found");
        }
        String roomFolder = room.getRoomFolderPath();

        //Push files in the room if any
        boolean hasFiles = RoomUtils.hasFiles(room); 
        if (hasFiles) {
            ClusterUtil.pushRoom(room.getId());
        }

        //Set the insight folder to this room
        this.insight.setInsightFolder(roomFolder);

        return new NounMetadata(true, PixelDataType.BOOLEAN);
    }
}