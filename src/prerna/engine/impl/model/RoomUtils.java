package prerna.engine.impl.model;


import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;

public class RoomUtils {
	
	 public static Room getOrLoadRoom(String roomId, String userId, Insight insight) {
	        Room room;
	        if (insight.getUser().roomHash.containsKey(roomId)) {
				try {
					room = (Room) insight.getUser().roomHash.get(roomId);
					return room;
				} catch (ClassCastException e) {
					insight.getUser().roomHash.remove(roomId);
				}

	        }
	        
	        boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckConversationExists(roomId);
	        if (!roomExistsInDB) throw new IllegalArgumentException("User room is not valid");
	        room = ModelInferenceLogsUtils.getRoomById(roomId, userId);
	        room.setInsight(insight);
	        room.parseMessages();
	        insight.getUser().roomHash.put(roomId, room);
	        return room;
	    }

}
