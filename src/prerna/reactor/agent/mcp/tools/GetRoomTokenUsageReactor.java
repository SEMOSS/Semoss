package prerna.reactor.agent.mcp.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns the total token count for the current room message history. */
public class GetRoomTokenUsageReactor extends AbstractReactor {

  public GetRoomTokenUsageReactor() {
    this.keysToGet = new String[] {"roomId"};
    this.keyRequired = new int[] {0};
  }

  @Override
  public NounMetadata execute() {
	  organizeKeys();
    String roomId = this.keyValue.get("roomId");
    if (roomId == null || roomId.trim().isEmpty()) {
      roomId = insight.getRoomId();
    }
    if (roomId == null || roomId.trim().isEmpty()) {
      throw new IllegalArgumentException("roomId is required to calculate token usage");
    }

    Room room = RoomUtils.getOrLoadRoom(roomId, this.insight);
    List<AbstractMessage> messages = room.getMessages();
    int totalTokens = 0;
    for (AbstractMessage message : messages) {
      totalTokens += message.getTokensInMessage();
    }

    Map<String, Object> result = new HashMap<>();
    result.put("roomId", roomId);
    result.put("messageCount", messages.size());
    result.put("totalTokens", totalTokens);
    return new NounMetadata(result, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Returns the total token count for the current room message history.";
  }
}