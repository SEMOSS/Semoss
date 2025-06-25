package prerna.playground.reactors;

import java.util.HashMap;
import java.util.Map;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenRoomReactor extends AbstractReactor {

  

  @Override
  public NounMetadata execute() {

    Room room = RoomUtils.createRoomIfNotExists(null, this.insight, null, null);

    // save the roomId
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("roomId", room.getId());
    return new NounMetadata(output, PixelDataType.MAP);
  }
}
