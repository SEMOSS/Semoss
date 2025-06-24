package prerna.playground.reactors;

import java.util.HashMap;
import java.util.Map;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenRoomReactor extends AbstractReactor {

  @Override
  public NounMetadata execute() {

    String roomId = ModelInferenceLogsUtils.openRoom(this.insight);

    // save the roomId
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("roomId", roomId);
    return new NounMetadata(output, PixelDataType.MAP);
  }
}
