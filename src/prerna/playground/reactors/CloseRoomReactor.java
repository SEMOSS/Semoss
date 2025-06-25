package prerna.playground.reactors;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloseRoomReactor extends AbstractReactor {

  public CloseRoomReactor() {
    this.keysToGet = new String[] {"roomId"};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    String roomId = this.keyValue.get(this.keysToGet[0]);
    boolean result =
        ModelInferenceLogsUtils.doSetRoomToInactive(this.insight.getUser().getPrimaryLoginToken().getId(), roomId);
    return new NounMetadata(result, PixelDataType.BOOLEAN);
  }
}
