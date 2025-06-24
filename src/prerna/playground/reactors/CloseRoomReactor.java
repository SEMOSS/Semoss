package prerna.playground.reactors;

import java.sql.SQLException;
import prerna.playground.utils.PlaygroundUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloseRoomReactor extends AbstractPlaygroundReactor {

  public CloseRoomReactor() {
    this.keysToGet = new String[] {"roomId"};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata doExecute() throws SQLException {
    String roomId = this.keyValue.get(this.keysToGet[0]);
    boolean result =
        PlaygroundUtils.deactivateRoom(
            roomId, user.getPrimaryLoginToken().getId(), modelInferenceLogsDb);
    return new NounMetadata(result, PixelDataType.BOOLEAN);
  }
}
