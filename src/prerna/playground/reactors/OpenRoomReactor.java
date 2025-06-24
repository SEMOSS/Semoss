package prerna.playground.reactors;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import prerna.playground.utils.PlaygroundUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpenRoomReactor extends AbstractPlaygroundReactor {

  @Override
  public NounMetadata doExecute() throws SQLException {

    String roomId =
        PlaygroundUtils.openRoom(
            insight.getInsightId(),
            user.getPrimaryLoginToken(),
            modelInferenceLogsDb,
            projectId,
            projectName);

    // save the roomId
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("roomId", roomId);
    return new NounMetadata(output, PixelDataType.MAP);
  }
}
