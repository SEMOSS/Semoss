package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.List;
import java.util.Map;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetProjectUserRoomsReactor extends AbstractReactor {

  public GetProjectUserRoomsReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    if (user == null) {
      throw new IllegalArgumentException("You are not properly logged in");
    }

    String projectId = insight.getContextProjectId();
    if (projectId == null) {
      projectId = insight.getProjectId();
    }
    List<Map<String, Object>> userRooms =
        ModelInferenceLogsUtils.getUserRoomsMetadataPerProject(
            projectId, user.getPrimaryLoginToken().getId());
    return new NounMetadata(userRooms, PixelDataType.VECTOR);
  }
}
