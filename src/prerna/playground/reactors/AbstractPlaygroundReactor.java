package prerna.playground.reactors;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.playground.utils.CustomMapper;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractPlaygroundReactor extends AbstractReactor {

  private static final Logger LOGGER = LogManager.getLogger(AbstractPlaygroundReactor.class);

  protected User user;
  protected RDBMSNativeEngine modelInferenceLogsDb;
  protected NounMetadata result = null;
  protected String projectId;
  protected String projectName;

  @Override
  public NounMetadata execute() {
    try {
      preExecute();
      modelInferenceLogsDb =
          (RDBMSNativeEngine) Utility.getDatabase(Constants.MODEL_INFERENCE_LOGS_DB);
      if (modelInferenceLogsDb == null) {
        throw new IllegalArgumentException("Unable to find database");
      }
      try {
        result = doExecute();
      } finally {
        ConnectionUtils.closeAllConnectionsIfPooling(modelInferenceLogsDb, null, null);
      }
      return result;
    } catch (Exception e) {
      LOGGER.error(String.format("Reactor %s threw an error", this.getClass().getSimpleName()), e);
      return new NounMetadata(e, PixelDataType.MAP, PixelOperationType.ERROR);
    }
  }

  protected void preExecute() {

    user = this.insight.getUser();
    if (user == null) {
      throw new NullPointerException("User is null");
    }
    projectId = insight.getContextProjectId();
    if (projectId == null) {
      projectId = insight.getProjectId();
    }
    projectName = null;
    if (projectId != null) {
      IProject project = Utility.getProject(projectId);
      projectName = project.getProjectName();
    }
    organizeKeys();
  }

  protected static <T> T getPayloadObject(
      NounStore nounStore, String[] keysToGet, Class<T> targetClass) {
    Map<String, Object> payloadMap = new HashMap<>();
    for (String key : keysToGet) {
      if ("no keys defined".equals(key)) {
        break;
      }
      GenRowStruct grs = nounStore.getNoun(key);
      if (grs == null || grs.isEmpty()) {
        payloadMap.put(key, null);
      } else {
        payloadMap.put(key, grs.getAllValues());
      }
    }
    T payload;
    try {
      payload = CustomMapper.PAYLOAD_MAPPER.convertValue(payloadMap, targetClass);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid request: inputs could not be parsed");
    }
    return payload;
  }

  protected abstract NounMetadata doExecute() throws SQLException;
}
