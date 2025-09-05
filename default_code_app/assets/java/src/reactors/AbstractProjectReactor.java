package reactors;

import domain.base.ErrorCode;
import domain.base.ProjectException;
import java.sql.SQLException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import util.ProjectProperties;

public abstract class AbstractProjectReactor extends AbstractReactor {

  private static final Logger LOGGER = LogManager.getLogger(AbstractProjectReactor.class);

  protected User user;

  protected String projectId;
  protected ProjectProperties projectProperties;

  // protected String engineId;
  // protected RDBMSNativeEngine engine; 

  protected NounMetadata result = null;

  @Override
  public NounMetadata execute() {
    try {
      preExecute();
      return doExecute();
    } catch (Exception e) {
      ProjectException ex = null;
      if (e instanceof ProjectException) {
        ex = (ProjectException) e;
      } else {
        ex = new ProjectException(ErrorCode.INTERNAL_SERVER_ERROR, e);
      }
      LOGGER.error(String.format("Reactor %s threw an error", this.getClass().getSimpleName()), e);

      // NounMetadata result =
      //     new NounMetadata(ex.getAsMap(), PixelDataType.MAP, PixelOperationType.ERROR);
      // if (projectProperties != null && projectProperties.getDebuggingEnabled()) {
      //   result.addAdditionalReturn(
      //       new NounMetadata(
      //           ExceptionUtils.getFullStackTrace(e),
      //           PixelDataType.CONST_STRING,
      //           PixelOperationType.ERROR));
      // }
      return new NounMetadata(ex.getAsMap(), PixelDataType.MAP, PixelOperationType.ERROR);
    }
  }

  protected void preExecute() {
    projectId = this.insight.getContextProjectId();
    if (projectId == null) {
      projectId = this.insight.getProjectId();
    }

    projectProperties = ProjectProperties.getInstance(projectId);

    // TODO: Load engines into protected variables that reactors can access
    // engineId = projectProperties.getEngineId();
    // engine = (RDBMSNativeEngine) Utility.getDatabase(projectProperties.getEngineId());
    
    user = this.insight.getUser();

    organizeKeys();
  }

  protected abstract NounMetadata doExecute();

}
