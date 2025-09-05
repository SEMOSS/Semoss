package util;

import domain.base.ErrorCode;
import domain.base.ProjectException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Loads property values from a project asset at
 *
 * <pre>[proj]/app_root/version/assets/java/project.properties</pre>
 *
 * into a static instance. The project used to infer the path is an argument at runtime.
 *
 * <p>File format conforms to typical java.util.Properties style load patterns: one property per
 * line with white space, {@code '='}, or {@code ':'} separating the key and value.
 *
 * <p>Replace the engineId applicable for your project in a format such as:
 *
 * <pre>engineId=fc6a3fab-2425-4987-be93-58ad2efeee24</pre>
 *
 * @see java.util.Properties#load(java.io.Reader)
 */
public class ProjectProperties {
  private static ProjectProperties INSTANCE = null;

  // private String engineId;
  // private boolean debuggingEnabled;

  private ProjectProperties() {}

  public static ProjectProperties getInstance() {
    if (INSTANCE == null) {
      throw new ProjectException(
          ErrorCode.INTERNAL_SERVER_ERROR, "Unable to load project configuration");
    }
    return INSTANCE;
  }

  public static ProjectProperties getInstance(String projectId) {
    if (INSTANCE == null) {
      loadProp(projectId);
    }
    return INSTANCE;
  }

  private static void loadProp(String projectId) {
    ProjectProperties newInstance = new ProjectProperties();

    try (final FileInputStream fileIn =
        new FileInputStream(
            Utility.normalizePath(
                AssetUtility.getProjectAssetsFolder(projectId) + "/java/project.properties"))) {
      Properties projectProperties = new Properties();
      projectProperties.load(fileIn);

      // TODO Add any properties to be read by the properties file and add the corresponding getter

      // newInstance.engineId = projectProperties.getProperty("engineId");
      // newInstance.debuggingEnabled =
      //     Boolean.parseBoolean(projectProperties.getProperty("debuggingEnabled"));

      INSTANCE = newInstance;
    } catch (IOException e) {
      INSTANCE = null;
      throw new ProjectException(
          ErrorCode.INTERNAL_SERVER_ERROR, "Unable to load project configuration", e);
    }
  }

  // public String getEngineId() {
  //   return engineId;
  // }

  // public boolean getDebuggingEnabled() {
  //   return debuggingEnabled;
  // }
}
