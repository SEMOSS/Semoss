package prerna.engine.impl.model.workspace;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.OpenSearchRestVectorDatabaseEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class AddWorkspaceDocumentReactor extends AbstractReactor {

  private static final Logger LOGGER = LogManager.getLogger(AddWorkspaceDocumentReactor.class);

  public static final String WORKSPACE_ID = "workspaceId";
  public static final String FILE_PATHS = "filePaths";
  public static final String VECTOR_DB_DETAILS = "vectorDbDetails";

  public static final String PATH_TO_UNZIP_FILES = "zipFileExtractFolder";

  private IVectorDatabaseEngine db = null;
  private String dbId = null;
  private String dbName = null;
  private Map<String, Object> dbDetails = null;
  private VectorDatabaseTypeEnum dbType = null;

  public AddWorkspaceDocumentReactor() {
    this.keysToGet =
        new String[] {
          WORKSPACE_ID,
          FILE_PATHS,
          ReactorKeysEnum.SPACE.getKey(),
          ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
          VECTOR_DB_DETAILS
        };
    this.keyRequired = new int[] {1, 1, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();

    String workspaceId = this.keyValue.get(WORKSPACE_ID);

    Map<String, Object> paramMap = getMap();
    if (paramMap == null) {
      paramMap = new HashMap<String, Object>();
    }

    // validate workspace and permission
    Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
    if (current == null) {
      throw new IllegalArgumentException("Workspace not found");
    }
    String currentOwner = (String) current.get("owner");
    Boolean currentlyShared = (Boolean) current.get("sharing_enabled");
    if (currentlyShared == null) currentlyShared = Boolean.FALSE;
    boolean hasOwnerPermission = false;
    if (currentOwner != null) {
      for (AuthProvider provider : user.getLogins()) {
        if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
          hasOwnerPermission = true;
          break;
        }
      }
    }
    int permissionLevel =
        Math.min(
            hasOwnerPermission ? AccessPermissionEnum.OWNER.getId() : Integer.MAX_VALUE,
            currentlyShared
                ? ModelInferenceLogsUtils.getWorkspaceSharePermission(
                    workspaceId,
                    user,
                    AccessPermissionEnum.OWNER.getId(),
                    AccessPermissionEnum.EDIT.getId())
                : Integer.MAX_VALUE);
    if (permissionLevel > AccessPermissionEnum.EDIT.getId()) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    // ensure we have a vector db setup
    db = Utility.getVectorDatabase(workspaceId);
    if (db == null) {
      dbId = workspaceId;
      dbName = ModelInferenceLogsUtils.WORKSPACE_DATABASE_TAG + "_" + dbId;
      dbDetails = getVectorDatabaseDetails();
      dbType = getVectorDatabaseType(dbDetails);

      String workspaceKnowledgeId = UUID.randomUUID().toString();
      try {
        db = ModelInferenceLogsUtils.createWorkspaceVectorDb(user, dbId, dbName, dbDetails, dbType);
        ModelInferenceLogsUtils.createNewWorkspaceKnowledge(
            workspaceKnowledgeId,
            workspaceId,
            dbId,
            db.getCatalogType().toString() + "__" + dbType.toString());
      } catch (Exception e) {
        LOGGER.error(Constants.STACKTRACE, e);
        ModelInferenceLogsUtils.deleteWorkspaceKnowledge(workspaceKnowledgeId);
        return getError("Failed to create database for added document: " + e.getMessage());
      }
    }

    paramMap.put(AbstractVectorDatabaseEngine.INSIGHT, this.insight);
    String rootFolder = getRootFolder();
    List<String> validFiles = new ArrayList<>();
    List<String> invalidFiles = new ArrayList<>();
    try {
      getFiles(rootFolder, validFiles, invalidFiles);
      if (validFiles.isEmpty()) {
        throw new IllegalArgumentException(
            "Please provide valid input files using \"filePaths\". File types supported are pdf, word, ppt, or txt files");
      }
      for (String filePath : validFiles) {
        File file = new File(Utility.normalizePath(filePath));
        if (!file.exists()) {
          throw new IllegalArgumentException(
              "File path for "
                  + file.getName()
                  + " does not exist within the insight or project space.");
        }
      }

      db.addDocument(validFiles, paramMap);
    } catch (Exception e) {
      LOGGER.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException("The following exception occured: " + e.getMessage());
    } finally {
      File zipFileExtractionDir = new File(rootFolder + "/" + PATH_TO_UNZIP_FILES);
      if (zipFileExtractionDir.exists()) {
        try {
          FileUtils.forceDelete(zipFileExtractionDir);
        } catch (IOException e) {
          LOGGER.error(Constants.STACKTRACE, e);
        }
      }
    }

    NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
    if (!invalidFiles.isEmpty()) {
      List<String> invalidFileNamesRelative = new ArrayList<>(invalidFiles.size());
      for (String invalidF : invalidFiles) {
        invalidFileNamesRelative.add(invalidF.replace(rootFolder, ""));
      }
      noun.addAdditionalReturn(
          NounMetadata.getWarningNounMessage(
              "Unable to upload " + String.join(", ", invalidFileNamesRelative)));
    }
    return noun;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getVectorDatabaseDetails() {
    // use reactor input preferentially, fallback to rdf_map, fallback to tagged engine

    Map<String, Object> vectorDbDetails = null;

    GenRowStruct grs = this.store.getNoun(VECTOR_DB_DETAILS);
    if (grs != null && !grs.isEmpty()) {
      List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
      if (mapNouns != null && !mapNouns.isEmpty()) {
        vectorDbDetails = (Map<String, Object>) mapNouns.get(0).getValue();
      }
    } else {
      List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
      if (mapNouns != null && !mapNouns.isEmpty()) {
        vectorDbDetails = (Map<String, Object>) mapNouns.get(0).getValue();
      }
    }

    // if not provided, try to load via rdf_map
    if (vectorDbDetails == null) {
      Properties coreProp = DIHelper.getInstance().getCoreProp();
      if (coreProp != null && !coreProp.isEmpty()) {
        Set<String> workspaceProps =
            coreProp.stringPropertyNames().stream()
                .filter(str -> str.startsWith("workspace_default_"))
                .collect(Collectors.toSet());
        if (!workspaceProps.isEmpty()) {
          vectorDbDetails = new HashMap<>();
          for (String workspaceProp : workspaceProps) {
            String workspacePropValue = StringUtils.trimToNull(coreProp.getProperty(workspaceProp));
            if (workspacePropValue != null) {
              vectorDbDetails.put(workspaceProp, workspacePropValue);
            }
          }
        }
      }
    }

    // support the legacy encoder_id since the create vector db reactor does
    String embedderEngineId = null;
    if (vectorDbDetails != null) {
      embedderEngineId =
          StringUtils.trimToNull((String) vectorDbDetails.get(Constants.EMBEDDER_ENGINE_ID));
      if (embedderEngineId == null) {
        embedderEngineId = StringUtils.trimToNull((String) vectorDbDetails.get("ENCODER_ID"));
      }
    }

    // not found? try to find via a tag of "workspace_default_embedder"
    if (embedderEngineId == null) {
      RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);
      SelectQueryStruct qs = new SelectQueryStruct();
      qs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID", "engine_id"));
      qs.addExplicitFilter(
          SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", "tag"));
      qs.addExplicitFilter(
          SimpleQueryFilter.makeColToValFilter(
              "ENGINEMETA__METAVALUE", "==", "workspace_default_embedder"));
      qs.setLimit(1L);
      List<Map<String, Object>> embeddingModels =
          QueryExecutionUtility.flushRsToMap(securityDb, qs);
      if (!embeddingModels.isEmpty()) {
        embedderEngineId = (String) embeddingModels.get(0).get("engine_id");
      }
    }
    if (embedderEngineId == null) {
      throw new IllegalArgumentException("EMBEDDER_ENGINE_ID undefined for workspace");
    }

    IModelEngine embeddingModel = Utility.getModel(embedderEngineId);
    if (embeddingModel == null) {
      throw new IllegalArgumentException(
          "EMBEDDER_ENGINE_ID " + embeddingModel + " could not be found");
    }

    if (vectorDbDetails == null) {
      vectorDbDetails = new HashMap<>();
      vectorDbDetails.put(Constants.EMBEDDER_ENGINE_ID, embedderEngineId);
    }

    String embeddingModelAlias = embeddingModel.getSmssProp().getProperty(Constants.ENGINE_ALIAS);
    vectorDbDetails.put(Constants.EMBEDDER_ENGINE_NAME, embeddingModelAlias);

    if (!vectorDbDetails.containsKey(Constants.INDEX_CLASSES)) {
      vectorDbDetails.put(Constants.INDEX_CLASSES, "default");
    }

    return vectorDbDetails;
  }

  private VectorDatabaseTypeEnum getVectorDatabaseType(Map<String, Object> vectorDbDetails) {
    VectorDatabaseTypeEnum vectorDbType = null;

    String vectorDbTypeStr = (String) vectorDbDetails.get(IVectorDatabaseEngine.VECTOR_TYPE);
    if (vectorDbTypeStr == null || (vectorDbTypeStr = vectorDbTypeStr.trim()).isEmpty()) {
      vectorDbType = VectorDatabaseTypeEnum.FAISS;
    } else {
      try {
        vectorDbType = VectorDatabaseTypeEnum.getEnumFromName(vectorDbTypeStr);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid model type " + vectorDbTypeStr);
      }
    }

    if (vectorDbType == VectorDatabaseTypeEnum.OPEN_SEARCH) {
      if (vectorDbDetails.get(Constants.USERNAME) == null) {
        throw new IllegalArgumentException(Constants.USERNAME + " is not provided.");
      }
      if (vectorDbDetails.get(Constants.PASSWORD) == null) {
        throw new IllegalArgumentException(Constants.PASSWORD + " is not provided.");
      }
      if (vectorDbDetails.get(Constants.HOSTNAME) == null) {
        throw new IllegalArgumentException(Constants.HOSTNAME + " is not provided.");
      }
      if (vectorDbDetails.get(OpenSearchRestVectorDatabaseEngine.INDEX_NAME) == null) {
        throw new IllegalArgumentException(
            OpenSearchRestVectorDatabaseEngine.INDEX_NAME + " is not provided.");
      }
    }
    if (vectorDbType == VectorDatabaseTypeEnum.WEAVIATE) {
      if (vectorDbDetails.get(Constants.API_KEY) == null) {
        throw new IllegalArgumentException(Constants.API_KEY + " is not provided.");
      }
      if (vectorDbDetails.get(Constants.HOSTNAME) == null) {
        throw new IllegalArgumentException(Constants.HOSTNAME + " is not provided.");
      }
    }
    return vectorDbType;
  }

  private String getRootFolder() {
    String space = null;
    GenRowStruct spaceGrs = store.getNoun(ReactorKeysEnum.SPACE.getKey());
    if (spaceGrs != null && !spaceGrs.isEmpty()) {
      space = spaceGrs.get(0).toString();
    }

    return AssetUtility.getRootFolderPath(this.insight, space, false);
  }

  private void getFiles(String rootFolder, List<String> validFiles, List<String> invalidFiles)
      throws IOException {
    GenRowStruct grs = this.store.getNoun(FILE_PATHS);
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) {
        String filePath = rootFolder + "/" + grs.get(i).toString();
        if (isZipFile(filePath)) {
          String zipFileLocation = filePath.replace('\\', '/');
          File zipFileExtractFolder = new File(rootFolder, PATH_TO_UNZIP_FILES);
          unzipAndFilter(
              zipFileLocation, zipFileExtractFolder.getAbsolutePath(), validFiles, invalidFiles);
        } else {
          // String filePath = destDirectory + File.separator + entry.getName();
          if (isSupportedFileType(filePath)) {
            validFiles.add(filePath);
          } else {
            invalidFiles.add(filePath);
          }
        }
      }
    }
  }

  private Map<String, Object> getMap() {
    GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
    if (mapGrs != null && !mapGrs.isEmpty()) {
      List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
      if (mapInputs != null && !mapInputs.isEmpty()) {
        return (Map<String, Object>) mapInputs.get(0).getValue();
      }
    }
    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      return (Map<String, Object>) mapInputs.get(0).getValue();
    }
    return null;
  }

  private void unzipAndFilter(
      String zipFilePath, String destDirectory, List<String> validFiles, List<String> invalidFiles)
      throws IOException {
    File destDir = new File(Utility.normalizePath(destDirectory));
    if (!destDir.exists()) {
      destDir.mkdir();
    }

    try (ZipInputStream zipIn =
        new ZipInputStream(new FileInputStream(Utility.normalizePath(zipFilePath)))) {
      ZipEntry entry = zipIn.getNextEntry();

      while (entry != null) {
        String filePath = destDirectory + "/" + entry.getName();
        if (!entry.isDirectory()) {
          if (isSupportedFileType(filePath)) {
            extractFile(zipIn, filePath);
            validFiles.add(filePath);
          } else {
            invalidFiles.add(filePath);
          }
        } else if (entry.isDirectory()) {
          File dir = new File(Utility.normalizePath(filePath));
          dir.mkdirs();
        } else if (isZipFile(filePath)) {
          // Handle nested zip file
          this.extractFile(zipIn, filePath);

          // Check if the entry is not in the root directory
          String parentPath = null;
          if (filePath.contains("/")) { // ZIP entries use "/" as a separator
            parentPath = filePath.substring(0, filePath.lastIndexOf('/'));
          }

          // Extract the last part of the path (file name + extension)
          String fileNameWithExtension =
              filePath.contains("/") ? filePath.substring(filePath.lastIndexOf('/') + 1) : filePath;

          // Remove the extension
          String baseName =
              fileNameWithExtension.contains(".")
                  ? fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'))
                  : fileNameWithExtension;

          unzipAndFilter(filePath, parentPath + "/" + baseName, validFiles, invalidFiles);
        }

        zipIn.closeEntry();
        entry = zipIn.getNextEntry();
      }
    }
  }

  private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(Utility.normalizePath(filePath))) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = zipIn.read(buffer)) != -1) {
        fos.write(buffer, 0, bytesRead);
      }
    }
  }

  private boolean isSupportedFileType(String filePath) {
    // Find the last index of '.'
    int dotIndex = filePath.lastIndexOf('.');

    if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
      // Extract the extension and convert it to lower case
      String extension = filePath.substring(dotIndex + 1).toLowerCase();

      return extension.equals("pdf")
          || extension.equals("pptx")
          || extension.equals("ppt")
          || extension.equals("doc")
          || extension.equals("docx")
          || extension.equals("txt")
          || extension.equals("csv");
    } else {
      // do a mime type check
      Tika tika = new Tika();
      File file = new File(Utility.normalizePath(filePath));
      try (FileInputStream inputstream = new FileInputStream(file)) {
        String mimeType = tika.detect(inputstream, new Metadata());

        switch (mimeType) {
          case "application/pdf":
          case "application/vnd.openxmlformats-officedocument.wordprocessingml.document": // .docx
          case "application/vnd.ms-powerpoint": // .ppt
          case "application/vnd.openxmlformats-officedocument.presentationml.presentation": // .pptx
          case "text/plain":
            return true;
          default:
            return false;
        }
      } catch (IOException e) {
        LOGGER.error(Constants.ERROR_MESSAGE, e);
        return false;
      }
    }
  }

  private boolean isZipFile(String filePath) {
    // Find the last index of '.'
    int dotIndex = filePath.lastIndexOf('.');

    if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
      // Extract the extension and convert it to lower case
      String extension = filePath.substring(dotIndex + 1).toLowerCase();

      return extension.equals("zip");
    } else {
      // do a mime type check
      Tika tika = new Tika();
      File file = new File(Utility.normalizePath(filePath));
      try (FileInputStream inputstream = new FileInputStream(file)) {
        String mimeType = tika.detect(inputstream, new Metadata());

        if (mimeType != null) {
          if (mimeType.equalsIgnoreCase("application/zip")) {
            return true;
          }
        }

        return false;
      } catch (IOException e) {
        LOGGER.error(Constants.ERROR_MESSAGE, e);
        return false;
      }
    }
  }
}
