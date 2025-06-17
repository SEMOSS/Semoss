package prerna.reactor.codeexec;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetPythonFunctionEngineFilesReactor extends AbstractEngineFileReactor {
    
    private static final Logger classLogger = LogManager.getLogger(GetPythonFunctionEngineFilesReactor.class);


    public GetPythonFunctionEngineFilesReactor() {
        this.keysToGet = new String[]{
        ReactorKeysEnum.ENGINE.getKey(),
        ReactorKeysEnum.FILE_PATH.getKey(),
        ReactorKeysEnum.EXTENTION.getKey()
        
        };
    }

    @Override
    public NounMetadata execute() {
    	organizeKeys();
    	User user = this.insight.getUser();
        String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        String extentionFilter = this.keyValue.get(ReactorKeysEnum.EXTENTION.getKey());
        validateUserAndEngineAccess(user);
        
        if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
            throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to this engine");
        }

        String enginePath = getEngineBasePath(engineId);
        Map<String, Object> responseData = null;
		try {
			responseData = getEngineFiles(enginePath, extentionFilter);
		} catch (IOException e) {
			classLogger.error("Error processing files", e);
            throw new RuntimeException("File processing failed: " + e.getMessage(), e);
		}
        
		return new NounMetadata(responseData, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
    }
    
    private Map<String, Object> getEngineFiles(String enginePath, String extentionFilter) throws IOException {
      String engineSubPath = Utility.normalizePath(this.keyValue.getOrDefault(ReactorKeysEnum.FILE_PATH.getKey(), ""));
      if (!engineSubPath.startsWith("/") && !engineSubPath.startsWith("\\")) {
          engineSubPath = "/" + engineSubPath;
      }

      String engineFilePath = enginePath + engineSubPath;
      File target = new File(engineFilePath);

      if (!target.exists()) return new HashMap<>();

      if (target.isFile()) {
          Map<String, String> fileData = new HashMap<>();
          fileData.put("fileName", target.getName());
          fileData.put("content", readFileContent(target));

          Map<String, Object> singleFileMap = new HashMap<>();
          singleFileMap.put("files", Arrays.asList(fileData));

          Map<String, Object> result = new HashMap<>();
          result.put(target.getParentFile().getName(), singleFileMap);
          return result;
      }

      return traverseDirectory(engineFilePath, extentionFilter);
  }

}

       

