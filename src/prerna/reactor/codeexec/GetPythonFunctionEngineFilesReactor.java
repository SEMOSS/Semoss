package prerna.reactor.codeexec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GetPythonFunctionEngineFilesReactor extends AbstractReactor {
    
    private static final Logger classLogger = LogManager.getLogger(GetPythonFunctionEngineFilesReactor.class);


    public GetPythonFunctionEngineFilesReactor() {
        this.keysToGet = new String[]{
        ReactorKeysEnum.ENGINE.getKey(),
        };
    }

    @Override
    public NounMetadata execute() {
        User user = this.insight.getUser();
        if (user == null) {
            NounMetadata noun = new NounMetadata(
                    "User must be signed into an account to retrieve the function engine files", 
                    PixelDataType.CONST_STRING,
                    PixelOperationType.ERROR, 
                    PixelOperationType.LOGGIN_REQUIRED_ERROR);
            SemossPixelException err = new SemossPixelException(noun);
            err.setContinueThreadOfExecution(false);
            throw err;
        }

        if (AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
            throwAnonymousUserError();
        }

        // validate user's access rights to retrieve function engine files
        if (SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
            throwUserNotPublisherError();
        }

        if (AbstractSecurityUtils.adminOnlyFunctionAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
            throwFunctionalityOnlyExposedForAdminsError();
        }

        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Function Engine " + engineId + " does not exist or user does not have access to this function");
		}
		String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);

		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		
        if (engineName == null) {
            throw new IllegalArgumentException("Function Engine Name must be provided");
        }

        // Directory Path Generation
        String funtionEnginePath = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.FUNCTION, engineId, engineName);

        Map<String, Object> folderStructure = traverseDirectory(funtionEnginePath);
        
		return new NounMetadata(folderStructure, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
    }

    public static Map<String, Object> traverseDirectory(String path) {
        Map<String, Object> result = new HashMap<>();
        File root = new File(path);
        if (!root.exists() || !root.isDirectory()) {
        	classLogger.warn("Invalid directory path.");
            return result;
        }
        result.put(root.getName(), traverse(root));
        return result;
    }

    // Recursive method that returns structure of current folder
    private static Map<String, Object> traverse(File directory) {
        Map<String, Object> currentFolder = new HashMap<>();
        List<Map<String, String>> fileList = new ArrayList<>();

        File[] files = directory.listFiles();
        if (files == null) {
            return currentFolder;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                currentFolder.put(file.getName(), traverse(file)); // recurse
            } else if (file.getName().endsWith(".py")) {
                Map<String, String> fileData = new HashMap<>();
                fileData.put("fileName", file.getName());
                fileData.put("content", readFileContent(file));
                fileList.add(fileData);
            }
        }

        // Always put files list even if empty, so we know folder exists
        currentFolder.put("files", fileList);
        return currentFolder;
    }

    // Method to read the content of the python file
    private static String readFileContent(File file) {
        StringBuilder content = new StringBuilder();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return content.toString();
    }

}

       

