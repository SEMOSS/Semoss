package prerna.reactor.codeexec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.text.StringEscapeUtils;
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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class UpdatePythonFunctionEngineReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(UpdatePythonFunctionEngineReactor.class);

    public UpdatePythonFunctionEngineReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.ENGINE.getKey(),
            ReactorKeysEnum.PAYLOAD.getKey()
        };
    }

    @Override
    public NounMetadata execute() {
        User user = this.insight.getUser();
        if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
            throwAnonymousUserError();
        }

        if (SecurityQueryUtils.userIsPublisher(user)) {
            throwUserNotPublisherError();
        }

        if (AbstractSecurityUtils.adminOnlyFunctionAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
            throwFunctionalityOnlyExposedForAdminsError();
        }

        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        
        Map<String, Object> payload = getPayload();
        if (payload == null || payload.isEmpty()) {
            throw new IllegalArgumentException("Payload must be provided with the Python files and structure");
        }

        if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
            throw new IllegalArgumentException("User does not have permission to update this engine");
        }

        String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";
        Properties prop = Utility.loadProperties(smssFile);
        String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

        if (engineName == null) {
            throw new IllegalArgumentException("Function Engine Name must be provided");
        }

        String engineBasePath = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.FUNCTION, engineId, engineName);
        File engineBaseDir = new File(engineBasePath);

        Set<String> currentPath = new HashSet<>();
       try {
		writeFilesRecursively(engineBaseDir.toPath(), payload, currentPath );
		deleteRemovedFiles(engineBaseDir, currentPath);
	} catch (IOException e) {
		e.printStackTrace();
	}
       
       return new NounMetadata("Python Function Engine files successfully updated.", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
    }
    
    @SuppressWarnings("unchecked")
    public void writeFilesRecursively(Path baseDir, Map<String, Object> payload, Set<String> currentPath) throws IOException {
        for (Map.Entry<String, Object> folderEntry : payload.entrySet()) {
            String folderName = folderEntry.getKey();
            Object folderDataObj = folderEntry.getValue();

            if (!(folderDataObj instanceof Map)) continue;

            Map<String, Object> folderData = (Map<String, Object>) folderDataObj;
            currentPath.add(baseDir.toString());
            processFolder(baseDir, folderData, currentPath);
        }
    }

    @SuppressWarnings("unchecked")
    private void processFolder(Path currentDir, Map<String, Object> folderData, Set<String> touchedPaths) throws IOException {
        // Write files in the current folder
        Object filesObj = folderData.get("files");
        if (filesObj instanceof List<?>) {
            List<Map<String, String>> files = (List<Map<String, String>>) filesObj;

            for (Map<String, String> file : files) {
                String fileName = file.get("fileName");
                String contentEscaped = file.get("content");
                String content = StringEscapeUtils.unescapeJava(contentEscaped);

                Path filePath = currentDir.resolve(fileName);
                Files.createDirectories(filePath.getParent());

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath.toFile()))) {
                    writer.write(content);
                }

                touchedPaths.add(filePath.toString());
            }
        }

        // Recurse into subfolders (any other key that maps to a Map<String, Object>)
        for (Map.Entry<String, Object> entry : folderData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.equals("files") || !(value instanceof Map)) {
                continue;
            }

            Path subfolderPath = currentDir.resolve(key);
            Files.createDirectories(subfolderPath);

            processFolder(subfolderPath, (Map<String, Object>) value, touchedPaths);
        }
    }
    
    private void deleteRemovedFiles(File baseDir, Set<String> validPaths) throws IOException {
        List<File> allFiles = getAllFilesAndFolders(baseDir);

        // Sort in reverse order (children before parents)
        allFiles.sort((a, b) -> b.getAbsolutePath().length() - a.getAbsolutePath().length());

        for (File file : allFiles) {
            String path = file.getAbsolutePath();

            if (!validPaths.contains(path)) {
                try {
                    if (file.isDirectory()) {
                        File[] contents = file.listFiles();
                        if (contents == null || contents.length == 0) {
                            Files.delete(file.toPath());
                            classLogger.info("Deleted empty folder: " + path);
                        } else {
                            boolean allChildrenMarkedForDeletion = Arrays.stream(contents)
                                    .allMatch(child -> !validPaths.contains(child.getAbsolutePath()));
                            if (allChildrenMarkedForDeletion) {
                                deleteRecursively(file);
                                classLogger.info("Deleted folder with old content: " + path);
                            }
                        }
                    } else {
                        Files.deleteIfExists(file.toPath());
                        classLogger.info("Deleted file: " + path);
                    }
                } catch (IOException e) {
                    classLogger.error("Failed to delete: " + path, e);
                }
            }
        }
    }

    private void deleteRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            for (File child : Objects.requireNonNull(file.listFiles())) {
                deleteRecursively(child);
            }
        }
        Files.deleteIfExists(file.toPath());
    }

    

    private List<File> getAllFilesAndFolders(File baseDir) throws IOException {
        List<File> allFiles = new ArrayList<>();
        Files.walk(baseDir.toPath())
             .forEach(path -> allFiles.add(path.toFile()));
        return allFiles;
    }

    private Map<String, Object> getPayload() {
        GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PAYLOAD.getKey());

        if (grs != null && !grs.isEmpty()) {
            Object allValues = grs.getAllValues();

            if (allValues instanceof Map) {
                return (Map<String, Object>) allValues;
            } else if (allValues instanceof Vector) {
                Vector<?> vector = (Vector<?>) allValues;
                
                if (!vector.isEmpty() && vector.get(0) instanceof Map) {
                    // Directly return the first element if it's a Map
                    return (Map<String, Object>) vector.get(0);
                } else {
                    throw new ClassCastException("Expected Map inside Vector, but found: " + vector.get(0).getClass().getName());
                }
            } else {
                throw new ClassCastException("Payload is of unexpected type: " + allValues.getClass().getName());
            }
        }
        throw new NullPointerException("Payload must be defined for the Function Engine");
    }
}
