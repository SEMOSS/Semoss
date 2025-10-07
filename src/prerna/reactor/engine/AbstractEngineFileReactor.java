package prerna.reactor.engine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;

public abstract class AbstractEngineFileReactor extends AbstractReactor{
	
	protected static final Logger classLogger = LogManager.getLogger(AbstractEngineFileReactor.class);

	/**
	 * 
	 * @param user
	 */
    protected void validateUserAndEngineAccess(User user) {
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

        if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
            throwAnonymousUserError();
        }

        if (SecurityQueryUtils.userIsPublisher(user)) {
            throwUserNotPublisherError();
        }

        if (AbstractSecurityUtils.adminOnlyFunctionAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
            throwFunctionalityOnlyExposedForAdminsError();
        }

    }

    /**
     * 
     * @param engineId
     * @return
     */
    protected String getLocalEngineBaseDirectory(String engineId) {
        IEngine.CATALOG_TYPE catalogType = SecurityEngineUtils.getEngineType(engineId);
        return EngineUtility.getLocalEngineBaseDirectory(catalogType);
    }
    
    /**
     * 
     * @param engineId
     * @return
     */
    protected String getSpecificEngineBaseFolder(String engineId) {
        IEngine.CATALOG_TYPE catalogType = SecurityEngineUtils.getEngineType(engineId);
        String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);
        return EngineUtility.getSpecificEngineBaseFolder(catalogType,  engineId,  engineName);
    }
    
    /**
     * 
     * @return
     */
    protected Map<String, Object> getPayload() {
        GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.PAYLOAD.getKey());

        if (grs != null && !grs.isEmpty()) {
            Object allValues = grs.getAllValues();

            if (allValues instanceof Map) {
                return (Map<String, Object>) allValues;
            } else if (allValues instanceof List) {
            	List<?> vector = (List<?>) allValues;
                
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
    
    /**
     * 
     * @param baseDir
     * @param payload
     * @param currentPath
     * @throws IOException
     */
    public void writeFilesRecursively(Path baseDir, Map<String, Object> payload, Set<String> currentPath) throws IOException {
        for (Map.Entry<String, Object> folderEntry : payload.entrySet()) {
            Object folderDataObj = folderEntry.getValue();
            if (!(folderDataObj instanceof Map)) continue;

            Map<String, Object> folderData = (Map<String, Object>) folderDataObj;
            currentPath.add(baseDir.toString());
            processFolder(baseDir, folderData, currentPath);
        }
    }

    /**
     * 
     * @param currentDir
     * @param folderData
     * @param touchedPaths
     * @throws IOException
     */
    private void processFolder(Path currentDir, Map<String, Object> folderData, Set<String> touchedPaths) throws IOException {
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

        for (Map.Entry<String, Object> entry : folderData.entrySet()) {
            if ("files".equals(entry.getKey()) || !(entry.getValue() instanceof Map)) continue;

            Path subfolderPath = currentDir.resolve(entry.getKey());
            Files.createDirectories(subfolderPath);
            processFolder(subfolderPath, (Map<String, Object>) entry.getValue(), touchedPaths);
        }
    }

    /**
     * 
     * @param baseDir
     * @param validPaths
     * @throws IOException
     */
    protected void deleteRemovedFiles(File baseDir, Set<String> validPaths) throws IOException {
        List<File> allFiles = getAllFilesAndFolders(baseDir);
        allFiles.sort((a, b) -> b.getAbsolutePath().length() - a.getAbsolutePath().length());

        for (File file : allFiles) {
            String path = file.getAbsolutePath();
            if (!validPaths.contains(path)) {
                try {
                    if (file.isDirectory()) {
                        File[] contents = file.listFiles();
                        if (contents == null || contents.length == 0) {
                            Files.delete(file.toPath());
                        } else if (Arrays.stream(contents).allMatch(child -> !validPaths.contains(child.getAbsolutePath()))) {
                            deleteRecursively(file);
                        }
                    } else {
                        Files.deleteIfExists(file.toPath());
                    }
                } catch (IOException e) {
                    classLogger.error("Failed to delete: " + path, e);
                }
            }
        }
    }

    /**
     * 
     * @param file
     * @throws IOException
     */
    private void deleteRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            for (File child : Objects.requireNonNull(file.listFiles())) {
                deleteRecursively(child);
            }
        }
        Files.deleteIfExists(file.toPath());
    }

    /**
     * 
     * @param baseDir
     * @return
     * @throws IOException
     */
    private List<File> getAllFilesAndFolders(File baseDir) throws IOException {
        List<File> allFiles = new ArrayList<>();
        Files.walk(baseDir.toPath()).forEach(path -> allFiles.add(path.toFile()));
        return allFiles;
    }

    /**
     * 
     * @param path
     * @return
     */
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

    /**
     * 
     * @param dir
     * @return
     */
    private static Map<String, Object> traverse(File dir) {
        Map<String, Object> folderData = new HashMap<>();
        List<Map<String, String>> filesList = new ArrayList<>();

        File[] entries = dir.listFiles();
        if (entries != null) {
            for (File entry : entries) {
                if (entry.isDirectory()) {
                    folderData.put(entry.getName(), traverse(entry));
                } else {
                    Map<String, String> fileData = new HashMap<>();
                    fileData.put("fileName", entry.getName());
                    fileData.put("content", readFileContent(entry));
                    filesList.add(fileData);
                }
            }
        }
        folderData.put("files", filesList);
        return folderData;
    }

    /**
     * 
     * @param file
     * @return
     */
    protected static String readFileContent(File file) {
        try {
            return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            classLogger.error("Failed to read file: " + file.getAbsolutePath(), e);
            return "";
        }
    }
}


