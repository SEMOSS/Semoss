package prerna.reactor.codeexec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.text.StringEscapeUtils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExecuteTempPythonFunctionEngineReactor extends AbstractReactor  {

    private static final Logger classLogger = LogManager.getLogger(ExecuteTempPythonFunctionEngineReactor.class);

    public ExecuteTempPythonFunctionEngineReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.PAYLOAD.getKey()
        };
    }

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> payload = getPayload();
		if (payload == null || payload.isEmpty()) {
			throw new IllegalArgumentException("Payload must be provided with the Python files and structure");
		}

		// Directory Path Generation
		Path tempDir = createTempDirectory("temp_engine_");

		if (!Files.exists(tempDir) || !Files.isDirectory(tempDir)) {
			throw new IllegalArgumentException(
					"The specified Temporary Function Engine directory does not exist: " + tempDir);
		}

		try {
			writeFilesToTempDirectory(tempDir, payload);
			return executeFunctionEngine(tempDir, payload);
		} catch (Exception e) {
			classLogger.error("Failed to execute Python Function Engine", e);
			throw new RuntimeException("Error while executing Python Function Engine: " + tempDir);
		} finally {
			deleteTempDirectory(tempDir);
		}
	}

	private Path createTempDirectory(String tempDirName) {
		try {
			Path tempDir = Files.createTempDirectory(tempDirName);
			classLogger.info("Temporary directory created at: " + tempDir);
			return tempDir;
		} catch (IOException e) {
			classLogger.error("Failed to create temporary directory", e);
			throw new RuntimeException("Could not create temporary directory for Function Engine: " + tempDirName);
		}
	}

	private void writeFilesToTempDirectory(Path tempDir, Map<String, Object> payload) throws IOException {
		// Extract the root folder name dynamically
		String rootKey = payload.keySet().iterator().next();
		Map<String, Object> rootFolderData = (Map<String, Object>) payload.get(rootKey);

		// Start the recursive processing from root
		Path rootPath = tempDir.resolve(rootKey);
		Files.createDirectories(rootPath);
		createRecursiveFiles(rootFolderData, rootPath);
	}

	private void createRecursiveFiles(Map<String, Object> folderData, Path currentPath) throws IOException {
		// Write files in current folder
		if (folderData.containsKey("files")) {
			List<Map<String, String>> files = (List<Map<String, String>>) folderData.get("files");
			for (Map<String, String> fileData : files) {
				String fileName = fileData.get("fileName");
				String contentEscaped = fileData.get("content");
				String content = StringEscapeUtils.unescapeJava(contentEscaped);

				Path filePath = currentPath.resolve(fileName);
				Files.write(filePath, content.getBytes(StandardCharsets.UTF_8));
			}
		}

		// Recurse into sub directories
		for (Map.Entry<String, Object> entry : folderData.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			if (!key.equals("files") && value instanceof Map) {
				Path subDir = currentPath.resolve(key);
				Files.createDirectories(subDir);
				createRecursiveFiles((Map<String, Object>) value, subDir);
			}
		}
	}

    
	private NounMetadata executeFunctionEngine(Path tempDir, Map<String, Object> folderStructure) throws Exception {
		// Find all *.py files recursively
		List<File> pythonFiles = Files.walk(tempDir).filter(p -> p.toString().endsWith(".py")).map(Path::toFile)
				.collect(Collectors.toList());
		
		String rootKey = folderStructure.keySet().iterator().next();
		Path rootPath = tempDir.resolve(rootKey);
		

		String pyFunctionPath = Utility.getDIHelperProperty(Constants.PY_FUNCTION);
		Map<String, String> fileOutputs = new HashMap<>();

		// Execute each .py file individually
		for (File pyFile : pythonFiles) {
			// Read the original Python script
			List<String> originalLines = Files.readAllLines(pyFile.toPath());
			
			// Check if the file is empty or only contains comments/whitespace
		    boolean isEmptyOrCommentOnly = true;
		    for (String line : originalLines) {
		        String trimmed = line.trim();
		        if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
		            isEmptyOrCommentOnly = false;
		            break;
		        }
		    }
		    if (isEmptyOrCommentOnly) {
		        continue; // Skip this file
		    }
			
			// Define the paths to inject
			String inject1 = "import sys";
			String inject2 = buildSysPathInject(rootPath.toAbsolutePath().toString());
			String inject3 = buildSysPathInject(pyFunctionPath);

			 // Check if already present
		    boolean needsInject1 = true;
		    boolean needsInject2 = true;
		    boolean needsInject3 = true;

		    for (String line : originalLines) {
		        String trimmed = line.trim();
		        if (trimmed.equals(inject1)) needsInject1 = false;
		        if (trimmed.equals(inject2)) needsInject2 = false;
		        if (trimmed.equals(inject3)) needsInject3 = false;
		    }

		    // Inject only if needed
		    List<String> newLines = new ArrayList<>();
		    if (needsInject1) newLines.add(inject1);
		    if (needsInject2) newLines.add(inject2);
		    if (needsInject3) newLines.add(inject3);
		    newLines.addAll(originalLines);

		    // Create temp file only if anything was injected
		    Path tempScript;
		    if (needsInject1 || needsInject2 || needsInject3) {
		        tempScript = Files.createTempFile("mod_", "_" + pyFile.getName());
		        Files.write(tempScript, newLines);
		    } else {
		        tempScript = pyFile.toPath(); // Use original script directly
		    }

			String relativePath = tempDir.relativize(pyFile.toPath()).toString();

			try {
				String output = executePythonScript(tempScript, tempDir.toFile());
				fileOutputs.put(relativePath, output);
			} catch (RuntimeException e) {
				fileOutputs.put(relativePath, e.getMessage());
			} finally {
			    if (!tempScript.equals(pyFile.toPath())) {
		            Files.deleteIfExists(tempScript); // Clean up temp file
		        }
			}
		}

		return new NounMetadata(fileOutputs, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
	}
    
	private  String executePythonScript(Path scriptPath, File workingDir) throws IOException, InterruptedException {

		String pyExecutionPath = Utility.getDIHelperProperty(Constants.PYTHONHOME);
		Path pythonExecutable = Paths.get(pyExecutionPath, "python.exe");

		if (!Files.exists(pythonExecutable)) {
			throw new IllegalStateException("Python executable not found at: " + pythonExecutable.toString());
		}

		// Prepare and run process with modified script
		ProcessBuilder pb = new ProcessBuilder(pythonExecutable.toString(), scriptPath.toAbsolutePath().toString());
		pb.directory(workingDir);
		pb.redirectErrorStream(true);
		Process process = pb.start();

		String output;
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
			output = reader.lines().collect(Collectors.joining("\n"));
		}

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			throw new RuntimeException("ERROR (code " + exitCode + "): " + output);
		}

		return output;
	}

	private String buildSysPathInject(String path) {
		return "sys.path.append(\"" + path.replace("\\", "\\\\") + "\")";
	}

    private void deleteTempDirectory(Path tempDir) {
        try (Stream<Path> walk = Files.walk(tempDir)) {
            walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            classLogger.info("Temporary directory deleted: " + tempDir);
        } catch (IOException e) {
            classLogger.error("Failed to delete temporary directory", e);
        }
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



