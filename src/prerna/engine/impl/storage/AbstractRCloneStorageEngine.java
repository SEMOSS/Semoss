package prerna.engine.impl.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IRCloneStorage;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractRCloneStorageEngine extends AbstractStorageEngine implements IRCloneStorage {

	private static final Logger classLogger = LogManager.getLogger(AbstractRCloneStorageEngine.class);

	// smss key for additional params
	protected String ADDITIONAL_PARAMETERS_KEY = "ADDITIONAL_PARAMETERS";

	// the path to rclone executable - default assumes in path
	protected String RCLONE = "rclone";

	protected String rcloneConfigFolder = null;
	protected String TRANSFER_LIMIT = "8";

	// this must be set in the implementing class
	protected String PROVIDER = null;

	// optional bucket
	protected String BUCKET = null;

	// optional additional keys
	protected String ADDITIONAL_RCLONE_PARAMETERS = null;

	/**
	 * Init the general storage values
	 * 
	 * @param builder
	 * @throws Exception
	 */
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.ADDITIONAL_RCLONE_PARAMETERS = smssProp.getProperty(ADDITIONAL_PARAMETERS_KEY);
	}

	@Override
	public void close() {
		// since we start and delete rclone configs based on the call
		// there is no disconnect logic
	}

	@Override
	public boolean canReuseRcloneConfig() {
		return true;
	}

	@Override
	public List<String> list(String path) throws IOException, InterruptedException {
		return list(path, null);
	}

	@Override
	public List<Map<String, Object>> listDetails(String path) throws IOException, InterruptedException {
		return listDetails(path, null);
	}

	@Override
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata) throws IOException, InterruptedException {
		syncLocalToStorage(localPath, storagePath, null, metadata);
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws IOException, InterruptedException {
		syncStorageToLocal(storagePath, localPath, null);
	}

	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata) throws IOException, InterruptedException {
		copyToStorage(localFilePath, storageFolderPath, null, metadata);
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws IOException, InterruptedException {
		copyToLocal(storageFilePath, localFolderPath, null);
	}

	@Override
	public void deleteFromStorage(String storagePath) throws IOException, InterruptedException {
		deleteFromStorage(storagePath, false, null);
	}

	@Override
	public void deleteFromStorage(String storagePath, String rCloneConfig) throws IOException, InterruptedException {
		deleteFromStorage(storagePath, false, rCloneConfig);
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws IOException, InterruptedException {
		deleteFromStorage(storagePath, leaveFolderStructure, null);
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws IOException, InterruptedException {
		deleteFolderFromStorage(storageFolderPath, null);
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void deleteRcloneConfig(String rCloneConfig) throws IOException, InterruptedException {
		String configPath = getConfigPath(rCloneConfig);
		try {
			runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "config", "delete", rCloneConfig);
		} finally {
			configPath = Utility.normalizePath(configPath);
			new File(configPath).delete();
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * List the folders/files in the path
	 */
	@Override
	public List<String> list(String path, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (path != null) {
				path = path.replace("\\", "/");
				if (!path.startsWith("/")) {
					rClonePath += "/" + path;
				} else {
					rClonePath += path;
				}
			}
			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}
			List<String> results = runRcloneFastListProcess(rCloneConfig, RCLONE, "lsf", rClonePath);
			return results;
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	/**
	 * List the folders/files in the path
	 */
	@Override
	public List<Map<String, Object>> listDetails(String path, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (path != null) {
				path = path.replace("\\", "/");
				if (!path.startsWith("/")) {
					rClonePath += "/" + path;
				} else {
					rClonePath += path;
				}
			}
			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}
			List<Map<String, Object>> results = runRcloneListJsonProcess(rCloneConfig, RCLONE, "lsjson", rClonePath,
					"--max-depth=1", "--metadata");
			return results;
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void syncLocalToStorage(String localPath, String storagePath, String rCloneConfig, Map<String, Object> metadata)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (localPath == null || localPath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}

			storagePath = storagePath.replace("\\", "/");
			localPath = localPath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}
			rClonePath += storagePath;

			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localPath.startsWith("'")) {
//				localPath = "'"+localPath+"'";
//			}
			
			// Initialize metadata to an empty map if it is null
            if (metadata == null) {
                metadata = new HashMap<>();
            }
			
			List<String> values = new ArrayList<>(metadata.keySet().size()*2+5);
			values.add(RCLONE);
			values.add("sync");
			values.add(localPath);
			values.add(rClonePath);
			values.add("--metadata");
			
			if(!metadata.isEmpty()) {
				for(String key : metadata.keySet()) {
					Object value = metadata.get(key);
					
					values.add("--metadata-set");
					// wrap around in quotes just in case ...
					values.add("\""+key+"\"=\""+value+"\"");
				}
			}
			
			runRcloneTransferProcess(rCloneConfig, values.toArray(new String[]{}));
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}

	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (localPath == null || localPath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}

			storagePath = storagePath.replace("\\", "/");
			localPath = localPath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}
			rClonePath += storagePath;

			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localPath.startsWith("'")) {
//				localPath = "'"+localPath+"'";
//			}
			runRcloneTransferProcess(rCloneConfig, RCLONE, "sync", rClonePath, localPath);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, String rCloneConfig, Map<String, Object> metadata)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (localFilePath == null || localFilePath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if (storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}

			storageFolderPath = storageFolderPath.replace("\\", "/");
			localFilePath = localFilePath.replace("\\", "/");

			if (!storageFolderPath.startsWith("/")) {
				storageFolderPath = "/" + storageFolderPath;
			}
			rClonePath += storageFolderPath;

//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localFilePath.startsWith("'")) {
//				localFilePath = "'"+localFilePath+"'";
//			}
			
			// Initialize metadata to an empty map if it is null
            if (metadata == null) {
                metadata = new HashMap<>();
            }
			
			List<String> values = new ArrayList<>(metadata.keySet().size()*2+5);
			values.add(RCLONE);
			values.add("copy");
			values.add(localFilePath);
			values.add(rClonePath);
			values.add("--metadata");
			
			if(!metadata.isEmpty()) {
				for(String key : metadata.keySet()) {
					Object value = metadata.get(key);
					
					values.add("--metadata-set");
					// wrap around in quotes just in case ...
					values.add("\""+key+"\"=\""+value+"\"");
				}
			}
			
			runRcloneTransferProcess(rCloneConfig, values.toArray(new String[]{}));
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (storageFilePath == null || storageFilePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to download");
			}
			if (localFolderPath == null || localFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the location of the local folder to move to");
			}

			storageFilePath = storageFilePath.replace("\\", "/");
			localFolderPath = localFolderPath.replace("\\", "/");

			if (!storageFilePath.startsWith("/")) {
				storageFilePath = "/" + storageFilePath;
			}
			rClonePath += storageFilePath;

//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localFolderPath.startsWith("'")) {
//				localFolderPath = "'"+localFolderPath+"'";
//			}
			runRcloneTransferProcess(rCloneConfig, RCLONE, "copy", rClonePath, localFolderPath);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to delete");
			}

			storagePath = storagePath.replace("\\", "/");

			if (!storagePath.startsWith("/")) {
				storagePath = "/" + storagePath;
			}
			rClonePath += storagePath;

//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}

			if (leaveFolderStructure) {
				// always do delete
				runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "delete", rClonePath);
			} else {
				// we can only do purge on a folder
				// so need to check
				List<String> results = runRcloneFastListProcess(rCloneConfig, RCLONE, "lsf", rClonePath);
				if (results.size() == 1 && !results.get(0).endsWith("/")) {
					runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "delete", rClonePath);
				} else {
					runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "purge", rClonePath);
				}
			}
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath, String rCloneConfig)
			throws IOException, InterruptedException {
		boolean delete = false;
		if (rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig + ":";
			if (BUCKET != null) {
				rClonePath += BUCKET;
			}
			if (storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the folder to delete");
			}

			storageFolderPath = storageFolderPath.replace("\\", "/");

			if (!storageFolderPath.startsWith("/")) {
				storageFolderPath = "/" + storageFolderPath;
			}
			rClonePath += storageFolderPath;

//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("'")) {
//				rClonePath = "'"+rClonePath+"'";
//			}

			runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "purge", rClonePath);
		} finally {
			if (delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}

	}

	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneProcess(String rcloneConfig, String... command)
			throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneFastListProcess(String rcloneConfig, String... command)
			throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		commandList.add("--fast-list");
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneTransferProcess(String rcloneConfig, String... command)
			throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--transfers");
		commandList.add(TRANSFER_LIMIT);
		commandList.add("--config");
		commandList.add(configPath);
		commandList.add("--fast-list");

		if (this.ADDITIONAL_RCLONE_PARAMETERS != null && !this.ADDITIONAL_RCLONE_PARAMETERS.isEmpty()) {
			String[] additionalParams = this.ADDITIONAL_RCLONE_PARAMETERS.split(" ");
			for (String addP : additionalParams) {
				if (addP == null || addP.isEmpty()) {
					continue;
				}
				commandList.add(addP);
			}
		}

		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneDeleteFileProcess(String rcloneConfig, String... command)
			throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<Map<String, Object>> runRcloneListJsonProcess(String rcloneConfig, String... command)
			throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runProcessListJsonOutput(newCommand);
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @return
	 */
	protected String getConfigPath(String rcloneConfig) {
		if (rcloneConfigFolder == null) {
			rcloneConfigFolder = Utility.getBaseFolder() + FILE_SEPARATOR + Constants.STORAGE_FOLDER + FILE_SEPARATOR
					+ SmssUtilities.getUniqueName(this.engineName, this.engineId);
			new File(Utility.normalizePath(rcloneConfig)).mkdirs();
		}

		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
	}

	@Override
	public void setRCloneConfigFolder(String folderPath) {
		this.rcloneConfigFolder = folderPath;
	}

	/**
	 * 
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected static List<String> runAnyProcess(String... command) throws IOException, InterruptedException {
		// Need to allow this process to execute the below commands
//		SecurityManager priorManager = System.getSecurityManager();
//		System.setSecurityManager(null);
		try {
			Process p = null;
			try {
				ProcessBuilder pb = new ProcessBuilder(command);
				pb.directory(new File(Utility.normalizePath(System.getProperty("user.home"))));
				pb.redirectOutput(Redirect.PIPE);
				pb.redirectError(Redirect.PIPE);
				p = pb.start();
				p.waitFor();
				List<String> results = streamOutput(p.getInputStream());
				streamError(p.getErrorStream());
				return results;
			} finally {
				if (p != null) {
					p.destroyForcibly();
				}
			}
		} finally {
//			System.setSecurityManager(priorManager);
		}
	}

	/**
	 * 
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected static Map<String, Object> runProcessJsonOutput(String... command)
			throws IOException, InterruptedException {
		// Need to allow this process to execute the below commands
//		SecurityManager priorManager = System.getSecurityManager();
//		System.setSecurityManager(null);
		try {
			Process p = null;
			try {
				ProcessBuilder pb = new ProcessBuilder(command);
				pb.directory(new File(Utility.normalizePath(System.getProperty("user.home"))));
				pb.redirectOutput(Redirect.PIPE);
				pb.redirectError(Redirect.PIPE);
				p = pb.start();
				p.waitFor();
				Map<String, Object> results = streamJsonOutput(p.getInputStream());
				streamError(p.getErrorStream());
				return results;
			} finally {
				if (p != null) {
					p.destroyForcibly();
				}
			}
		} finally {
//			System.setSecurityManager(priorManager);
		}
	}

	/**
	 * 
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected static List<Map<String, Object>> runProcessListJsonOutput(String... command)
			throws IOException, InterruptedException {
		// Need to allow this process to execute the below commands
//		SecurityManager priorManager = System.getSecurityManager();
//		System.setSecurityManager(null);
		try {
			Process p = null;
			try {
				ProcessBuilder pb = new ProcessBuilder(command);
				pb.directory(new File(Utility.normalizePath(System.getProperty("user.home"))));
				pb.redirectOutput(Redirect.PIPE);
				pb.redirectError(Redirect.PIPE);
				p = pb.start();
				p.waitFor();
				List<Map<String, Object>> results = streamListJsonOutput(p.getInputStream());
				streamError(p.getErrorStream());
				return results;
			} finally {
				if (p != null) {
					p.destroyForcibly();
				}
			}
		} finally {
//			System.setSecurityManager(priorManager);
		}
	}

	/**
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	protected static List<String> streamOutput(InputStream stream) throws IOException {
		return stream(stream, false);
	}

	/**
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	protected static List<String> streamError(InputStream stream) throws IOException {
		return stream(stream, true);
	}

	/**
	 * 
	 * @param stream
	 * @param error
	 * @return
	 * @throws IOException
	 */
	protected static List<String> stream(InputStream stream, boolean error) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			List<String> lines = reader.lines().collect(Collectors.toList());
			for (String line : lines) {
				if (error) {
					classLogger.warn(line);
					System.err.println(line);
				} else {
					classLogger.info(line);
					System.out.println(line);
				}
			}
			return lines;
		}
	}

	/**
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	protected static Map<String, Object> streamJsonOutput(InputStream stream) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			StringBuilder builder = new StringBuilder();
			reader.lines().forEach(line -> builder.append(line));
			classLogger.info(builder.toString());
			System.out.println(builder.toString());
			return new Gson().fromJson(builder.toString(), new TypeToken<Map<String, Object>>() {
			}.getType());
		}
	}

	/**
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	protected static List<Map<String, Object>> streamListJsonOutput(InputStream stream) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			StringBuilder builder = new StringBuilder();
			reader.lines().forEach(line -> builder.append(line));
			classLogger.info(builder.toString());
			System.out.println(builder.toString());
			return new Gson().fromJson(builder.toString(), new TypeToken<List<Map<String, Object>>>() {
			}.getType());
		}
	}

}
