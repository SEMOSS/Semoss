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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IRCloneStorage;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRCloneStorageEngine extends AbstractStorageEngine implements IRCloneStorage {

	private static final Logger classLogger = LogManager.getLogger(AbstractRCloneStorageEngine.class);

	// the path to rclone executable - default assumes in path
	protected String RCLONE = "rclone";
	
	protected String rcloneConfigFolder = null;
	protected String TRANSFER_LIMIT = "8";
	
	// this must be set in the implementing class
	protected String PROVIDER = null;

	// optional bucket
	protected String BUCKET = null;
	
	@Override
	public void close() {
		// since we start and delete rclone configs based on the call
		// there is no disconnect logic
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
	public void syncLocalToStorage(String localPath, String storagePath) throws Exception {
		syncLocalToStorage(localPath, storagePath, null);
	}
	
	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		syncStorageToLocal(storagePath, localPath, null);
	}
	
	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath) throws Exception {
		copyToStorage(localFilePath, storageFolderPath, null);
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		copyToLocal(storageFilePath, localFolderPath, null);
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		deleteFromStorage(storagePath, false, null);
	}
	
	@Override
	public void deleteFromStorage(String storagePath, String rCloneConfig) throws IOException, InterruptedException {
		deleteFromStorage(storagePath, false, rCloneConfig);
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		deleteFromStorage(storagePath, leaveFolderStructure, null);
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
			runRcloneDeleteFileProcess(rCloneConfig, "rclone", "config", "delete", rCloneConfig);
		} finally {
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
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(path != null) {
				path = path.replace("\\", "/");
				if(!path.startsWith("/")) {
					rClonePath += "/"+path;
				} else {
					rClonePath += path;
				}
			}
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rClonePath);
			return results;
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	/**
	 * List the folders/files in the path
	 */
	@Override
	public List<Map<String, Object>> listDetails(String path, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(path != null) {
				path = path.replace("\\", "/");
				if(!path.startsWith("/")) {
					rClonePath += "/"+path;
				} else {
					rClonePath += path;
				}
			}
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			List<Map<String, Object>> results = runRcloneListJsonProcess(rCloneConfig, "rclone", "lsjson", rClonePath, "--max-depth=1");
			return results;
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void syncLocalToStorage(String localPath, String storagePath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(localPath == null || localPath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if(storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}
	
			storagePath = storagePath.replace("\\", "/");
			localPath = localPath.replace("\\", "/");
	
			if(!storagePath.startsWith("/")) {
				storagePath = "/"+storagePath;
			}
			rClonePath += storagePath;
			
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			// wrap in quotes just in case of spaces, etc.
			if(!localPath.startsWith("\"")) {
				localPath = "\""+localPath+"\"";
			}
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", localPath, rClonePath);
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
		
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(localPath == null || localPath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if(storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}
	
			storagePath = storagePath.replace("\\", "/");
			localPath = localPath.replace("\\", "/");
	
			if(!storagePath.startsWith("/")) {
				storagePath = "/"+storagePath;
			}
			rClonePath += storagePath;
			
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			// wrap in quotes just in case of spaces, etc.
			if(!localPath.startsWith("\"")) {
				localPath = "\""+localPath+"\"";
			}
			runRcloneTransferProcess(rCloneConfig, "rclone", "sync", rClonePath, localPath);
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(localFilePath == null || localFilePath.isEmpty()) {
				throw new NullPointerException("Must define the local location of the file to push");
			}
			if(storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the location of the storage folder to move to");
			}
	
			storageFolderPath = storageFolderPath.replace("\\", "/");
			localFilePath = localFilePath.replace("\\", "/");
	
			if(!storageFolderPath.startsWith("/")) {
				storageFolderPath = "/"+storageFolderPath;
			}
			rClonePath += storageFolderPath;
			
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			// wrap in quotes just in case of spaces, etc.
			if(!localFilePath.startsWith("\"")) {
				localFilePath = "\""+localFilePath+"\"";
			}
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", localFilePath, rClonePath);
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(storageFilePath == null || storageFilePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to download");
			}
			if(localFolderPath == null || localFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the location of the local folder to move to");
			}
			
			storageFilePath = storageFilePath.replace("\\", "/");
			localFolderPath = localFolderPath.replace("\\", "/");
			
			if(!storageFilePath.startsWith("/")) {
				storageFilePath = "/"+storageFilePath;
			}
			rClonePath += storageFilePath;
	
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			// wrap in quotes just in case of spaces, etc.
			if(!localFolderPath.startsWith("\"")) {
				localFolderPath = "\""+localFolderPath+"\"";
			}
			runRcloneTransferProcess(rCloneConfig, "rclone", "copy", rClonePath, localFolderPath);
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig();
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(BUCKET != null) {
				rClonePath += BUCKET;
			}
			if(storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to download");
			}
			
			storagePath = storagePath.replace("\\", "/");
			
			if(!storagePath.startsWith("/")) {
				storagePath = "/"+storagePath;
			}
			rClonePath += storagePath;
	
			// wrap in quotes just in case of spaces, etc.
			if(!rClonePath.startsWith("\"")) {
				rClonePath = "\""+rClonePath+"\"";
			}
			
			if(leaveFolderStructure) {
				// always do delete
				runRcloneDeleteFileProcess(rCloneConfig, "rclone", "delete", rClonePath);
			} else {
				// we can only do purge on a folder
				// so need to check
				List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rClonePath);
				if(results.size() == 1 && !results.get(0).endsWith("/")) {
					runRcloneDeleteFileProcess(rCloneConfig, "rclone", "delete", rClonePath);
				} else {
					runRcloneDeleteFileProcess(rCloneConfig, "rclone", "purge", rClonePath);
				}
			}
		} finally {
			if(delete && rCloneConfig != null) {
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
	protected List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
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
	protected List<String> runRcloneTransferProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--transfers");
		commandList.add(TRANSFER_LIMIT);
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
	protected List<String> runRcloneDeleteFileProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
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
	protected List<Map<String, Object>> runRcloneListJsonProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
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
		if( rcloneConfigFolder == null) {
			rcloneConfigFolder =  DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
					+ FILE_SEPARATOR + Constants.DB_FOLDER + FILE_SEPARATOR + 
					SmssUtilities.getUniqueName(this.engineName, this.engineId);
			new File(rcloneConfig).mkdirs();
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
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
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
			System.setSecurityManager(priorManager);
		}
	}
	
	/**
	 * 
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected static Map<String, Object> runProcessJsonOutput(String... command) throws IOException, InterruptedException {
		// Need to allow this process to execute the below commands
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
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
			System.setSecurityManager(priorManager);
		}
	}
	
	/**
	 * 
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected static List<Map<String, Object>> runProcessListJsonOutput(String... command) throws IOException, InterruptedException {
		// Need to allow this process to execute the below commands
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
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
			System.setSecurityManager(priorManager);
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
			for(String line : lines) {
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
			return new Gson().fromJson(builder.toString(), new TypeToken<Map<String, Object>>(){}.getType());
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
			return new Gson().fromJson(builder.toString(), new TypeToken<List<Map<String, Object>>>(){}.getType());
		}
	}

}
