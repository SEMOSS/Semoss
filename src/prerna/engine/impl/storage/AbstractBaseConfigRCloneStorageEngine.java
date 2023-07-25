package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AbstractBaseConfigRCloneStorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "s3";
	}

	/**
	 * While these are not final values as they are set from the smss
	 * They should not be altered
	 */
	
	protected String BUCKET = null;
	protected String REGION = null;
	protected String ACCESS_KEY = null;
	protected String SECRET_KEY = null;

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
	
}
