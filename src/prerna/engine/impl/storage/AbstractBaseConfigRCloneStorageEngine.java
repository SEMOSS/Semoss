package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.List;

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
	public List<String> list(String path, String rCloneConfig) throws IOException, InterruptedException {
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRcloneConfig();
		}
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
	}
	
	public void copyToStorage(String localFilePath, String storageFolderPath, String rCloneConfig) throws IOException, InterruptedException {
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRcloneConfig();
		}
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
		runRcloneProcess(rCloneConfig, "rclone", "copy", localFilePath, rClonePath);
	}
	
	public void copyToLocal(String storageFilePath, String localFolderPath, String rCloneConfig) throws IOException, InterruptedException {
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRcloneConfig();
		}
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
		runRcloneProcess(rCloneConfig, "rclone", "copy", rClonePath, localFolderPath);
	}
	
	public void deleteFromStorage(String storageFilePath, boolean leaveFolderStructure, String rCloneConfig) throws IOException, InterruptedException {
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRcloneConfig();
		}
		String rClonePath = rCloneConfig+":";
		if(BUCKET != null) {
			rClonePath += BUCKET;
		}
		if(storageFilePath == null || storageFilePath.isEmpty()) {
			throw new NullPointerException("Must define the storage location of the file to download");
		}
		
		storageFilePath = storageFilePath.replace("\\", "/");
		
		if(!storageFilePath.startsWith("/")) {
			storageFilePath = "/"+storageFilePath;
		}
		rClonePath += storageFilePath;

		// wrap in quotes just in case of spaces, etc.
		if(!rClonePath.startsWith("\"")) {
			rClonePath = "\""+rClonePath+"\"";
		}
		
		if(leaveFolderStructure) {
			// always do delete
			runRcloneProcess(rCloneConfig, "rclone", "delete", rClonePath);
		} else {
			// we can only do purge on a folder
			// so need to check
			List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rClonePath);
			if(results.size() == 1 && !results.get(0).endsWith("/")) {
				runRcloneProcess(rCloneConfig, "rclone", "delete", rClonePath);
			} else {
				runRcloneProcess(rCloneConfig, "rclone", "purge", rClonePath);
			}
		}
	}
	
}
