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
	public List<String> list(String path) throws IOException, InterruptedException {
		String rCloneConfig = createRcloneConfig();
		String rClonePath = rCloneConfig+":";
		if(BUCKET != null) {
			rClonePath += BUCKET;
		}
		if(path != null) {
			if(!path.startsWith("/")) {
				rClonePath += "/"+path;
			} else {
				rClonePath += path;
			}
		}
		List<String> results = runRcloneProcess(rCloneConfig, "rclone", "lsf", rClonePath);
		return results;
	}
	
	public void moveToStorage(String localFilePath, String storagePath) throws IOException, InterruptedException {
		String rCloneConfig = createRcloneConfig();
		String rClonePath = rCloneConfig+":";
		if(BUCKET != null) {
			rClonePath += BUCKET;
		}
		if(storagePath != null) {
			if(!storagePath.startsWith("/")) {
				rClonePath += "/"+storagePath;
			} else {
				rClonePath += storagePath;
			}
		}
		// wrap in quotes just in case of spaces, etc.
		if(!localFilePath.startsWith("\"")) {
			localFilePath = "\""+localFilePath+"\"";
		}
		runRcloneProcess(rCloneConfig, "rclone", "copy", localFilePath, rClonePath);
	}
	
}
