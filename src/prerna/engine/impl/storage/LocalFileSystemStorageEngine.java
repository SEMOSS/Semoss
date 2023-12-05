package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Properties;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Utility;

public class LocalFileSystemStorageEngine extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "local";
	}
	
	// this is not really needed
	public static final String LOCAL_PATH_PREFIX = "LOCAL_PATH_PREFIX";

	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.BUCKET = smssProp.getProperty(LOCAL_PATH_PREFIX);
		if(this.BUCKET == null) {
			throw new IllegalArgumentException("Must provide the " + LOCAL_PATH_PREFIX + " for the local file system");
		}
		
		this.BUCKET = this.BUCKET.replace("\\", "/");
	}
	
	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER);
		return rcloneConfig;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.LOCAL_FILE_SYSTEM;
	}

}
