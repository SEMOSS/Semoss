package prerna.engine.impl.storage;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;
import prerna.util.Utility;

public class AzureBlobStorageEngine extends AbstractRCloneStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureBlobStorageEngine.class);

	{
		this.PROVIDER = "azureblob";
	}

	public static final String AZ_ACCOUNT_NAME = "AZ_ACCOUNT_NAME";
	public static final String AZ_PRIMARY_KEY = "AZ_PRIMARY_KEY";

	public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
	public static final String AZ_SAS_URL = "SAS_URL";
	public static final String AZ_URI = "AZ_URI";

	public static final String AZ_GENERATE_DYNAMIC_SAS = "AZ_GENERATE_DYNAMIC_SAS";
	
	private transient String accountName = null;
	private transient String primaryKey = null;
	
	private transient CloudBlobClient serviceClient = null;
	private transient String connectionString = null;

	private transient boolean generateDynamicSAS = true;
	
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		this.accountName = smssProp.getProperty(AZ_ACCOUNT_NAME);
		this.primaryKey = smssProp.getProperty(AZ_PRIMARY_KEY);
		this.connectionString = smssProp.getProperty(AZ_CONN_STRING);
		
		// default to using dynamic SAS
		this.generateDynamicSAS = Boolean.parseBoolean(smssProp.getProperty(AZ_GENERATE_DYNAMIC_SAS, "true"));
		
		if(this.generateDynamicSAS) {
			createServiceClient();
		}
	}
	
	/**
	 * 
	 */
	public void createServiceClient() {
		try {
			CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
			this.serviceClient = account.createCloudBlobClient();
		} catch (URISyntaxException use) {
			classLogger.error(Constants.STACKTRACE, use);
		} catch (InvalidKeyException ike) {
			classLogger.error(Constants.STACKTRACE, ike);
		}
	}
	
	/**
	 * 
	 * @param containerName
	 * @return
	 */
	public String getDynamicSAS(String containerName) {
		String retString = null;
		try {
			//createServiceClient();
			CloudBlobContainer container = serviceClient.getContainerReference(containerName);
//			if(!container.exists()) {
//				classLogger.info("Created service access signature for container that does not yet exist");
//				container.create();
//			}
			container.createIfNotExists();
			retString = container.getUri() + "?" + container.generateSharedAccessSignature(getSASConstraints(), null); 
		} catch (URISyntaxException use) {
			classLogger.error(Constants.STACKTRACE, use);
		} catch (StorageException se) {
			classLogger.error(Constants.STACKTRACE, se);
		} catch (InvalidKeyException ike) {
			classLogger.error(Constants.STACKTRACE, ike);
		}

		return retString;
	}
	
	/**
	 * 
	 * @return
	 */
	private SharedAccessBlobPolicy getSASConstraints() {
		SharedAccessBlobPolicy sasConstraints = null;
		sasConstraints = new SharedAccessBlobPolicy();

		// get the current time + 24 hours or some
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, +5);
		Date date = calendar.getTime();

		sasConstraints.setSharedAccessExpiryTime(date);

		EnumSet <SharedAccessBlobPermissions> permSet = EnumSet.noneOf(SharedAccessBlobPermissions.class);
		// I need to read the database to find if this guy is allowed etc. but for now
		permSet.add(SharedAccessBlobPermissions.LIST);
		permSet.add(SharedAccessBlobPermissions.WRITE);
		permSet.add(SharedAccessBlobPermissions.CREATE);
		permSet.add(SharedAccessBlobPermissions.READ);
		permSet.add(SharedAccessBlobPermissions.DELETE);
		permSet.add(SharedAccessBlobPermissions.ADD);

		sasConstraints.setPermissions(permSet);
		return sasConstraints;
	}
	
	@Override
	public String createRCloneConfig() throws IOException, InterruptedException {
		if(this.generateDynamicSAS) {
			classLogger.warn("Calling creation of rclone without passing in the container name to generate a SAS");
			classLogger.warn("Calling creation of rclone without passing in the container name to generate a SAS");
			classLogger.warn("Calling creation of rclone without passing in the container name to generate a SAS");
		}
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "account", accountName, "key", primaryKey);
		return rcloneConfig;
	}
	
	public String createRCloneConfig(String containerName) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		
		if(this.generateDynamicSAS) {
			String sasUrl = getDynamicSAS(containerName);
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "sas_url", sasUrl);
		} else {
			runRcloneProcess(rcloneConfig, RCLONE, "config", "create", rcloneConfig, PROVIDER, "account", accountName, "key", primaryKey);
		}
		
		return rcloneConfig;
	}
	
	@Override
	public boolean canReuseRcloneConfig() {
		return !this.generateDynamicSAS;
	}


	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.MICROSOFT_AZURE_BLOB_STORAGE;
	}
	
	/*
	 * 
	 * OVERRIDING THESE METHODS FROM BASE BECAUSE WE NEED TO FIGURE OUT THE CONTAINER WHEN USING DYNAMIC SAS
	 * 
	 */
	
	private String getContainerFromPath(String path) {
		if(path.startsWith("/") || path.startsWith("\\")) {
			path.substring(1);
		}
		File f = new File(path);
		while(f.getParentFile() != null) {
			f = f.getParentFile();
		}
		return f.getName();
	}
	
	/**
	 * List the folders/files in the path
	 */
	@Override
	public List<String> list(String path, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(path));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(path != null) {
				path = path.replace("\\", "/");
				if(!path.startsWith("/")) {
					rClonePath += "/"+path;
				} else {
					rClonePath += path;
				}
			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
			List<String> results = runRcloneFastListProcess(rCloneConfig, RCLONE, "lsf", rClonePath);
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
			rCloneConfig = createRCloneConfig(getContainerFromPath(path));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(path != null) {
				path = path.replace("\\", "/");
				if(!path.startsWith("/")) {
					rClonePath += "/"+path;
				} else {
					rClonePath += path;
				}
			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
			List<Map<String, Object>> results = runRcloneListJsonProcess(rCloneConfig, RCLONE, "lsjson", rClonePath,
					"--max-depth=1", "--metadata");
			return results;
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void syncLocalToStorage(String localPath, String storagePath, String rCloneConfig, Map<String, Object> metadata) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storagePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
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
			
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localPath.startsWith("\"")) {
//				localPath = "\""+localPath+"\"";
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
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
		
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storagePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
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
			
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localPath.startsWith("\"")) {
//				localPath = "\""+localPath+"\"";
//			}
			runRcloneTransferProcess(rCloneConfig, RCLONE, "sync", rClonePath, localPath);
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, String rCloneConfig, Map<String, Object> metadata) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storageFolderPath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
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
			
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localFilePath.startsWith("\"")) {
//				localFilePath = "\""+localFilePath+"\"";
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
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storageFilePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
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
	
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
//			// wrap in quotes just in case of spaces, etc.
//			if(!localFolderPath.startsWith("\"")) {
//				localFolderPath = "\""+localFolderPath+"\"";
//			}
			runRcloneTransferProcess(rCloneConfig, RCLONE, "copy", rClonePath, localFolderPath);
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
			rCloneConfig = createRCloneConfig(getContainerFromPath(storagePath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(storagePath == null || storagePath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the file to delete");
			}
			
			storagePath = storagePath.replace("\\", "/");
			
			if(!storagePath.startsWith("/")) {
				storagePath = "/"+storagePath;
			}
			rClonePath += storagePath;
	
			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
			
			if(leaveFolderStructure) {
				// always do delete
				runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "delete", rClonePath);
			} else {
				// we can only do purge on a folder
				// so need to check
				List<String> results = runRcloneFastListProcess(rCloneConfig, RCLONE, "lsf", rClonePath);
				if(results.size() == 1 && !results.get(0).endsWith("/")) {
					runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "delete", rClonePath);
				} else {
					runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "purge", rClonePath);
				}
			}
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}
	
	@Override
	public void deleteFolderFromStorage(String storageFolderPath, String rCloneConfig) throws IOException, InterruptedException {
		boolean delete = false;
		if(rCloneConfig == null || rCloneConfig.isEmpty()) {
			rCloneConfig = createRCloneConfig(getContainerFromPath(storageFolderPath));
			delete = true;
		}
		try {
			String rClonePath = rCloneConfig+":";
			if(storageFolderPath == null || storageFolderPath.isEmpty()) {
				throw new NullPointerException("Must define the storage location of the folder to delete");
			}
			
			storageFolderPath = storageFolderPath.replace("\\", "/");
			
			if(!storageFolderPath.startsWith("/")) {
				storageFolderPath= "/"+storageFolderPath;
			}
			rClonePath += storageFolderPath;
	
//			// wrap in quotes just in case of spaces, etc.
//			if(!rClonePath.startsWith("\"")) {
//				rClonePath = "\""+rClonePath+"\"";
//			}
			
			runRcloneDeleteFileProcess(rCloneConfig, RCLONE, "purge", rClonePath);
		} finally {
			if(delete && rCloneConfig != null) {
				deleteRcloneConfig(rCloneConfig);
			}
		}
	}

	
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
//	public static void main(String[] args) throws Exception {
//		// these are not real/import access/secret 
//		Properties mockSmss = new Properties();
//		mockSmss.put(AZ_CONN_STRING, "");
//		mockSmss.put(AZ_ACCOUNT_NAME, "");
//		mockSmss.put(AZ_PRIMARY_KEY, "");
//		mockSmss.put(AZ_GENERATE_DYNAMIC_SAS, "true");
//
//		AzureBlobStorageEngine engine = new AzureBlobStorageEngine();
//		engine.open(mockSmss);
//		
//		{
//			List<String> list = engine.list("08e03a5f-9b8d-4f24-a3f7-ba6959f2c5c0/version");
//			System.out.println(list);
//		}
//		{
//			List<Map<String, Object>> list = engine.listDetails("08e03a5f-9b8d-4f24-a3f7-ba6959f2c5c0/version");
//			System.out.println(list);
//		}
//		engine.close();
//	}
	
	
	
}
