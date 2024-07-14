package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;

public class JCIFSStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(JCIFSStorageEngine.class);

	private static final String NETWORK_DOMAIN = "NETWORK_DOMAIN";
	private static final String PATH_PREFIX = "PATH_PREFIX";
	
	private transient String networkDomain = null;
	private transient String networkUsername = null;
	private transient String networkPassword = null;
	private transient NtlmPasswordAuthentication auth = null;
	
	private transient String pathPrefix = null;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.networkDomain = smssProp.getProperty(NETWORK_DOMAIN);
		this.networkUsername = smssProp.getProperty(Constants.USERNAME);
		this.networkPassword = smssProp.getProperty(Constants.PASSWORD);
		this.auth = new NtlmPasswordAuthentication(this.networkDomain, this.networkUsername, this.networkPassword);
		
		this.pathPrefix = smssProp.getProperty(PATH_PREFIX);
		if(this.pathPrefix == null) {
			this.pathPrefix = "";
		}
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.SMB_CIFS;
	}

	@Override
	public List<String> list(String path) throws Exception {
		SmbFile smbF = new SmbFile(this.pathPrefix + path, this.auth);
		return Arrays.asList(smbF.list());
	}

	@Override
	public List<Map<String, Object>> listDetails(String path) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath, Map<String, Object> metadata) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
