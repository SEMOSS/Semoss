package prerna.engine.impl.storage;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.FileMode.Type;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import net.schmizz.sshj.xfer.LocalDestFile;
import net.schmizz.sshj.xfer.LocalSourceFile;
import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;

public class SFTPStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(SFTPStorageEngine.class);

	private transient SSHClient sshClient = null;
	private transient SFTPClient sftpClient = null;

	private String host = null;
	private String port = "22";
	private transient String username = null;
	private transient String password = null;

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.SFTP;
	}

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.host = smssProp.getProperty(Constants.HOSTNAME);
		this.port = smssProp.getProperty(Constants.PORT);
		if(this.port == null || this.port.isEmpty()) {
			this.port = "22"; // default sftp port
		}
		this.username = smssProp.getProperty(Constants.USERNAME);
		this.password = smssProp.getProperty(Constants.PASSWORD);

		sshClient = new SSHClient();
		try {
			sshClient.loadKnownHosts();
		} catch(IOException e) {
			classLogger.warn("Unable to find/load known hosts... ignoring error");
		}
		sshClient.addHostKeyVerifier(new PromiscuousVerifier());
		sshClient.connect(this.host, Integer.parseInt(this.port.trim()));
		sshClient.getConnection().getKeepAlive().setKeepAliveInterval(5);
		sshClient.authPassword(username, password);
		
		sftpClient = sshClient.newSFTPClient();
	}

	@Override
	public void close() {
		if(this.sftpClient != null) {
			try {
				this.sftpClient.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		if(this.sshClient != null) {
			try {
				this.sshClient.disconnect();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	@Override
	public List<String> list(String path) throws Exception {
		List<RemoteResourceInfo> info = this.sftpClient.ls(path);
		List<String> names = new ArrayList<>(info.size());
		for(RemoteResourceInfo remoteInfo : info) {
			String name = remoteInfo.getName();
			Type fType = remoteInfo.getAttributes().getType();
			if(fType == Type.DIRECTORY) {
				names.add(name+"/");
			} else {
				names.add(name);	
			}
		}
		return names;
	}

	@Override
	public List<Map<String, Object>> listDetails(String path) throws Exception {
		// making these match the rclone names
		// [
		// {Path=Uber Eats July 17.pdf, Name=Uber Eats July 17.pdf, Size=51049.0, MimeType=application/pdf, ModTime=2023-07-17T20:54:33.767000000Z, IsDir=false, Tier=STANDARD}, 
		// {Path=college.csv, Name=college.csv, Size=1698156.0, MimeType=text/csv, ModTime=2023-07-15T17:30:38.574000000Z, IsDir=false, Tier=STANDARD}, 
		// {Path=testFolder, Name=testFolder, Size=0.0, MimeType=inode/directory, ModTime=2023-07-18T17:46:11.358608500-04:00, IsDir=true}
		// ]

		List<RemoteResourceInfo> info = this.sftpClient.ls(path);
		List<Map<String, Object>> names = new ArrayList<>(info.size());
		for(RemoteResourceInfo remoteInfo : info) {
			Map<String, Object> item = new HashMap<>();
			item.put("Name", remoteInfo.getName());
			item.put("Path", remoteInfo.getPath());
			FileAttributes attributes = remoteInfo.getAttributes();
			item.put("Size", attributes.getSize());
			item.put("IsDir", attributes.getType() == Type.DIRECTORY);
			item.put("ModTime", ZonedDateTime.ofInstant(
					Instant.ofEpochMilli(attributes.getMtime()*1000L), TimeZone.getDefault().toZoneId())
					);
			names.add(item);
		}
		return names;
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
		
		LocalSourceFile lsf = new FileSystemFile(localFilePath);
		sftpClient.put(lsf, storageFolderPath);
	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
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
		
		LocalDestFile ldf = new FileSystemFile(localFolderPath);
		sftpClient.get(storageFilePath, ldf);
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {
		deleteFromStorage(storagePath, false);
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		if(storagePath == null || storagePath.isEmpty()) {
			throw new NullPointerException("Must define the storage location of the file to delete");
		}
		storagePath = storagePath.replace("\\", "/");

		if(!storagePath.startsWith("/")) {
			storagePath = "/"+storagePath;
		}
		
		FileAttributes attributes = sftpClient.statExistence(storagePath);
		if(attributes == null) {
			throw new IllegalArgumentException("Storage file/folder " + storagePath + " does not exist");
		}
		
		if(attributes.getType() == FileMode.Type.DIRECTORY) {
			if(leaveFolderStructure) {
				recursivelyDeleteFiles(storagePath);
			} else {
				sftpClient.rmdir(storagePath);
			}
		} else {
			sftpClient.rm(storagePath);
		}
	}
	
	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		if(storageFolderPath == null || storageFolderPath.isEmpty()) {
			throw new NullPointerException("Must define the storage location of the folder to delete");
		}
		storageFolderPath = storageFolderPath.replace("\\", "/");

		if(!storageFolderPath.startsWith("/")) {
			storageFolderPath = "/"+storageFolderPath;
		}
		
		FileAttributes attributes = sftpClient.statExistence(storageFolderPath);
		if(attributes == null) {
			throw new IllegalArgumentException("Storage folder " + storageFolderPath + " does not exist");
		}
		
		if(attributes.getType() != FileMode.Type.DIRECTORY) {
			throw new IllegalArgumentException("Storage path " + storageFolderPath + " is not a directory");
		}

		sftpClient.rmdir(storageFolderPath);
	}

	private void recursivelyDeleteFiles(String storageDirectory) throws IOException {
		List<RemoteResourceInfo> ls = sftpClient.ls(storageDirectory);
		for(RemoteResourceInfo f : ls) {
			if(f.getAttributes().getType() == FileMode.Type.DIRECTORY) {
				recursivelyDeleteFiles(f.getPath());
			} else {
				sftpClient.rm(f.getPath());
			}
		}
	}

	

	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception {
//		/**
//		 * 
//		 	version: '3.1'
//			services:
//			    sftp:
//			        image: atmoz/sftp
//			        volumes:
//			            - C:\Users\mahkhalil\Documents\sftp\mount:/home/foo/upload
//			        ports:
//			            - "2222:22"
//			        command: foo:pass:1001
//		 */
//
//		// these are not real/import access/secret - only for local docker
//		Properties mockSmss = new Properties();
//		mockSmss.put(Constants.HOSTNAME, "localhost");
//		mockSmss.put(Constants.PORT, "2222");
//		mockSmss.put(Constants.USERNAME, "foo");
//		mockSmss.put(Constants.PASSWORD, "pass");
//
//		SFTPStorageEngine engine = new SFTPStorageEngine();
//		engine.connect(mockSmss);
//
//		{
//			List<String> list = engine.list("/");
//			System.out.println(list);
//		}
//		{
//			List<Map<String, Object>> list = engine.listDetails("/upload/");
//			System.out.println(list);
//		}
//		{
//			engine.copyToStorage("C:\\Users\\mahkhalil\\Downloads\\MooseAI Logo.png", "upload/test1");
//		}
//		{
//			engine.copyToLocal("upload/MooseAI Logo.png", "C:\\Users\\mahkhalil");
//		}
//		{
//			engine.deleteFromStorage("upload/test1/MooseAI Logo.png");
//		}
//		
//		engine.disconnect();
//	}


}
