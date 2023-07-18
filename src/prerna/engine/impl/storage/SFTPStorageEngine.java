package prerna.engine.impl.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.FileMode.Type;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import prerna.util.Constants;

public class SFTPStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(SFTPStorageEngine.class);

	private transient SSHClient sshClient = null;
	private transient SFTPClient sftpClient = null;

	private String host = null;
	private String port = "22";
	private String username = null;
	private String password = null;

	@Override
	public STORAGE_TYPE getStorageType() {
		return STORAGE_TYPE.SFTP;
	}

	@Override
	public void connect(Properties smssProp) throws Exception {
		super.connect(smssProp);

		this.host = smssProp.getProperty(Constants.HOSTNAME);
		this.port = smssProp.getProperty(Constants.PORT);
		if(this.port == null || this.port.isEmpty()) {
			this.port = "22"; // default sftp port
		}
		this.username = smssProp.getProperty(Constants.USERNAME);
		this.password = smssProp.getProperty(Constants.PASSWORD);

		sshClient = new SSHClient();
		sshClient.loadKnownHosts();
		sshClient.addHostKeyVerifier(new PromiscuousVerifier());
		sshClient.connect(this.host, Integer.parseInt(this.port.trim()));
		sshClient.getConnection().getKeepAlive().setKeepAliveInterval(5);
		sshClient.authPassword(username, password);
		
		sftpClient = sshClient.newSFTPClient();
	}

	@Override
	public void disconnect() {
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void copyToStorage(String localFilePath, String storageFolderPath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void copyToLocal(String storageFilePath, String localFolderPath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromStorage(String storageFilePath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteFromStorage(String storageFilePath, boolean leaveFolderStructure) throws Exception {
		// TODO Auto-generated method stub

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
//			List<String> list = engine.list("/upload/");
//			System.out.println(list);
//		}
//		{
//			engine.copyToStorage("C:\\Users\\mahkhalil\\Downloads\\MooseAI Logo.png", "test1");
//		}
//		{
//			engine.copyToLocal("test1/MooseAI Logo.png", "C:\\Users\\mahkhalil");
//		}
//		{
//			engine.deleteFromStorage("test1/MooseAI Logo.png");
//		}
//	}


}
