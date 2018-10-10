package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.zookeeper.Watcher.Event.EventType;

import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.SMSSWebWatcher;
import prerna.util.Utility;

import com.google.common.io.Files;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

public class AZClient {
	
	// this is a singleton
	
	// does some basic ops
	// get the SAS URL for a given container - boolean create or not
	// Delete the container
		
	private static final String PROVIDER = "azureblob";
	private static final String FILE_SEPARATOR = System.getProperty("file.separator");
	
	public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
	public static final String SAS_URL = "SAS_URL";
	public static final String AZ_URI = "AZ_URI";
	public static final String STORAGE = "STORAGE"; // says if this is local / cluster
	public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled
	
	public String azKeyRoot = "/khome";
	
	static AZClient client = null;
	
	CloudBlobClient serviceClient = null;
	SharedAccessBlobPolicy sasConstraints = null;
	String connectionString = null;
	String blobURI = null;
	String sasURL = null;
	String dbFolder = null;
	
	protected AZClient()
	{
		
	}
	
	// create an instance
	// Also needs to be synchronized so multiple calls don't try to init at the same time
	public static synchronized AZClient getInstance()
	{
		if(client == null)
		{
			client = new AZClient();
			client.init();
		}
		return client;
	}
	
	// initialize
	public void init()
	{
		// if the zookeeper is defined.. find from zookeeper what the key is
		// and register for the key change
		// if not.. the storage key is sitting some place pick it up and get it
		String storage = DIHelper.getInstance().getProperty(STORAGE);
		dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "db";
		
		Map <String, String> env = System.getenv();
		if(env.containsKey(KEY_HOME))
			azKeyRoot = env.get(KEY_HOME);

		if(env.containsKey(KEY_HOME.toUpperCase()))
			azKeyRoot = env.get(KEY_HOME.toUpperCase());

		
		if(storage == null || storage.equalsIgnoreCase("LOCAL"))
		{
			// dont bother with anything
			// TODO >>>timb: these should all be centralized somewhere so we know what is needed for cluster
			connectionString = DIHelper.getInstance().getProperty(AZ_CONN_STRING);
			blobURI = DIHelper.getInstance().getProperty(AZ_URI);
			sasURL = DIHelper.getInstance().getProperty(SAS_URL);

		}
		else
		{
			// need the zk piece here
			ZKClient client = ZKClient.getInstance();
			connectionString = client.getNodeData(azKeyRoot, client.zk);
			
			// if SAS_URL it should starts with SAS_URL=			
			if(connectionString.startsWith("SAS_URL="))
				sasURL = connectionString.replace("SAS_URL=", "");
			
			AZStorageListener azList = new AZStorageListener();
			
			client.watchEvent(azKeyRoot, EventType.NodeDataChanged, azList);
			
		}

		createServiceClient();
	}
	
	public void createServiceClient()
	{
		try {
			if(sasURL != null)
			{
				serviceClient = new CloudBlobClient(new StorageUri(new URI(blobURI)),
						new StorageCredentialsSharedAccessSignature(connectionString));
			}
			else
			{
				CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
	            serviceClient = account.createCloudBlobClient();
	            
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	// get SAS URL for a container
	public String getSAS(String containerName)
	{
		String retString = null;
		try {
			//createServiceClient();
			CloudBlobContainer container = serviceClient.getContainerReference(containerName);
			container.createIfNotExists();
			retString = container.getUri() + "?" + container.generateSharedAccessSignature(getSASConstraints(), null); 
			
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retString;
	}
	
	// swaps the key
	public void swapKey(String key)
	{
		// if sasURL is null then it is account
		if(sasURL != null)
			sasURL = key;
		else
			connectionString = key;
		createServiceClient();
	}
	
	public void quarantineContainer(String containerName)
	{
		// take this out in terms of listing
		
	}
	
	public SharedAccessBlobPolicy getSASConstraints()
	{
		if(sasConstraints == null)
		{
			
        sasConstraints = new SharedAccessBlobPolicy();
        
        // get the current time + 24 hours or some
        
        Calendar calendar = Calendar.getInstance();
        
        calendar.add(Calendar.MINUTE, +20);
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
        
		}
        return sasConstraints;

	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		DIHelper.getInstance().loadCoreProp("C:\\Users\\tbanach\\Documents\\Workspace\\Semoss\\RDF_Map.prop");
//		String appId = "a295698a-1f1c-4639-aba6-74b226cd2dfc";
		System.out.println(AZClient.getInstance().getSAS("timb"));
//		AZClient.getInstance().pushAllApps();
//		AZClient.getInstance().pushApp(appId);
//		AZClient.getInstance().pullApp(appId);
	}
	
	private void pushAllApps() throws IOException, InterruptedException {
		File db = new File(dbFolder);
		for (File file : db.listFiles()) {
			String fname = file.getName();
			if (file.isFile() && fname.endsWith(".smss") && !fname.equals("security.smss") && !fname.equals("LocalMasterDatabase.smss")) {
				String appId = fname.replaceAll(".smss", "").substring(fname.indexOf("__") + 2);
				System.out.println("Syncing db for " + appId);
//				AZClient.getInstance().pushApp(appId);
				Thread pushAppThread = new Thread(new PushAppRunner(appId));
				pushAppThread.start();
			}
		}
		
	}
	
	public void updateApp(String appId) throws IOException, InterruptedException {
		if (Utility.getEngine(appId) == null) {
			throw new IllegalArgumentException("App needs to be defined in order to update...");
		}
		pullApp(appId, false);
	}
	
	// TODO >>>timb: need to ctrl shift g this guy
	public void pullApp(String appId) throws IOException, InterruptedException {
		pullApp(appId, true);
	}
	
	// TODO >>>timb: rclone delete on app delete (security utils)
	// TODO >>>timb: need to remove file lock to pull
	// TODO >>>timb: close engine and reopen like export
	private void pullApp(String appId, boolean newEngine) throws IOException, InterruptedException {
		
		// List the smss directory to get the alias + app id
		String smssContainer = appId + "-smss";
		String smssConfig = createRcloneConfig(smssContainer);
		List<String> results = runProcess("rclone", "ls", smssConfig + ":");
		String smss = null;
		for (String result : results) {
			if (result.endsWith(".smss")) {
				smss = result.substring(result.lastIndexOf(' ') + 1);
				break;
			}
		}
		if (smss == null) {
			System.err.println("Failed to pull app for appid=" + appId);
			return;
		}
		String aliasAppId = smss.replaceAll(".smss", "");
		
		// Pull the contents of the app folder before the smss
		File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
		appFolder.mkdir();
		String appConfig = createRcloneConfig(appId);
		System.out.println("Pulling from remote=" + appId + " to target=" + appFolder.getPath());
		
		// If it is a new engine, just pull
		if (newEngine) {
			runProcess("rclone", "copy", appConfig + ":", appFolder.getPath());
		} else {
			
			// Otherwise, need to remove any locks then reopen
			try {
				IEngine engine = Utility.getEngine(appId);
				engine.closeDB();
				runProcess("rclone", "copy", appConfig + ":", appFolder.getPath());
			} finally {
				DIHelper.getInstance().removeLocalProperty(appId);
				Utility.getEngine(appId);
			}
		}
		
		System.out.println("Done pulling from remote=" + appId + " to target=" + appFolder.getPath());
		deleteRcloneConfig(appConfig);

		// Now pull the smss
		System.out.println("Pulling from remote=" + smssContainer + " to target=" + dbFolder);
		runProcess("rclone", "copy", smssConfig + ":", dbFolder);
		System.out.println("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);
		deleteRcloneConfig(smssConfig);
		
		if (newEngine) {
			SMSSWebWatcher.catalogDB(smss, dbFolder);
		}
	}
	
	public void pushApp(String appId) throws IOException, InterruptedException {
		File temp = null;
		File copy = null;
		try {
			File db = new File(dbFolder);
			List<File> files = Arrays.stream(db.listFiles()).parallel().filter(s -> s.getName().contains(appId)).collect(Collectors.toList());
			for (File file : files) {
				String remote = file.isDirectory() ? appId : appId + "-smss";
				String rcloneConfig = createRcloneConfig(remote);
				System.out.println("Pushing from source=" + file.getName() + " to remote=" + remote);
				if (file.isDirectory()) {
					System.out.println("(directory)");
					runProcess("rclone", "copy", file.getPath(), rcloneConfig + ":");
				} else {
					System.out.println("(file)");
					String tempFolder = Utility.getRandomString(10);
					temp = new File(file.getParent() + FILE_SEPARATOR + tempFolder);
					temp.mkdir();
					copy = new File(temp.getPath() + FILE_SEPARATOR + file.getName());
					Files.copy(file, copy);
					runProcess("rclone", "copy", temp.getPath(), rcloneConfig + ":");
				}
				System.out.println("Done pushing from source=" + file.getName() + " to remote=" + remote);
				deleteRcloneConfig(rcloneConfig);
			}
		} finally {
			if (copy != null) {
				copy.delete();
			}
			if (temp != null) {
				temp.delete();
			}
		}
	}
	
	// TODO >>>timb: need to delete too
	private static String createRcloneConfig(String container) throws IOException, InterruptedException {
		System.out.println("Generating SAS for container=" + container);
		String sasUrl = client.getSAS(container);
		String rcloneConfig = Utility.getRandomString(10);
		runProcess("rclone", "config", "create", rcloneConfig, PROVIDER, "sas_url", sasUrl);
		return rcloneConfig;
	}
	
	private static void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
		runProcess("rclone", "config", "delete", rcloneConfig);
	}
	
	// Unfortunately the processes have to be synchronized, otherwise rclone throws errors when it tries to access the config file at the same time
	private static synchronized List<String> runProcess(String... command) throws IOException, InterruptedException {
		Process p = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.directory(new File(System.getProperty("user.home")));
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
	}
	
	private static List<String> streamOutput(InputStream stream) throws IOException {
		return stream(stream, false);
	}
	
	private static List<String> streamError(InputStream stream) throws IOException {
		return stream(stream, true);
	}
	
	private static List<String> stream(InputStream stream, boolean error) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			List<String> lines = reader.lines().collect(Collectors.toList());
			for(String line : lines) {
				if (error) {
					System.err.println(line);
				} else {
					System.out.println(line);
				}
			}
			return lines;
		}
	}
		
}
