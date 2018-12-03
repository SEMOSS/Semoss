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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.zookeeper.Watcher.Event.EventType;

import prerna.auth.utils.SecurityQueryUtils;
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
	private static final String SMSS_POSTFIX = "-smss";
	private static final String FILE_SEPARATOR = System.getProperty("file.separator");
	
	public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
	public static final String AZ_NAME = "AZ_NAME";
	public static final String AZ_KEY = "AZ_KEY";
	public static final String SAS_URL = "SAS_URL";
	public static final String AZ_URI = "AZ_URI";
	public static final String STORAGE = "STORAGE"; // says if this is local / cluster
	public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled
	
	public String azKeyRoot = "/khome";
	
	static AZClient client = null;
	static String rcloneConfigFolder = null;

	CloudBlobClient serviceClient = null;
	SharedAccessBlobPolicy sasConstraints = null;
	String connectionString = null;
	String name = null;
	String key = null;
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
		rcloneConfigFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";		
		new File(rcloneConfigFolder).mkdir();
		
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
			name = DIHelper.getInstance().getProperty(AZ_NAME);
			key = DIHelper.getInstance().getProperty(AZ_KEY);
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
        
        calendar.add(Calendar.HOUR, +24);
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
//		System.out.println(AZClient.getInstance().getSAS("timb"));
//		AZClient.getInstance().deleteApp("1bab355d-a2ea-4fde-9d2c-088287d46978");
//		AZClient.getInstance().pushApp(appId);
//		AZClient.getInstance().pullApp(appId);
//		List<String> containers = AZClient.getInstance().listAllBlobContainers();
//		for(String container : containers) {
//			System.out.println(container);
//		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Push ////////////////////////////////////////////
	
	public void pushApp(String appId) throws IOException, InterruptedException {
		IEngine engine = Utility.getEngine(appId, false);
		if (engine == null) {
			throw new IllegalArgumentException("App not found...");
		}
		
		// We need to push the folder alias__appId and the file alias__appId.smss
		String alias = SecurityQueryUtils.getEngineAliasForId(appId);
		String appReference = alias + "__" + appId;
		String appFolder = dbFolder + FILE_SEPARATOR + appReference;
		String smss = appReference + ".smss";
		String smssFile = dbFolder + FILE_SEPARATOR + smss;

		// Start with the sas token
		String appRcloneConfig = null;
		String smssRCloneConfig = null;
		try {
			appRcloneConfig = createRcloneConfig(appId);
			String smssContainer = appId + "-smss";
			smssRCloneConfig = createRcloneConfig(smssContainer);
			
			// Some temp files needed for the transfer
			File temp = null;
			File copy = null;
			boolean opened = false;
			try {
				
				// Close the database, so that we can push without file locks (also ensures that the db doesn't change mid push)
				engine.closeDB();
				
				// Push the app folder
				System.out.println("Pushing from source=" + appFolder + " to remote=" + appId);
				runRcloneProcess(appRcloneConfig, "rclone", "sync", appFolder, appRcloneConfig + ":");
				System.out.println("Done pushing from source=" + appFolder + " to remote=" + appId);
				
				// Move the smss to an empty temp directory (otherwise will push all items in the db folder)
				String tempFolder = Utility.getRandomString(10);
				temp = new File(dbFolder + FILE_SEPARATOR + tempFolder);
				temp.mkdir();
				copy = new File(temp.getPath() + FILE_SEPARATOR + smss);
				Files.copy(new File(smssFile), copy);
				
				// Push the smss
				System.out.println("Pushing from source=" + smssFile + " to remote=" + smssContainer);
				runRcloneProcess(smssRCloneConfig, "rclone", "sync", temp.getPath(), smssRCloneConfig + ":");
				System.out.println("Done pushing from source=" + smssFile + " to remote=" + smssContainer);
				
				// Re-open the database
				DIHelper.getInstance().removeLocalProperty(appId);
				Utility.getEngine(appId, false);
				opened = true;
			} finally {
				if (copy != null) {
					copy.delete();
				}
				if (temp != null) {
					temp.delete();
				}
				if (!opened) {
					DIHelper.getInstance().removeLocalProperty(appId);
					Utility.getEngine(appId, false);
				}
			}
		} finally {
			if (appRcloneConfig != null) {
				deleteRcloneConfig(appRcloneConfig);
			}
			if (smssRCloneConfig != null) {
				deleteRcloneConfig(smssRCloneConfig);
			}
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Pull ////////////////////////////////////////////
	
	public void pullApp(String appId) throws IOException, InterruptedException {
		pullApp(appId, true);
	}
	
	private void pullApp(String appId, boolean newEngine) throws IOException, InterruptedException {
		
		// List the smss directory to get the alias + app id
		String smssContainer = appId + SMSS_POSTFIX;
		String smssConfig = createRcloneConfig(smssContainer);
		try {
			List<String> results = runRcloneProcess(smssConfig, "rclone", "lsf", smssConfig + ":");
			String smss = null;
			for (String result : results) {
				if (result.endsWith(".smss")) {
					smss = result;
					break;
				}
			}
			if (smss == null) {
				throw new IOException("Failed to pull app for appid=" + appId);
			}
			String aliasAppId = smss.replaceAll(".smss", "");
			
			// Pull the contents of the app folder before the smss
			File appFolder = new File(dbFolder + FILE_SEPARATOR + aliasAppId);
			appFolder.mkdir();
			System.out.println("Pulling from remote=" + appId + " to target=" + appFolder.getPath());
			
			// If it is a new engine, just pull
			String appConfig = createRcloneConfig(appId);
			try {
				if (newEngine) {
					runRcloneProcess(appConfig, "rclone", "sync", appConfig + ":", appFolder.getPath());
				} else {
					
					// Otherwise, need to remove any locks then reopen
					IEngine engine = Utility.getEngine(appId, false);
					try {
						engine.closeDB();
						runRcloneProcess(appConfig, "rclone", "sync", appConfig + ":", appFolder.getPath());
					} finally {
						DIHelper.getInstance().removeLocalProperty(appId);
						Utility.getEngine(appId, false);
					}
				}
			} finally {
				deleteRcloneConfig(appConfig);
			}

			System.out.println("Done pulling from remote=" + appId + " to target=" + appFolder.getPath());
	
			// Now pull the smss
			System.out.println("Pulling from remote=" + smssContainer + " to target=" + dbFolder);
			
			// THIS MUST BE COPY AND NOT SYNC TO AVOID DELETING EVERYTHING IN THE DB FOLDER
			runRcloneProcess(smssConfig, "rclone", "copy", smssConfig + ":", dbFolder);
			System.out.println("Done pulling from remote=" + smssContainer + " to target=" + dbFolder);
			
			// Catalog the db if it is new
			if (newEngine) {
				SMSSWebWatcher.catalogDB(smss, dbFolder);
			}
		} finally {
			deleteRcloneConfig(smssConfig);
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Update///////////////////////////////////////////
	
	// TODO >>>timb: pixel to update app so that neel can add refresh button or something
	// TODO >>>timb: still need to test this method
	public void updateApp(String appId) throws IOException, InterruptedException {
		if (Utility.getEngine(appId, true) == null) {
			throw new IllegalArgumentException("App needs to be defined in order to update...");
		}
		pullApp(appId, false);
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Delete //////////////////////////////////////////
	
	// TODO >>>timb: test out delete functionality
	public void deleteApp(String appId) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			System.out.println("Deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + appId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + appId + SMSS_POSTFIX);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + appId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + appId + SMSS_POSTFIX);
			System.out.println("Done deleting container=" + appId + ", " + appId + SMSS_POSTFIX);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////// Cleanup//////////////////////////////////////////	
	public List<String> listAllBlobContainers() throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		List<String> allContainers = new ArrayList<>();
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			List<String> results = runRcloneProcess(rcloneConfig, "rclone", "lsf", rcloneConfig + ":");
			for (String result : results) {
				allContainers.add(result);
			}
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
		return allContainers;
	}
	
	public void deleteContainer(String containerId) throws IOException, InterruptedException {
		String rcloneConfig = Utility.getRandomString(10);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "account", name, "key", key);
			System.out.println("Deleting container=" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "delete", rcloneConfig + ":" + containerId);
			runRcloneProcess(rcloneConfig, "rclone", "rmdir", rcloneConfig + ":" + containerId);
			System.out.println("Done deleting container=" + containerId);
		} finally {
			deleteRcloneConfig(rcloneConfig);
		}
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////// Static Util Methods ////////////////////////////////////
	
	private static String createRcloneConfig(String container) throws IOException, InterruptedException {
		System.out.println("Generating SAS for container=" + container);
		String sasUrl = client.getSAS(container);
		String rcloneConfig = Utility.getRandomString(10);
		runRcloneProcess(rcloneConfig, "rclone", "config", "create", rcloneConfig, PROVIDER, "sas_url", sasUrl);
		return rcloneConfig;
	}
	
	private static void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "delete", rcloneConfig);
		} finally {
			new File(configPath).delete();
		}
	}
	
	private static List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);	
	}
	
	private static String getConfigPath(String rcloneConfig) {
		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
	}
	
	private static List<String> runAnyProcess(String... command) throws IOException, InterruptedException {
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
