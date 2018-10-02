package prerna.cluster.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;

import org.apache.zookeeper.Watcher.Event.EventType;

import prerna.util.DIHelper;

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
	
	protected AZClient()
	{
		
	}
	
	// create an instance
	public static AZClient getInstance()
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
		
		Map <String, String> env = System.getenv();
		if(env.containsKey(KEY_HOME))
			azKeyRoot = env.get(KEY_HOME);

		if(env.containsKey(KEY_HOME.toUpperCase()))
			azKeyRoot = env.get(KEY_HOME.toUpperCase());

		
		if(storage == null || storage.equalsIgnoreCase("LOCAL"))
		{
			// dont bother with anything
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

	public static void main(String[] args) {
		DIHelper.getInstance().loadCoreProp("C:\\Users\\tbanach\\Documents\\Workspace\\Semoss\\RDF_Map.prop");
		AZClient client = new AZClient();
		client.init();
		System.out.println(client.getSAS("timb"));
	}

}
