package prerna.cluster.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;

import prerna.util.DIHelper;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
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
	public static final String ZK_SERVER = "zk";
	public static final String KEY_HOME = "khome"; // this is where the various keys are cycled
	
	static AZClient client = null;
	
	CloudBlobClient serviceClient = null;
	SharedAccessBlobPolicy sasConstraints = null;
	
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
		String connectionString = null;
		String blobURI = null;
		String sasURL = null;
		
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
		}

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


}
