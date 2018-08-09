package prerna.cluster.util;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Location;

import com.microsoft.azure.AzureEnvironment;
import com.microsoft.azure.PagedList;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.Azure;
import com.microsoft.azure.management.compute.VirtualMachine;
import com.microsoft.azure.management.network.Network;
import com.microsoft.azure.management.resources.Feature;
import com.microsoft.azure.management.resources.ResourceGroup;
// password - ***REMOVED***
// user - ***REMOVED***


// ***REMOVED*** - app id
// d3f1e9fd-803a-4306-9292-e9000a6833a3 - obj id
// fkey - wWsZMbYkD/KrAN8XWhlzBy8VfJczCroGDCglETywW6k=
// tenant id - 90bdf71d-5ae7-4b3d-bd9c-c656d6fb6e43
// subscription id - 835449bb-9175-4058-8e6e-f2b8a2dc9398



public class AzureJCloud {
	
	public static void main(String [] args)
	{
		String acName = "sparksemossstorage";
		String acKey = "W+jqJE2O+Kq9qt9zuF9/HI3kdlKNsG3p4dISZQEnH0ZugLwOKJ/HiEVciTr9iuPwnkhXf2oFmQ0ZL372Fj4LYg==";
		// Get a context with amazon that offers the portable BlobStore api
		BlobStoreContext context = ContextBuilder.newBuilder("azureblob")
		                 .credentials(acName, acKey)
		                 .buildView(BlobStoreContext.class);

		// Access the BlobStore
		org.jclouds.blobstore.BlobStore blobStore = context.getBlobStore();

		Iterator<? extends StorageMetadata> smd = blobStore.list().iterator();
		
		Location loc = null;
		for(int smdIndex = 0;smd.hasNext();smdIndex++)
		{
			StorageMetadata md = smd.next();
			System.out.println("Blob.. " + md.getName());
			System.out.println("Blob.. " + md.getLocation());
			loc = md.getLocation();
		}
		
		
		// Create a Container
		//blobStore.
		//blobStore.createContainerInLocation(loc, "sample2");

		smd = blobStore.list().iterator();
		
		for(int smdIndex = 0;smd.hasNext();smdIndex++)
			System.out.println("Blob.. " + smd.next().getName());
		
		
		
		
		Blob blob = context.getBlobStore().blobBuilder("Sample_1")
			    .payload("Hello World")  // or InputStream
			    //.contentLength(payload.size())
			    .build();
		
		blobStore.putBlob("sample", blob);

		context.close();
		/*
		blobStore.createContainerInLocation(null, containerName);

		// Create a blob. 
		ByteSource payload = Files.asByteSource(new File(blobFullyQualifiedFileName));
		Blob blob = context.getBlobStore().blobBuilder(blobName)
		    .payload(payload)  // or InputStream
		    .contentLength(payload.size())
		    .build();

		// Upload the Blob
		blobStore.putBlob(containerName, blob);

		// When you need access to azureblob-specific features, use the provider-specific context
		AzureBlobClient azureBlobClient = context.unwrapApi(AzureBlobClient.class);
		Object object = azureBlobClient.getBlobProperties(containerName, blobName);

		System.out.println("Object: " + object);
		context.close();
		*/
	}
	
	public static void main3(String [] args)
	{
		try {
			String subscription="835449bb-9175-4058-8e6e-f2b8a2dc9398";
			String client="***REMOVED***";
			String key="wWsZMbYkD/KrAN8XWhlzBy8VfJczCroGDCglETywW6k=";
			String tenant="90bdf71d-5ae7-4b3d-bd9c-c656d6fb6e43";


			// cortex
			
//			String subscription="8ed5cf1f-c45d-49b4-b179-9ad729631aa9";
//			String client="***REMOVED***";
//			String key="wWsZMbYkD/KrAN8XWhlzBy8VfJczCroGDCglETywW6k=";
//			String tenant="90bdf71d-5ae7-4b3d-bd9c-c656d6fb6e43";
			
			
			ApplicationTokenCredentials credentials = new ApplicationTokenCredentials(
			        client, tenant, key, AzureEnvironment.AZURE);
			Azure azure = Azure.authenticate(credentials).withDefaultSubscription();
			
			final File credFile = new File("C:\\users\\pkapaleeswaran\\workspacej3\\azureauth.prop");
		    Azure azure2 = Azure.configure()
		        //.withLogLevel(LogLevel.BASIC)
		        .authenticate(credFile)
		        .withDefaultSubscription();
		    
		    azure = azure2;
		    // list hte features
		    PagedList <Feature> features = azure.features().list();
		    
		    for(int fIndex = 0;fIndex < features.size();fIndex++)
		    {
		    	System.out.println(features.get(fIndex).name());
		    }
		    
		    // list the vms
		    PagedList <VirtualMachine> machines = azure.virtualMachines().list();
		    System.out.println("Listing Virtual Machines.. ");
		    
		    for(int macIndex = 0;macIndex < machines.size();macIndex++)
		    {
		    	System.out.println("VM..  " + machines.get(macIndex).name());
		    }
		    
		    PagedList <ResourceGroup> resources = azure.resourceGroups().list();
		    System.out.println("Listing Resource Groups.. ");
		    
		    for(int macIndex = 0;macIndex < resources.size();macIndex++)
		    {
		    	System.out.println("Resource Group..  " + resources.get(macIndex).name());
		    }

		    PagedList <Network> networks = azure.networks().list();
		    System.out.println("Listing Networks.. ");
		    
		    for(int macIndex = 0;macIndex < networks.size();macIndex++)
		    {
		    	System.out.println("network..  " + networks.get(macIndex).name());
		    }

		    System.out.println("Creating virtual network...");
		    /*Network network = azure.networks()
		        .define("myVN")
		        .withRegion(Region.US_EAST)
		        .withExistingResourceGroup("myResourceGroup")
		        .withAddressSpace("10.0.0.0/16")
		        .withSubnet("mySubnet","10.0.0.0/24")
		        .create();
		    */
		    
//		    System.out.println("Creating virtual machine...");
//		    VirtualMachine virtualMachine = azure.virtualMachines()
//		        .define("myVM")
//		        .withRegion(Region.US_EAST)
//		        .withExistingResourceGroup("try1")
//		        .withExistingPrimaryNetworkInterface(sample)
//		        .withLatestWindowsImage("MicrosoftWindowsServer", "WindowsServer", "2012-R2-Datacenter")
//		        .withAdminUsername("azureuser")
//		        .withAdminPassword("Azure12345678")
//		        .withComputerName("myVM")
//		        //.withExistingAvailabilitySet(availabilitySet)
//		        .withSize("Standard_DS1")
//		        .create();
//		    Scanner input = new Scanner(System.in);
//		    System.out.println("Press enter to get information about the VM...");
//		    input.nextLine();		    
		    
		} catch (Exception e) {
		    System.out.println(e.getMessage());
		    e.printStackTrace();
		}
	}
	
	
	public static void main2(String [] args)
	{
		
		String providerId = "aws-ec2";
		String accesskeyid = "***REMOVED***";
		String secretkey = "***REMOVED***";
		String imageOwnerId = "099720109477";
		String locationId = "us-east-1d";
		String imageId = "us-east-1/ami-0022c769";
		String hardwareId = org.jclouds.ec2.domain.InstanceType.T1_MICRO;
		String securityGroupName = "jcloudecrgroup";
		String keyPairName = "jcouds";
		String groupName = "jcloudecrgroup"; // Must be lower case
		int numVMs = 1;
		 
		Properties imageOwnerIdFilter = new Properties();
		imageOwnerIdFilter.setProperty(
		    "jclouds.ec2.ami-query", "owner-id=" +
		    imageOwnerId +
		    ";state=available;image-type=machine");
		 
		List<List<String>> launchedNodesAddresses = launchInstances(providerId,
		    accesskeyid,
		    secretkey,
		    locationId,
		    imageId,
		    hardwareId,
		    securityGroupName,
		    keyPairName,
		    groupName,
		    numVMs,
		    imageOwnerIdFilter);
		 
		System.out.println(launchedNodesAddresses);
		}
	
	public static List<List<String>> launchInstances(
	        String providerId,
	        String userName,
	        String password,
	        String locationId,
	        String imageId,
	        String hardwareId,
	        String securityGroupName,
	        String keyPairName,
	        String groupName,
	        int numVMs,
	        Properties imageOwnerIdFilter) {
	 
	    // Get the Compute abstraction for the provider.
	    // Override the available VM images
	    ComputeService compute = ContextBuilder.
	            newBuilder(providerId).
	            credentials(userName, password).
	            overrides(imageOwnerIdFilter).
	            buildView(ComputeServiceContext.class).getComputeService();
	 
	    // Create a template for the VM
	    Template template = compute.
	            templateBuilder().
	            locationId(locationId).
	            imageId(imageId).
	            hardwareId(hardwareId).build();
	 
	    // Specify your own security group
	    TemplateOptions options = template.getOptions();
	    options.securityGroups(securityGroupName);
	 
	    // Specify your own keypair if the current provider allows for this
	    try {
	        Method keyPairMethod = options.getClass().getMethod("keyPair", String.class);
	        keyPairMethod.invoke(options, keyPairName);
	    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
	            | InvocationTargetException e) {
	        throw new IllegalStateException("Provider: " + providerId + " does not support specifying key-pairs.", e);
	    }
	 
	    final List<List<String>> launchedNodesAddresses = new ArrayList<>();
	    try {
	        // Launch the instances...
	        Set<? extends NodeMetadata> launchedNodesMetadata = compute.createNodesInGroup(groupName, numVMs, template);
	 
	        // Collect the addresses ...
	        for (NodeMetadata nodeMetadata : launchedNodesMetadata) {
	            launchedNodesAddresses.add(new ArrayList<>(nodeMetadata.getPublicAddresses()));
	        }
	    } catch (RunNodesException e) {
	        throw new IllegalStateException("Nodes could not be created.", e);
	    }
	 
	    return launchedNodesAddresses;
	}

}
