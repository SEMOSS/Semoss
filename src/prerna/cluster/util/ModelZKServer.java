package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryOneTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import prerna.ds.py.TCPPyTranslator;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.PythonUtils;

public class ModelZKServer implements Watcher, CuratorCacheListener
{
	
	/*
	// Environment Variables this depnds on
	 * zk - semicolon separated list of zk to use for registration
	 * home - what is the main registration root - default is assumed as /semoss_root
	 * app - The root for various apps - default is assumed as /app
	 * host -  ip and port - this is useful if you are running multiple containers on the same box
	 * bu - the boot user who is being loaded on this box - defaults to generic
	 * DNS container address
	
	// registers to semoss_root/semoss- as a ephemeral_sequential
	// the semoss_root is an environment variable - this will allow me to spin as many cluster as I want
	 * Major version
	 * minor version
	 * semoss_sequential node
	 * host ip and port
	 * number of cpus
	 * memory
	 * User who is booted on it
	 * The registration is typically 
	 * register_root / #cpu / #memory / semoss-node
	 * register_root / semoss-node
	 * nginx will only watch the resigter_root/semoss_node initially
	 *  I could even do this for specific databases so the same thing is not loaded multiple times
	// zk e
	
	*/

	public static final String ZK_SERVER = "zk";
	public static final String HOST = "host";
	public static final String TIMEOUT = "to";
	public static final String BOOTUSER = "bu";
	public static final String HOME = "zk_home";
	public static final String APP_HOME = "app";
	public static final String SEMOSS_HOME = "sem";
	
	public static final String AVAILABLE = "available";
	public static final String MODEL_ROOT = "/model";
	public static final String SERVER_PATH = "/server";
	String workingDirectoryBasePath = null;
	NativePySocketClient socketClient = null;
	TCPPyTranslator pyt = null;
	Process process = null;
	String workingDirectory = null;
	String prefix = null;
	File cacheFolder = null;
	private static final Logger classLogger = LogManager.getLogger(ModelZKServer.class);
	Properties prop = null;
	Gson gson = new Gson();
	Map modelLock = new HashMap();
	Map modelClient = new HashMap();
	List <String> existingModels = new ArrayList();
	List <String> supportedModels = new ArrayList();
	Map <String, String> modelSMSS= new HashMap();
	String id = "RANDOM_ID";
	boolean catchup = true;

	
	
	public ZooKeeper zk = null;
	Map <String, String> env = null;
	public String zkServer = "localhost:2181";
	public String host = "localhost:8888";
	public String user = "generic";
	public String home = "/semoss_root";
	public String container = "/container";
	public String app = "/app";
	public static String semossHome = "/opt/semosshome/";
	boolean init = false;
	Map <String, List<IModelZKListener>> listeners = new HashMap<String, List<IModelZKListener>>();

	
	
	boolean connected = false;
	
	public static ModelZKServer zkClient = null;
	
	Map <String, Boolean> repeat = new HashMap<String, Boolean>();
	
	CuratorFramework client = null;
	
	int version = 0;

	protected ModelZKServer()
	{
		
	}
	
	public static ModelZKServer getInstance(String id)
	{
		if(zkClient == null)
		{
			zkClient = new ModelZKServer();
			zkClient.id = id;
			zkClient.init();
			zkClient.initCurator();
		}
		if(zkClient.connected)
		{
			return zkClient;
		}
		return null;
	}
	
	public void reconnect()
	{
		zkClient.init();
	}
	
	public void initCurator()
	{
		// make the curator here also
		try {
			client =  CuratorFrameworkFactory.newClient(zkServer, new RetryOneTime(1));
			client.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}

	}
	
	public void waitHere()
	{
		try {
			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
			
		}catch(Exception ignored)
		{
			
		}
	}
	
	public void init()
	{
		// initiates connection to zk and makes the connection
		try {
			env = System.getenv();
			if(env.containsKey(ZK_SERVER))
				zkServer = env.get(ZK_SERVER);

			if(env.containsKey(ZK_SERVER.toUpperCase()))
				zkServer = env.get(ZK_SERVER.toUpperCase());

			if(env.containsKey(HOST))
				host = env.get(HOST);

			if(env.containsKey(HOST.toUpperCase()))
				host = env.get(HOST.toUpperCase());

			int timeout = (30 * 60 * 1000);

			if(env.containsKey(TIMEOUT))
				host = env.get(TIMEOUT);

			if(env.containsKey(TIMEOUT.toUpperCase()))
				host = env.get(TIMEOUT.toUpperCase());

			if(env.containsKey(BOOTUSER))
				user = env.get(BOOTUSER);

			if(env.containsKey(BOOTUSER.toUpperCase()))
				user = env.get(BOOTUSER.toUpperCase());

			
			if(env.containsKey(HOME))
				home = env.get(HOME);

			
			if(env.containsKey(HOME.toUpperCase()))
				home = env.get(HOME.toUpperCase());

			if(env.containsKey(APP_HOME))
				app = env.get(APP_HOME);

			if(env.containsKey(APP_HOME.toUpperCase()))
				app = env.get(APP_HOME.toUpperCase());

			if(env.containsKey(SEMOSS_HOME))
				semossHome = env.get(SEMOSS_HOME);

			if(env.containsKey(SEMOSS_HOME.toUpperCase()))
				semossHome = env.get(SEMOSS_HOME.toUpperCase());
			
			// TODO >>>timb:not sure if the host is needed for both the engine and user containers
			if(zkServer != null && host != null)
			{
				// open zk
				// default time is 30 min
				zk = new ZooKeeper(zkServer, timeout, this);
				connected = true;
				initCurator();
			}
			
			// start a base python
			
			socketClient = connect2Py(null);
			pyt = new TCPPyTranslator();
			pyt.setSocketClient(socketClient);
			
			// publish this node ? - do we even need to ?
			addServer();

			// catchup
			catchup();
			
			// listen for getChildren on /model
			addCuratorListener(this, MODEL_ROOT);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}	
	
	public void addServer()
	{
		// this merely adds the server
		
		// create server if one does not exist
		// gets the lock
		// updates the server count
		try {
			Stat s = zk.exists(SERVER_PATH, false);
			
			if(s == null)
			{
				String data = "1";
				zk.create(SERVER_PATH, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			// add this as an ephemeral sequential node
			// although I dont know why I need sequential.. I could just add it
			zk.create(SERVER_PATH + "/" + id, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	

	@Override
	public void process(WatchedEvent event) {	
		// TODO Auto-generated method stub
		// System.out.println("Ok.. something came back on this" + arg0);
		processEvent(event.getPath(), event);
	}
	
	public String getNodeData(String path)
	{
		try {
			byte [] nodeBytes = zk.getData(path, true, new Stat());
			String nodeData = new String(nodeBytes, "UTF-8");
			return nodeData;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	public void updateNodeData(String path, String data, boolean create)
	{
		try {
			Stat stat = zk.exists(path, false);
			if(stat == null && create)
			{
				Stat ver = new Stat(1, 1, 0, 0, 1, 0, 0, 1, data.length(), 3, 1);
				zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, ver);
			}
			else if(stat != null)
			{
				zk.setData(path, data.getBytes(), -1);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
		
	public List<String> getChildren(String node, final boolean watch) {
		
		List<String> childNodes = null;
		
		try {
			childNodes = zk.getChildren(node, watch);
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return childNodes;
	}
	

	public void watch4Data(String path)
	{
		try 
		{
			Stat stat = new Stat();
			zk.getData(path, true, stat);
			System.out.println(stat.getVersion());
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
	}

	public void watch4Children(String path)
	{
		try 
		{
			Stat stat = new Stat();
			zk.getChildren(path, true);
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
	}
	
	public void spinModel(String propFileAsString)
	{
		// get childern for /nodes
		// for each childern - get the data
		// find which one to spin into
		// get the lock
		// spin
		// release the lock
		
		try {
			prop = new Properties();
			StringReader reader = new StringReader(propFileAsString);
			
			prop.load(reader);
			
			// get the name of the engine
			String engineName = prop.getProperty(Constants.ENGINE);
			String model = engineName; //prop.getProperty(Constants.MODEL);
			
			int numInstances = 1;
			if(prop.containsKey(Settings.COUNT))
				numInstances = Integer.parseInt(prop.getProperty(Settings.COUNT));
			
			// the pattern is
			// /model/<model>/engineName
			//String nodeParent = "/model/" + model;
			//List <String> modelInstances = zk.getChildren(nodeParent, false);
			
			String modelPath = MODEL_ROOT + "/" + model;
			
			// check to see if the number of children is satisfied
			//if(modelInstances.size() < numInstances)
			{
				// get all the stats
				String needs = prop.getProperty(Settings.REQUIREMENTS);
				Object needObject = gson.fromJson(needs, Object.class); // typically this is a dictionary
				if(needObject instanceof Map)
				{
					Map needMap = (Map)needObject;
					Map capMap = getCurrentCapabilities();
					boolean accomodate = canAccomodate(capMap, needMap);
					// take the mutex lock
					getLock(modelPath);
					
					// force a fail
					//accomodate = false;
					if(accomodate)
					{
						// check the status to see this is not something we got after someone else released it.. 
						String statusNode = MODEL_ROOT +"/" + model + "/status";
						String status = getNodeData(statusNode);
						// make sure this is not released by someone else
						if(status.equalsIgnoreCase("INIT"))
						{
							// spin the server
							// set the status
							// connect2Py i.e. spin an engine
							System.err.println("Spinning the server now.. ");
							
							// update the endpoint
							updateNodeData(MODEL_ROOT + "/" + model + "/endpoint", "abracadabra" , true);
							
							// update the status node
							zk.setData(statusNode, "AVAILABLE".getBytes(), -1);
						}
					}
					else
					{
						// report failure count
						String failNode = MODEL_ROOT +"/" + model + "/fail/" + id;
						// record this node
						updateNodeData(failNode, "FAIL", true);
					}
					releaseLock(modelPath);
				}
				
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (JsonSyntaxException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} 
	}
	
	public void getLock(String modelPath)
	{
		// create the lock
		// need a way to create node if one doesnt exist
		try {
			InterProcessMutex lock = new InterProcessMutex(client, modelPath + "/locker");
			boolean acquired = lock.acquire(3, TimeUnit.SECONDS); 
			if(acquired)
			{
				modelLock.put(modelPath, lock);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void releaseLock(String modelPath)
	{
		try {
			if(modelLock.containsKey(modelPath))
			{
				InterProcessMutex lock = (InterProcessMutex)modelLock.get(modelPath);
				lock.release();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public Map getCurrentCapabilities()
	{
		boolean initialized = (Boolean)pyt.runScript("'hardware_util' in globals()");
		
		if(!initialized)
		{
			pyt.runScript("import gaas_hardware_util as ghu");
			pyt.runScript("hardware_util = ghu.HardwareUtil()");
		}		
		String capString = ""+ pyt.runScript("hardware_util.get_all()");
		System.err.println("Cap String ..  " + capString);
		Object cap = gson.fromJson(capString, Object.class);
		if(cap instanceof Map)
		{
			Map capMap = (Map)cap;
			return capMap;
		}
		else
		{
			return new HashMap();
		}
		
	}
	
	public boolean canAccomodate(Map capMap, Map needMap)
	{
		// compares the map with each of it
		boolean accomodate = true;
		Iterator <String> keys = needMap.keySet().iterator();
		while(keys.hasNext() && accomodate)
		{
			String thisNeed = keys.next();
			String needValueStr = needMap.get(thisNeed) + "";
			long needValue = Long.parseLong(needValueStr);
			
			// this could be a another map
			Object itemMap = capMap.get(thisNeed);
			if(itemMap instanceof Map)
			{
				double capValue = Double.parseDouble(((Map)itemMap).get(AVAILABLE) + "");
				accomodate = accomodate && (needValue <= capValue);
			}
			else
				accomodate = false;
		}
		return accomodate;
	}
	
	
	public NativePySocketClient connect2Py(String modelPath)
	{
		String port = PortAllocator.getInstance().getNextAvailablePort()+"";
		String timeout = "-1";
		
		createCacheFolder();

		// TODO verify this is correct.
		String loggerLevel = this.prop.getProperty(Settings.LOGGER_LEVEL, "WARNING");
		Object [] outputs = PythonUtils.startTCPServerNativePy(this.workingDirectoryBasePath, port, null, timeout, loggerLevel);
		this.process = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		NativePySocketClient client = new NativePySocketClient();
		client.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		client = connectClient(client);
		if(modelPath != null)
			modelClient.put(modelPath, client);
		
		return client;
	}
		
	
	private void createCacheFolder() {
		// create a generic folder
		this.workingDirectory = "MODEL_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);
		
		// make the folder if one does not exist
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}
	
	public NativePySocketClient connectClient(NativePySocketClient client) 
	{
		Thread t = new Thread(client);
		t.start();
		while(!client.isReady())
		{
			synchronized(client)
			{
				try 
				{
					client.wait(2000);
					classLogger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}								
			}
		}
		return client;
	}

	public void addCuratorListener(CuratorCacheListener listener, String path)
	{
		CuratorCache tc = CuratorCache.build(client, path);
		tc.start();
		tc.listenable().addListener(listener);
	}
	
	public void catchup()
	{
		// this is useful when the server starts up
		try {
			catchup = true;
			List <String> children = zk.getChildren(MODEL_ROOT, false);
			for(int childIndex = 0;childIndex < children.size();childIndex++)
			{
				String thisModel = children.get(childIndex);
				String modelPath = MODEL_ROOT + "/" + thisModel;
				String statusNode = modelPath + "/status";
				
				String status = getNodeData(statusNode);
				if(status.equalsIgnoreCase("INIT"))
				{
					String propFileAsString = getNodeData(modelPath);
					spinModel(propFileAsString);
				}	
			}
			catchup = false;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		
	}
	
	
	@Override
	public void event(Type type, ChildData oldData, ChildData data) 
	{
		// need to see if this is 
		
		// TODO Auto-generated method stub
		if(oldData != null)
			System.err.println("OLD >> " + oldData.getPath());
		if(data != null)
			System.err.println("NEW >> " + data.getPath());
		
		// need to remove the base path to see if this is a parent level
		// or some child level
		// only react if it is a parent level
		// i.e. /model/something.. 
		
		if(data != null && data.getPath().endsWith("/status"))
		{
			String inPath = data.getPath();
			String childPath = inPath.replace(MODEL_ROOT,"");
			String modelPath = data.getPath().replace("/status", "");
			{				
				try {
					byte [] smssBytes = zk.getData(modelPath, false, new Stat());
					ModelInitListener scl = new ModelInitListener(modelPath, this);
					
					String propFileAsString = new String(smssBytes, "UTF-8");
					prop = new Properties();
					StringReader reader = new StringReader(propFileAsString);
					prop.load(reader);
					String modelId = prop.getProperty(Constants.ENGINE);
					modelSMSS.put(MODEL_ROOT + "/" + modelId, propFileAsString);
					
					// dont spin the model
					// wait for event on /status
					zk.addWatch(modelPath, AddWatchMode.PERSISTENT_RECURSIVE);
					String statusNode = MODEL_ROOT + "/" + modelId + "/status";
					String status = getNodeData(statusNode);
					if(status != null &&status.equalsIgnoreCase("INIT"))
						spinModel(propFileAsString);
					//zk.addWatch(statusNode, this, AddWatchMode.PERSISTENT);
					//spinModel(propFileAsString);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					classLogger.error(Constants.STACKTRACE, e);
				}
				
			}	
		}
	}
	
	public void addZKListener(IModelZKListener listener)
	{
		String path = listener.getPath();
		List <IModelZKListener> listenerList = new Vector<IModelZKListener>();
		if(listeners.containsKey(path))
			listenerList = listeners.get(path); 
		listenerList.add(listener);
		listeners.put(path, listenerList);
	}
	
	public void processEvent(String path, WatchedEvent event)
	{
		// get all the listeners for this path
		// check to see if the event is listed
		// if the event is listed.. check the predicate ? <-- not sure I need this given I am making all of it specific
		if(listeners.containsKey(path))
		{
			List <IModelZKListener> listenerList = listeners.get(path);
			for(int listenerIndex = 0;listenerIndex < listenerList.size();listenerIndex++)
			{
				IModelZKListener thisListener = listenerList.get(listenerIndex);
				
				List <EventType> events = thisListener.getEvents();
				if(events.contains(event.getType()))
				{
					// process this event
					thisListener.process(path, zk);
				}
				
			}
		}	
	}
	
//	public static void main(String [] args)
//	{
//		DIHelper helper = DIHelper.getInstance();
//		helper.loadCoreProp("C:/users/pkapaleeswaran/workspacej3/SemossDev/RDF_Map.prop");
//		ModelZKServer zkc = ModelZKServer.getInstance("RANDOM_ID");
//		
//		
//		InterProcessMutex lock = new InterProcessMutex(zkc.client, "/locker");
//		try {
//		     lock.acquire(); //(3, TimeUnit.SECONDS)) 
//		    {
//		        try {
//		            // do some work inside of the critical section here
//		        	System.err.println("Starting this thread ");
//		        	Thread.sleep(10000);
//		        	System.err.println("Going to release lock");
//		        } finally {
//		            lock.release();
//		        }
//		    }
//		} catch (Exception e) {
//		    throw new RuntimeException(e);
//		}
//		try {
//			Thread.sleep(10000000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		
//		//zkc.waitHere();
//		
//	}


}
