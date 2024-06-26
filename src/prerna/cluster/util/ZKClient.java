package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZKClient implements Watcher{
	
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

	
	public ZooKeeper zk = null;
	Map <String, String> env = null;
	public String zkServer = "localhost:2181";
	public String host = "localhost:8888";
	public String user = "generic";
	public String home = "/semoss_root";
	public String container = "/container";
	public String app = "/app";
	public static String semossHome = "/opt/semosshome/";
	
	
	boolean connected = false;
	
	public static ZKClient zkClient = null;
	protected static final Logger classLogger = LogManager.getLogger(ZKClient.class);

	Map <String, List<IZKListener>> listeners = new HashMap<String, List<IZKListener>>();
	Map <String, Boolean> repeat = new HashMap<String, Boolean>();
	
	CuratorFramework client = null;
	
	int version = 0;

	protected ZKClient()
	{
		
	}
	
	public static ZKClient getInstance()
	{
		if(zkClient == null)
		{
			zkClient = new ZKClient();
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
			client =  CuratorFrameworkFactory.newClient(zkServer, new RetryNTimes(3, 10));
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
			// pretty sure this can be skipped
			if(zkServer != null && host != null)
			{
				// open zk
				// default time is 30 min
				zk = new ZooKeeper(zkServer, timeout, this);
				
				connected = true;
			}
//			if(ClusterUtil.IS_CLUSTERED_SCHEDULER) {
//				SchedulerListener.getListener();
//			}
//			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void publishNode()
	{
		// right now I dont have everything.. 
		// but this publishes, major, minor, ip:port, cpu, memory
		
		try {
			zk.create(home +"/semoss" , getPayload().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			touchRoot();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		// also need to publish to the user
		// usally this is home/user
	}
	

	public void publishContainer(String ipPort) {
		try {
			zk.create(home + container + "/" + ipPort ,  host.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			touchRoot();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void deleteContainer(String ipPort) {
		try {
			zk.delete(home + container + "/" + ipPort, -1);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void deleteDB(String engineID)
	{
		// right now I dont have everything.. 
		// but this publishes, major, minor, ip:port, cpu, memory
		
		try {
			zk.delete(home + app + "/" + engineID, -1);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		// also need to publish to the user
		// usally this is home/user
	}
	
	// I need another where I say publish database
	public void publishDB(String engineID)
	{
		// so assume I tell this as /app/<dbname> - does that do the trick ?
		// so when I want to load a github I say /user@github.com/app/<dbname>
		try {
			zk.create(home + app + "/" + engineID ,  host.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			// TODO >>>timb: need to not create if exists
			System.out.println("Node already exists for " + engineID);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	// TODO >>>timb:ought to do a true zk watch on this (event comes in, then process in some way)
	public String getHostForDB(String engineID) throws KeeperException, InterruptedException {
		List<String> apps = zk.getChildren(home + app, false);
		String host = null;
		for (String app : apps) {
			String[] appAndHost = app.split("@");
			String appId = appAndHost[0];
			
			// Since the insights RDBMS engineId = appId_InsightsRDBMS
			if (engineID.startsWith(appId)) {
				host = appAndHost[1];
				break;
			}
		}
		return host;
	}
	
	public String getPayload()
	{
		// this will make all the major minor versions etc. 
		StringBuilder sb = new StringBuilder();
		sb.append("cpu=").append(Runtime.getRuntime().availableProcessors()).append("|");
		sb.append("memory=").append(Runtime.getRuntime().maxMemory()).append("|");
		sb.append("rver=").append("3.5").append("|");
		sb.append("semoss=").append("3.5").append("|"); // woo hoo.. same version as R
		sb.append("url=").append(host).append("|");
		sb.append("user").append(user).append("|");
		
		return sb.toString();
	}
	
	
	public void getVersion(String path)
	{
		try {
			version = zk.exists(path,true).getVersion();
			System.out.println("Running current version at " + version);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void watchEvent(String path, EventType eventType, IZKListener listener)
	{	
		watchEvent(path, eventType, listener, true);
	}

	public void watchEvent(String path, EventType eventType, IZKListener listener, boolean watchAgain)
	{
		String key = path + "_" + eventType;
		List <IZKListener> llist = new Vector<IZKListener>();
		if(listeners.containsKey(key))
			llist = listeners.get(key);
		
		llist.add(listener);
		listeners.put(path + "_" + eventType, llist);
		repeat.put(path + "_" + eventType, watchAgain);

		if(eventType == EventType.NodeChildrenChanged)
			watchPath(path);
		
		else if(eventType == EventType.NodeDataChanged || eventType == EventType.NodeDeleted)
			watchPathD(path);

	}
	
	public void removeWatch(String path, EventType eventType)
	{
		
	}
	
	public void watchPath(String path)
	{
		// reset the watch
		try {
			zk.getChildren(path , true, null);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}

	public void watchPathD(String path)
	{
		// reset the watch
		try {
			zk.getData(path , true, null);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}		
	}
	
	public void touchRoot()
	{
		try
		{
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
			   LocalDateTime now = LocalDateTime.now();  
			   System.out.println(dtf.format(now));  
			zk.setData(home, dtf.format(now).getBytes(), -1);
		}catch(Exception ex)
		{
			
		}
	}

	@Override
	public void process(WatchedEvent event) {	
		// TODO Auto-generated method stub
		// System.out.println("Ok.. something came back on this" + arg0);
		EventType type = event.getType();
		
		
		String path = event.getPath();

		System.out.println("Some Event came in.. " + event.getType() + " at path " + path);
		
		
		if(path != null)
		{
		
			String key = path + "_" + event.getType();
			
			if(listeners.containsKey(key))
			{
				List <IZKListener> llist = listeners.get(key);
				
				for(int listIndex = 0;listIndex < llist.size();listIndex++)
				{
					IZKListener thisListener = llist.get(listIndex);
					thisListener.process(path, zk);
				}
				
			}
			if(repeat.containsKey(key) && repeat.get(key))
			{
				if(event.getType() == EventType.NodeChildrenChanged)
					watchPath(path);
				
				else if(event.getType() == EventType.NodeDataChanged || event.getType() == EventType.NodeDeleted)
					watchPathD(path);
			}			
/*			if(repeat.containsKey(key) && repeat.get(key))
			else
			{
				listeners.remove(key);
			}
*/		}
		
	}
	
	public static String getNodeData(String path, ZooKeeper zk)
	{
		String data = null;
		
		try {
			byte [] b = zk.getData(path, true, new Stat());
			data = new String(b, "UTF-8");
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
		
		return data;
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
	
	public boolean watchSchedulerNode(String node, boolean watch) {
		
		boolean watched = false;
		try {
			final Stat nodeStat =  zk.exists(node, watch);
			
			if(nodeStat != null) {
				watched = true;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return watched;
	}
	
	public String createSchedulerNode( String node,  boolean watch, boolean ephimeral) {
		String createdNodePath = null;
		try {
			
			final Stat nodeStat =  zk.exists(node, watch);
			
			if(nodeStat == null) {
				createdNodePath = zk.create(node, new byte[0], Ids.OPEN_ACL_UNSAFE, (ephimeral ?  CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
			} else {
				createdNodePath = node;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return createdNodePath;
	}
	
	
	// new methods
	// create the nodes for 
	// each db
	// and within each db, load for each insight
	public String createIfNotExist(String namespace, String newNode, String version, Watcher watcher)
	{
		try {
			Stat stat = zk.exists(namespace + "/" + newNode, watcher);
			
			if(stat == null)
			{
				// create the node
				zk.create(namespace + "/" + newNode, version.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				
				// also create the lock
				// this is the node that is used for locking
				zk.create(namespace + "/" + newNode + "/lock", version.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			else
			{
				version = new String(zk.getData(namespace + "/" + newNode, watcher, stat));
				// somebody already created the lock node
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return version;
	}
	
	// new methods
	// create the nodes for 
	// each db
	// and within each db, load for each insight
	public void createIfNotExist(String namespace, String newNode, String data)
	{
		System.err.println("Registering.. " + namespace + "/" + newNode);
		try {
			Stat stat = zk.exists(namespace + "/" + newNode, false);
			
			if(stat == null)
			{
				// create the node
				zk.create(namespace + "/" + newNode, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				
			}
			else
			{
				zk.getData(namespace + "/" + newNode, false, stat);
				// somebody already created the lock node
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	
	// updates the node to say there is new data now
	private void updateNode(String namespace, String node, String newValue)
	{
		// get the lock if youa re able to
		// then update the main node
		// release the lock
		try {
			InterProcessMultiLock lock = getLockOnPath(namespace + "/" + node + "/lock");
			lock.acquire();
			// alrite time to update
			Stat stat = new Stat();
			byte [] data = zk.getData(namespace, false, stat);
			int nextVersion = Integer.parseInt(new String(data));
			nextVersion++;
			int statVersion = stat.getVersion(); 
			// need a way to tell the watcher ignore this one
			zk.setData(namespace + "/" + node, (nextVersion + "").getBytes(), statVersion + 1);
			// release the lock
			lock.release();			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
	}
	
	public InterProcessMultiLock getLockOnPath(String path)
	{
		List <String> paths = new ArrayList<String>();
		paths.add(path);
		return new InterProcessMultiLock(client, paths);
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

}
