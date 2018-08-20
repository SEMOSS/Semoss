package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import prerna.util.Utility;

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
	public static final String HOME = "home";
	public static final String APP_HOME = "app";
	public static final String SEMOSS_HOME = "sem";

	
	ZooKeeper zk = null;
	Map <String, String> env = null;
	public String zkServer = "192.168.99.100:2181";
	public String host = "192.168.99.100:8888";
	public String user = "generic";
	public String home = "/semoss_root";
	public String app = "/app";
	public String semossHome = "/opt/semosshome/";
	
	
	boolean connected = false;
	
	public static ZKClient zkClient = null;
	
	Map <String, List<IZKListener>> listeners = new HashMap<String, List<IZKListener>>();
	
	
	
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
			if(zkClient.connected)
			{
				return zkClient;
			}
		}
		return null;
	}
	
	public void reconnect()
	{
		zkClient.init();
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
			
			if(zkServer != null && host != null)
			{
				// open zk
				// default time is 30 min
				zk = new ZooKeeper(zkServer, timeout, this);
				
				connected = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void publishNode()
	{
		// right now I dont have everything.. 
		// but this publishes, major, minor, ip:port, cpu, memory
		
		try {
			zk.create(home +"/semoss" , getPayload().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
			e.printStackTrace();
		}
	}
	
	public void watchEvent(String path, EventType eventType, IZKListener listener)
	{	
		String key = path + "_" + eventType;
		List <IZKListener> llist = new Vector<IZKListener>();
		if(listeners.containsKey(key))
			llist = listeners.get(key);
		
		llist.add(listener);
		listeners.put(path + "_" + eventType, llist);
		watchPath(path);
	}
	
	public void watchPath(String path)
	{
		// reset the watch
		try {
			zk.getChildren(path , true, null);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	@Override
	public void process(WatchedEvent event) {	
		// TODO Auto-generated method stub
		// System.out.println("Ok.. something came back on this" + arg0);
		EventType type = event.getType();
		
		
		
		System.out.println("Some Event came in.. " + event.getType());
		
		String path = event.getPath();
		
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
			
			watchPath(path);
		}
		
	}
	
	
	
	
}
