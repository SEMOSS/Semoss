package prerna.cluster.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import prerna.util.Constants;

public class SMSSPoster extends ModelZKServer implements Watcher, CuratorCacheListener
{
	boolean connected = false;
	String modelId = null;
	boolean lockSet = false;
	
	static SMSSPoster poster = null;
	
	protected SMSSPoster()
	{
		
	}
	
	public static SMSSPoster getInstance()
	{
		if(poster == null)
		{
			poster = new SMSSPoster();
			poster.init();
		}
		return poster;
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
				
				// set the listener for /server child
				String data = "0";
				Stat ver = new Stat(1, 1, 0, 0, 1, 0, 0, 1, data.length(), 3, 1);
				try {
					String serverData = getNodeData("/server");
					if(serverData == null)
						zk.create("/server", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, ver);
					System.err.println("setting the listener for server");
					zk.getChildren("/server", true);
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					System.err.println("Node exists.. ");
					//e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
				
			}
						
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
		
	public void addCuratorListener(CuratorCacheListener listener, String path)
	{
		CuratorCache tc = CuratorCache.build(client, path);
		tc.start();
		tc.listenable().addListener(listener);
		
	}

	public boolean checkServers()
	{
		try {
			List <String> children = zk.getChildren("/server", false);
			if(children.size() == 0)
			{
				throw new RuntimeException("No Resource Available");
			}
			return true;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	
	public void postSMSS(String smssFile)
	{
		
		try {

			//checkServers();
			
			String smssFileAsString = FileUtils.readFileToString(new File(smssFile));
			
			Properties prop = new Properties();
			prop.load(new FileInputStream(smssFile));
			
			modelId = prop.getProperty(Constants.ENGINE);
			String modelPath = MODEL_ROOT + "/" + modelId;
			String statusNode = MODEL_ROOT + "/" + modelId + "/status";
			String endpointNode = MODEL_ROOT + "/" + modelId + "/endpoint";
			String failNode = MODEL_ROOT + "/" + modelId + "/fail";
			
			zk.addWatch(modelPath, AddWatchMode.PERSISTENT_RECURSIVE);
			
			// you can add listeners even when the nodes dont exist
			ModelAvailableListener mal = new ModelAvailableListener(statusNode, this);
			addZKListener(mal);
			ModelUnavailableListener eual = new ModelUnavailableListener(endpointNode, this);
			addZKListener(eual);
			ModelFailListener fcl = new ModelFailListener(failNode, this);
			addZKListener(fcl);
			
			System.err.println(smssFileAsString);
			Stat ver = new Stat(1, 1, 0, 0, 1, 0, 0, 1, smssFileAsString.length(), 3, 1);
			zk.create(modelPath, smssFileAsString.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, ver);
			//zk.setData(MODEL_PATH + "/" + modelId, smssFileAsString.getBytes(), 1);

			// time to wait
			// I am just setting a watch
			// set the status and then wait on status watch
			
			// create status and listen
			zk.create(statusNode, "INIT".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, ver);
			zk.getData(statusNode, true, ver);
			
			// create a fail node and listen
			// create the fail node as a sequential
			zk.create(failNode, "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			//updateNodeData(failNode, "0", true);
			zk.getChildren(failNode, true);
						
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
	// old process method replaced with ModelZKServer
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		EventType type = event.getType();
		
		System.err.println("POSTER event " + event.getPath() + type);
		
		processEvent(event.getPath(), event);
		
		/*
		if(type == EventType.NodeDataChanged)
		{
			try {
				String path = event.getPath();
				// got the status
				if(path != null && path.equalsIgnoreCase(MODEL_PATH + "/" + modelId + "/status"))
				{
					// ok we got our bogey
					//Stat ver = new Stat(1, 1, 0, 0, 1, 0, 0, 1, smssFileAsString.length(), 3, 1);
					byte [] bstatus = zk.getData(MODEL_PATH + "/" + modelId + "/status", true, new Stat());
					String status = new String(bstatus, "UTF-8");
					if(status.equalsIgnoreCase("Available"))
					{
						System.err.println("We are done here.. ");
					
						String endpointNode = MODEL_PATH + "/" + modelId + "/endpoint";
	
						// listen to status changes
						//zk.addWatch(endpointNode, AddWatchMode.PERSISTENT);
	
						String endpoint = getNodeData(endpointNode);
						//System.out.println("Endpoint .. " + endpoint);
						//Stat ver = new Stat(1, 1, 0, 0, 1, 0, 0, 1, endpoint.length(), 3, 1);
						//zk.getData(endpointNode, true, ver);
					}
				}
				if(path != null && path.endsWith("fail")) // process failure
				{
					String strServers = getNodeData("/server");
					long servers = Long.parseLong(strServers);
					
					String strFails = getNodeData(path);
					long fails = Long.parseLong(strFails);
					
					if(servers == fails)
					{
						System.out.println("No Servers available to process this request");
					}
				}

			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(type == EventType.NodeDeleted)
		{
		}
		
		if(type == EventType.NodeChildrenChanged)
		{
		}
		
		// need to also see if the endpoint gets dropped because of ephemeral node		
		 */
	}
		
	
	public void deleteNode()
	{
		try {
			zk.delete(MODEL_ROOT + "/" + modelId + "/status", -1);
			zk.delete(MODEL_ROOT + "/" + modelId + "/fail", -1);
			zk.delete(MODEL_ROOT + "/" + modelId, -1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public static void main(String [] args)
	{
		SMSSPoster poster = SMSSPoster.getInstance();
		String smssFileName = "c:/users/pkapaleeswaran/workspacej3/SemossDev/model/Orca_Embedded__EMB_30991037-1e73-49f5-99d3-f28210e6b95c11.smss";
		poster.postSMSS(smssFileName);
		System.err.println("Posted node.. ");
		poster.deleteNode();
		
	}


}
