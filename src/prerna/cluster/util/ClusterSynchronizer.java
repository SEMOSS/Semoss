package prerna.cluster.util;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import prerna.cluster.util.clients.AppCloudClientProperties;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.Utility;

public class ClusterSynchronizer {

	private static ClusterSynchronizer sync = null;

	private static final Logger classLogger = LogManager.getLogger(ClusterSynchronizer.class);

	
	public static final String ZK_SERVER_STRING = "ZK_SERVER";
	public static final String HOST_IP = "HOST_IP";

	public static final String SYNC_PROJECT_PATH = "/sync/project";
	public static final String SYNC_ENGINE_PATH = "/sync/engine";

	private CuratorFramework client = null;
	private CuratorCache projectCache;
	private CuratorCache engineCache;
	
	String host;

	private ClusterSynchronizer() {
		initalizeClusterSyncronizer();
	}

	private void initalizeClusterSyncronizer() {
		classLogger.info("Starting up cluster synchronizer");
		AppCloudClientProperties clientProps = new AppCloudClientProperties();
		
		// what is the zk server ip
		String zk_server = clientProps.get(ZK_SERVER_STRING);
		
		if (zk_server == null || zk_server.isEmpty()) {
//			zk_server="localhost:2181";
			throw new IllegalArgumentException("Zookeeper Server endpoint is not defined");
			}
		
		// what is the host ip of the container/pod/box - this is used as a unique id for the container/singleton
		host = clientProps.get(HOST_IP);
		if (host == null || host.isEmpty()) {
			classLogger.info("Host IP is not set");
		   host="node_"+Utility.getRandomString(5);
		}

		
		// make the curator
		try {
			client =  CuratorFrameworkFactory.newClient(zk_server, new RetryNTimes(3, 10));
			client.start();
			
			
	        // Check if the ZNode exists before trying to create it - project
	        if (client.checkExists().forPath(SYNC_PROJECT_PATH) == null) {
	            client.create().creatingParentsIfNeeded().forPath(SYNC_PROJECT_PATH);
	        }
	        
	        // Check if the ZNode exists before trying to create it - engine
	        if (client.checkExists().forPath(SYNC_ENGINE_PATH) == null) {
	            client.create().creatingParentsIfNeeded().forPath(SYNC_ENGINE_PATH);
	        }
	        
	        projectCache= createCacheListener(SYNC_PROJECT_PATH);
	        engineCache=createCacheListener(SYNC_ENGINE_PATH);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

	private CuratorCache createCacheListener(String pathToWatch) {
	    CuratorCache cache = CuratorCache.build(client, pathToWatch);
	    CuratorCacheListener listener = CuratorCacheListener.builder()
	            .forPathChildrenCache(pathToWatch, client, new PathChildrenCacheListener() {
	                @Override
	                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
	                    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
	                        String updatedByNodeId = new String(event.getData().getData());
	                        // if the host updated it, then its already ready - other nodes have to pull
	                        if (!updatedByNodeId.equals(host)) {
	                            String fullPath = event.getData().getPath();
	                            System.out.println( fullPath + " updated, pulling latest data from cloud storage");
	                            if(fullPath.startsWith(SYNC_PROJECT_PATH)) {
	                                String[] path = fullPath.split(SYNC_PROJECT_PATH+"/");
	                                String projectID = path[1];
	                                ClusterUtil.pullProject(projectID);
	                            } else {
	                                String[] path = fullPath.split(SYNC_ENGINE_PATH+"/");
	                                String engineID = path[1];
	                                ClusterUtil.pullEngine(engineID);
	                            }
	                        }
	                    }
	                }
	            })
	            .build();

	    cache.listenable().addListener(listener);
	    cache.start();

	    return cache;
	}
	
	
	

	public static ClusterSynchronizer getInstance() throws Exception {
		if(sync != null) {
			return sync;
		}
		
		if(sync == null) {
			synchronized (ClusterSynchronizer.class) {
				if(sync != null) {
					return sync;
				}
				
				sync = new ClusterSynchronizer();
			}
		}
		
		return sync;
	}
	
	
	//TODO - break this out smarter to be for all different pushes
	public void publishEngineChange(String engineId) throws Exception {
		
		String enginePath = SYNC_ENGINE_PATH + "/" + engineId;
		
		//this creates the path if it doesnt exist
		if (client.checkExists().forPath(enginePath) == null) {
		    client.create().creatingParentsIfNeeded().forPath(enginePath);
		}
		
		// this updates the path and the watcher is watching for updates
		//TODO - pass a full map as the data where host will be a key along with the function used - ex. pushOwl, pushInsightDB
		 client.setData().forPath(enginePath, host.getBytes());
		
	}
	
	//TODO - break this out smarter to be for all different pushes
	public void publishProjectChange(String projectId) throws Exception {
		
		String projectPath = SYNC_PROJECT_PATH + "/" + projectId;
		
		//this creates the path if it doesnt exist
		if (client.checkExists().forPath(projectPath) == null) {
		    client.create().creatingParentsIfNeeded().forPath(projectPath);
		}
		
		// this updates the path and the watcher is watching for updates
		//TODO - pass a full map as the data where host will be a key along with the function used - ex. pushProjectFolder
		 client.setData().forPath(projectPath, host.getBytes());
		
	}
	
	public static void main(String[] args) {
		
		try {
			ClusterSynchronizer instance = ClusterSynchronizer.getInstance();
			
	        Thread.sleep(Integer.MAX_VALUE);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
	

