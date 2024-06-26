package prerna.cluster.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import prerna.project.api.IProject;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.Constants;
import prerna.util.DIHelper;
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
		//zk_server="localhost:2181";

		if (zk_server == null || zk_server.isEmpty()) {
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
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		

	}

	private CuratorCache createCacheListener(String pathToWatch) {
	    CuratorCache cache = CuratorCache.build(client, pathToWatch);
	    CuratorCacheListener listener = CuratorCacheListener.builder()
	            .forPathChildrenCache(pathToWatch, client, new PathChildrenCacheListener() {
	                @Override
	                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
	                    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
	                        ByteArrayInputStream byteIn = new ByteArrayInputStream(event.getData().getData());
	                        ObjectInputStream in = new ObjectInputStream(byteIn);
	                        //Map<String, String> dataMap = (Map<String, String>) in.readObject();
	                        Map<String, Object> dataMap = (Map<String, Object>) in.readObject();

	                        String updatedByNodeId = (String) dataMap.get("nodeId");
	                        // if the host updated it, then its already ready - other nodes have to pull
	                        if (!updatedByNodeId.equals(host)) {
	                            String fullPath = event.getData().getPath();
	                            classLogger.info( fullPath + " updated, pulling latest data from cloud storage");
	                            String id;
	                            boolean pull;
	                            if(fullPath.startsWith(SYNC_PROJECT_PATH)) {
	                                String[] path = fullPath.split(SYNC_PROJECT_PATH+"/");
	                                id = path[1];
	                                pull = projectLoaded(id);
	                            } else {
	                                String[] path = fullPath.split(SYNC_ENGINE_PATH+"/");
	                                 id = path[1];
		                             pull = engineLoaded(id);
	                            }
	                            
	                            // always check if the engine has been loaded before pulling. 

	                            if(pull) {
	                            try {
	                                List<String> params = (List<String>) dataMap.get("params");
	                                Class<?>[] paramTypes = new Class[params.size()];
	                                for (int i = 0; i < params.size(); i++) {
	                                    paramTypes[i] = params.get(i).getClass();
	                                }
	                                Method method = ClusterUtil.class.getMethod(dataMap.get("methodName").toString(), paramTypes);
	                                method.invoke(null, params.toArray());
	                            } catch (Exception e) {
	                            	classLogger.error(Constants.STACKTRACE, e);
	                            }
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
	
	
	private static boolean projectLoaded(String projectId) {
		String projects = DIHelper.getInstance().getProjectProperty(Constants.PROJECTS) + "";

		if (projects.startsWith(projectId) || projects.contains(";" + projectId + ";") || projects.endsWith(";" + projectId)) {
			classLogger.info("Loaded project " + projectId + " is out of date. Pulling latest changes");
			return true;
		} else {
			return false;
		}
	}
	
	private static boolean engineLoaded(String engineId) {
		String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";

		if (engines.startsWith(engineId) || engines.contains(";" + engineId + ";") || engines.endsWith(";" + engineId)) {
			classLogger.info("Loaded engine " + engineId + " is out of date. Pulling latest changes");
			return true;
		} else {
			return false;
		}
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
	
	
	public void publishEngineChange(String engineId, String methodName,  Object... params) throws Exception {
		
		String enginePath = SYNC_ENGINE_PATH + "/" + engineId;
		
		//this creates the path if it doesnt exist
		if (client.checkExists().forPath(enginePath) == null) {
		    client.create().creatingParentsIfNeeded().forPath(enginePath);
		}
		
	   classLogger.info("Publishing change for engine " + engineId + " and for nodes to " + methodName);
	   Map<String, Object> dataMap = new HashMap<>();
       dataMap.put("nodeId", host);
       dataMap.put("methodName", methodName);
       List<Object> paramList = Arrays.asList(params);
       dataMap.put("params", paramList);

       ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
       ObjectOutputStream out = new ObjectOutputStream(byteOut);
       out.writeObject(dataMap);
	
       client.setData().forPath(enginePath, byteOut.toByteArray());

	}
	
	public void publishProjectChange(String projectId, String methodName,  Object... params) throws Exception {
		
		String projectPath = SYNC_PROJECT_PATH + "/" + projectId;
		
		//this creates the path if it doesnt exist
		if (client.checkExists().forPath(projectPath) == null) {
		    client.create().creatingParentsIfNeeded().forPath(projectPath);
		}
		
	   classLogger.info("Publishing change for project " + projectId + " and for nodes to " + methodName);
	   Map<String, Object> dataMap = new HashMap<>();
       dataMap.put("nodeId", host);
       dataMap.put("methodName", methodName);
       List<Object> paramList = Arrays.asList(params);
       dataMap.put("params", paramList);

       ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
       ObjectOutputStream out = new ObjectOutputStream(byteOut);
       out.writeObject(dataMap);
	
       client.setData().forPath(projectPath, byteOut.toByteArray());

	}
	
	
//	
//	//TODO - break this out smarter to be for all different pushes
//	public void publishEngineChange(String engineId, String methodName) throws Exception {
//		
//		String enginePath = SYNC_ENGINE_PATH + "/" + engineId;
//		
//		//this creates the path if it doesnt exist
//		if (client.checkExists().forPath(enginePath) == null) {
//		    client.create().creatingParentsIfNeeded().forPath(enginePath);
//		}
//		
//	   Map<String, String> dataMap = new HashMap<>();
//       dataMap.put("nodeId", host);
//       dataMap.put("methodName", methodName);
//       
//       ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//       ObjectOutputStream out = new ObjectOutputStream(byteOut);
//       out.writeObject(dataMap);
//	
//		// this updates the path and the watcher is watching for updates
//		//TODO - pass a full map as the data where host will be a key along with the function used - ex. pushOwl, pushInsightDB
//		 //client.setData().forPath(enginePath, host.getBytes());
//       client.setData().forPath(enginePath, byteOut.toByteArray());
//
//	}
//	
//	//TODO - break this out smarter to be for all different pushes
//	public void publishProjectChange(String projectId, String methodName) throws Exception {
//		
//		String projectPath = SYNC_PROJECT_PATH + "/" + projectId;
//		
//		//this creates the path if it doesnt exist
//		if (client.checkExists().forPath(projectPath) == null) {
//		    client.create().creatingParentsIfNeeded().forPath(projectPath);
//		}
//		
//		   Map<String, String> dataMap = new HashMap<>();
//	       dataMap.put("nodeId", host);
//	       dataMap.put("methodName", methodName);
//	       
//	       ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//	       ObjectOutputStream out = new ObjectOutputStream(byteOut);
//	       out.writeObject(dataMap);
//		
//			// this updates the path and the watcher is watching for updates
//			//TODO - pass a full map as the data where host will be a key along with the function used - ex. pushOwl, pushInsightDB
//			 //client.setData().forPath(enginePath, host.getBytes());
//	       client.setData().forPath(projectPath, byteOut.toByteArray());
//		
//	}
//	
	
	
	public static void main(String[] args) {
		
		try {
			ClusterSynchronizer instance = ClusterSynchronizer.getInstance();
			
	        Thread.sleep(Integer.MAX_VALUE);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
	}


}
	

