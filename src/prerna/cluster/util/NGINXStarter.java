//package prerna.cluster.util;
//
//import org.apache.zookeeper.Watcher.Event.EventType;
//
//public class NGINXStarter {
//
//	
//	//public String semossHome = "/opt/semosshome/";
//	public String semossHome = "c:/users/pkapaleeswaran/workspacej3/docker/";
//
//	boolean connected = false;
//	
//	public static ZKClient zkClient = null;
//	
//	
//	
//	int version = 0;
//
//	
//	public static NginxClient getInstance()
//	{
//		if(zkClient == null)
//		{
//			zkClient = ZKClient.getInstance();
//			
//		}
//		return null;
//	}
//	
//	public static void main(String [] args)
//	{
//		if(zkClient == null)
//			zkClient = ZKClient.getInstance();
//		NGINXDomainListener nginxListener = new NGINXDomainListener();
//		zkClient.watchEvent(zkClient.home, EventType.NodeDataChanged, nginxListener);
//		
//		// generate once for the first time
//		//nginxListener.regenConfig(zkClient.home + zkClient.app, zkClient.zk);
//		nginxListener.regenConfig(zkClient.home, zkClient.zk);
//		
//		//zkClient.watchEvent(zkClient.home + zkClient.app, EventType.NodeChildrenChanged, nginxListener);
//		
//		
//		while(true)
//		{
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//			
//		}
//	}
//}
