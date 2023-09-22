//package prerna.cluster.util;
//
//import org.apache.zookeeper.ZooKeeper;
//
//import prerna.cluster.util.clients.AZClient;
//import prerna.cluster.util.clients.AbstractCloudClient;
//
//public class AZStorageListener implements IZKListener {
//
//	// main listener class to listen on key changes
//	// keys can change and these are SAS key changes
//	// or regular key changes
//	
//	@Override
//	public void process(String path, ZooKeeper zk) {
//		// TODO Auto-generated method stub
//		// pull the data from zk
//		// and reset on the AZClient
//		
//		String data = ZKClient.getNodeData(path, zk);
//		((AZClient) AbstractCloudClient.getClient()).swapKey(data);
//	}
//	
//}
