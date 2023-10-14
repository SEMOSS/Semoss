package prerna.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

public final class ZKCuratorUtility {

	private CuratorFramework curator; 
	
	ZKCuratorUtility(CuratorFramework curator) {
		this.curator = curator;
	}
	
	public void createPathIfNotExists(String pathToCheck) throws Exception {
        if (curator.checkExists().forPath(pathToCheck) == null) {
            curator.create().creatingParentsIfNeeded().forPath(pathToCheck, new byte[0]);
        }
	}
	
	public String createSequentialNode(String prefix) throws Exception {
        return curator.create()
        	.creatingParentsIfNeeded()
        	.withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        	.forPath(prefix, new byte[0]);
	}
}
