package prerna.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public final class ZKCuratorUtility {

	private CuratorFramework curator; 
	
	/**
	 * 
	 * @param curator
	 */
	ZKCuratorUtility(CuratorFramework curator) {
		this.curator = curator;
	}
	
	/**
	 * 
	 * @param pathToCheck
	 * @throws Exception
	 */
	public void createPathIfNotExists(String pathToCheck) throws Exception {
        if (this.curator.checkExists().forPath(pathToCheck) == null) {
        	this.curator.create().creatingParentsIfNeeded().forPath(pathToCheck, new byte[0]);
        }
	}
	
	/**
	 * 
	 * @param prefix
	 * @return
	 * @throws Exception
	 */
	public String createSequentialNode(String prefix) throws Exception {
        return this.curator.create()
        	.creatingParentsIfNeeded()
        	.withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        	.forPath(prefix, new byte[0]);
	}
	
	/**
	 * 
	 * @param lockName
	 * @return
	 */
	public InterProcessMutex getLock(String lockName) {
		return new InterProcessMutex(this.curator, lockName);
	}

	/**
	 * 
	 * @param parentZNode
	 * @return
	 * @throws Exception
	 */
	public String getLatestZNode(String parentZNode) throws Exception {
		// if the parent node doesn't exist
		// then return null
        if(this.curator.checkExists().forPath(parentZNode) == null) {
        	return null;
        }

        
        // Get the list of znodes under the parent path
        Stat latestStat = null;
        String latestZNode = null;

        for (String child : this.curator.getChildren().forPath(parentZNode)) {
            String childPath = parentZNode + "/" + child;
            Stat childStat = new Stat();
            curator.getData().storingStatIn(childStat).forPath(childPath);

            if (latestStat == null || childStat.getCtime() > latestStat.getCtime()) {
                latestStat = childStat;
                latestZNode = childPath;
            }
        }
        
        return latestZNode;
	}
	
}
