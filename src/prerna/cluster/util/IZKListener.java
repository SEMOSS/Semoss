package prerna.cluster.util;

import org.apache.zookeeper.ZooKeeper;

public interface IZKListener {

	public void process(String path, ZooKeeper zk);
}
