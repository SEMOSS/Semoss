package prerna.cluster.util;

import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

// counts the number of fails
// and says if the overall status failed

public class ModelInitListener implements IModelZKListener {

	String path = null;
	ModelZKServer server = null;
	
	public ModelInitListener(String path, ModelZKServer server)
	{
		this.server = server;
		this.path = path;
	}
	
	@Override
	public void setModelZK(ModelZKServer server) {
		// TODO Auto-generated method stub
		this.server = server;
	}

	@Override
	public List<EventType> getEvents() {
		// TODO Auto-generated method stub
		List <EventType> retList = new ArrayList<EventType>();
		retList.add(EventType.NodeDataChanged);
		return retList;
	}

	@Override
	public String getPath() {
		// TODO Auto-generated method stub
		return this.path;
	}

	@Override
	public List<String> getPredicates() {
		// TODO Auto-generated method stub
		List <String> predicates = new ArrayList<String>();
		predicates.add("equals");
		return predicates;
	}

	@Override
	public void process(String path, ZooKeeper zk) {
		// TODO Auto-generated method stub
		String status = server.getNodeData(path);
		String model = path.replace("/status", "");
		
		String smss = server.getNodeData(model);
		
		if(status.equalsIgnoreCase("INIT") && !server.catchup)
			server.spinModel(smss);
		
	}
}
