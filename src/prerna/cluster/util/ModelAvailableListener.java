package prerna.cluster.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

public class ModelAvailableListener implements IModelZKListener {

	String path = null;
	public static final String AVAILABLE = "Available";
	ModelZKServer server = null;
	
	public ModelAvailableListener(String path, ModelZKServer server)
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
		if(status.equalsIgnoreCase("Available"))
		{
			System.err.println("We are done here.. ");
			String endpointNode = path.replace("status", "endpoint");
			String endpoint = server.getNodeData(endpointNode);
			
			System.err.println("Endpoint " + endpoint);
		}
		// need to account if this is a fail
		if(status.equalsIgnoreCase("Fail"))
		{
			String endpointNode = path.replace("status", "endpoint");
			System.err.println("Could not satisfy request");
		}
	}
}
