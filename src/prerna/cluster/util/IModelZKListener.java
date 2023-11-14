package prerna.cluster.util;

import java.util.List;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public interface IModelZKListener {

	// needs the zookeeper ?
	// need the model zk server this is where it puts everything
	// needs the list of events as an array or a list
	// needs path
	// needs the predicate - starts with, ends with, contains
	
	// finally a process
	public void setModelZK(ModelZKServer server);
	
	public List<EventType> getEvents();
	
	public String getPath();
	
	public List<String> getPredicates();
	
	public void process(String path, ZooKeeper zk);
}
