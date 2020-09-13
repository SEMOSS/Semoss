package prerna.cluster.util;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

import prerna.util.Utility;

public class SchedulerListener implements IZKListener { 

	private static SchedulerListener schedulerListener = null;
	public static final String LEADER_ELECTION_ROOT_NODE = "/election";
	private static final String PROCESS_NODE_PREFIX = "/p_";

	private static final Logger logger = LogManager.getLogger(SchedulerListener.class);

	private String watchedNodePath;
	private String processNodePath;
	private String id;
	
	public static boolean schedulerLeader = false;


	public static SchedulerListener getListener()
	{
		if(schedulerListener == null) {
			schedulerListener = new SchedulerListener();
		schedulerListener.init();
		}
		return schedulerListener;
	}



	private void init() {
		logger.info("ZK: Registering Schleduler node");
		id = Utility.getRandomString(8);
		logger.info("Process with id: " + id + " has started!");
		String rootNodePath = ZKClient.getInstance().createSchedulerNode(LEADER_ELECTION_ROOT_NODE, false, false);
		if(rootNodePath == null) {
			throw new IllegalStateException("Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		processNodePath = ZKClient.getInstance().createSchedulerNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
		if(processNodePath == null) {
			throw new IllegalStateException("Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		logger.info("[Process: " + id + "] Process node created with path: " + processNodePath);
		attemptForLeaderPosition();

	}



	@Override
	public void process(String path, ZooKeeper zk) {
		logger.info("[Process event at path: " + path);			
		if(path.equalsIgnoreCase(watchedNodePath)) {
			attemptForLeaderPosition();
		}


	}
	
	public boolean isZKLeader() {
		SchedulerListener.getListener();
		return 	SchedulerListener.schedulerLeader;
	}

	private void attemptForLeaderPosition() {

		final List<String> childNodePaths = ZKClient.getInstance().getChildren(LEADER_ELECTION_ROOT_NODE, false);

		Collections.sort(childNodePaths);

		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
		if(index == 0) {
			logger.info("[Process: " + id  + "] I am the new Scheduler leader!");
			schedulerLeader=true;

		} else {
			final String watchedNodeShortPath = childNodePaths.get(index - 1);

			watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;

			logger.info("[Process: " + id + "] - Setting watch on node with path: " + watchedNodePath);

			ZKClient.getInstance().watchEvent(watchedNodePath, EventType.NodeDeleted, getListener(), false);

			//ZKClient.getInstance().watchSchedulerNode(watchedNodePath, true);
		}
	}


}