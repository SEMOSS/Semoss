/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

	private static final Logger classLogger = LogManager.getLogger(SchedulerListener.class);

	private String watchedNodePath;
	private String processNodePath;
	private String id;

	public static boolean schedulerLeader = false;

	public static SchedulerListener getListener() {
		if (schedulerListener == null) {
			schedulerListener = new SchedulerListener();
			schedulerListener.init();
		}
		return schedulerListener;
	}

	private void init() {
		classLogger.info("ZK: Registering Schleduler node");
		id = Utility.getRandomString(8);
		classLogger.info("Process with id: " + id + " has started!");
		String rootNodePath = ZKClient.getInstance().createSchedulerNode(LEADER_ELECTION_ROOT_NODE, false, false);
		if (rootNodePath == null) {
			throw new IllegalStateException(
					"Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		processNodePath = ZKClient.getInstance().createSchedulerNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
		if (processNodePath == null) {
			throw new IllegalStateException(
					"Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		classLogger.info("[Process: " + id + "] Process node created with path: " + processNodePath);
		attemptForLeaderPosition();

	}

	@Override
	public void process(String path, ZooKeeper zk) {
		classLogger.info("[Process event at path: " + path);
		if (path.equalsIgnoreCase(watchedNodePath)) {
			attemptForLeaderPosition();
		}

	}

	public boolean isZKLeader() {
		SchedulerListener.getListener();
		return SchedulerListener.schedulerLeader;
	}

	private void attemptForLeaderPosition() {

		final List<String> childNodePaths = ZKClient.getInstance().getChildren(LEADER_ELECTION_ROOT_NODE, false);

		Collections.sort(childNodePaths);

		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
		if (index == 0) {
			classLogger.info("[Process: " + id + "] I am the new Scheduler leader!");
			schedulerLeader = true;

		} else {
			final String watchedNodeShortPath = childNodePaths.get(index - 1);

			watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;

			classLogger.info("[Process: " + id + "] - Setting watch on node with path: " + watchedNodePath);

			ZKClient.getInstance().watchEvent(watchedNodePath, EventType.NodeDeleted, getListener(), false);

			// ZKClient.getInstance().watchSchedulerNode(watchedNodePath, true);
		}
	}

}