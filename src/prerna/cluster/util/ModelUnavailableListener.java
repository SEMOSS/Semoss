/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.cluster.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

// happens when the endpoint disappears
// it will update the status back to INIT

public class ModelUnavailableListener implements IModelZKListener {

	String path = null;
	ModelZKServer server = null;

	public ModelUnavailableListener(String path, ModelZKServer server) {
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
		List<EventType> retList = new ArrayList<EventType>();
		retList.add(EventType.NodeDeleted);
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
		List<String> predicates = new ArrayList<String>();
		predicates.add("equals");
		return predicates;
	}

	@Override
	public void process(String path, ZooKeeper zk) {
		// TODO Auto-generated method stub
		String endpointNode = path.replace("endpoint", "status");
		server.updateNodeData(endpointNode, "INIT", true);
	}
}
