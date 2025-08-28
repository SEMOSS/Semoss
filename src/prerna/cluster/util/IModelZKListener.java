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
