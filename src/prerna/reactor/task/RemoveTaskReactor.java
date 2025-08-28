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
package prerna.reactor.task;

import java.util.HashMap;
import java.util.Map;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.insight.InsightUtility;

public class RemoveTaskReactor extends AbstractReactor {

	private static final String DROP_NOW_KEY = "dropNow";

	public RemoveTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK_ID.getKey(), DROP_NOW_KEY};
	}

	@Override
	public NounMetadata execute() {
		// get the task id
		NounMetadata inputNoun = this.curRow.getNoun(0);
		String taskId = null;
		if (inputNoun.getNounType() == PixelDataType.TASK) {
			taskId = ((ITask) inputNoun.getValue()).getId();
		} else {
			taskId = inputNoun.getValue().toString();
		}
		ITask task = insight.getTaskStore().getTask(taskId);
		if (task == null) {
			throw new IllegalArgumentException("Could not find task id = " + taskId);
		}

		// drop now
		if (dropNow()) {
			InsightUtility.removeTask(this.insight, taskId);
			Map<String, String> taskMap = new HashMap<>();
			taskMap.put("taskId", task.getId());
			return new NounMetadata(taskMap, PixelDataType.MAP, PixelOperationType.REMOVE_TASK);
		}

		return new NounMetadata(taskId, PixelDataType.REMOVE_TASK, PixelOperationType.REMOVE_TASK);
	}

	/**
	 * Determine if we should remove right away or during the stream
	 *
	 * @return
	 */
	protected boolean dropNow() {
		if (this.curRow.size() > 1) {
			return Boolean.parseBoolean(this.curRow.get(1).toString());
		}
		return false;
	}
}
