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

import java.util.List;
import java.util.Vector;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class TaskReactor extends AbstractReactor {

	public TaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK_ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// this just returns the task id
		String taskId = this.curRow.get(0).toString();
		ITask task = this.insight.getTaskStore().getTask(taskId);
		if (task == null) {
			throw new NullPointerException("Could not find task with id = " + taskId);
		}
		return new NounMetadata(task, PixelDataType.TASK, PixelOperationType.TASK);
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if (outputs != null) {
			return outputs;
		}

		outputs = new Vector<NounMetadata>();
		// since output is lazy
		// just return the execute
		outputs.add((NounMetadata) execute());
		return outputs;
	}
}
