package prerna.sablecc2.reactor.task;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;

public class RemoveTaskReactor extends AbstractReactor {
	
	public RemoveTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK_ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get the task id
		String taskId = this.curRow.get(0).toString();
		// get the task object
		ITask task = this.insight.getTaskStore().getTask(taskId);
		// remove the task id
		this.insight.getTaskStore().removeTask(taskId);
		// return the task object so we know what was removed
		return new NounMetadata(task, PixelDataType.TASK, PixelOperationType.REMOVE_TASK);
	}
}
