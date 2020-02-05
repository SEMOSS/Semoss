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
		NounMetadata inputNoun = this.curRow.getNoun(0);
		String taskId = null;
		if(inputNoun.getNounType() == PixelDataType.TASK) {
			taskId = ((ITask) inputNoun.getValue()).getId();
		} else {
			taskId = inputNoun.getValue().toString();
		}
		if(insight.getTaskStore().getTask(taskId) == null) {
			throw new IllegalArgumentException("Could not find task id = " + taskId);
		}
		return new NounMetadata(taskId, PixelDataType.REMOVE_TASK, PixelOperationType.REMOVE_TASK);
	}
}
