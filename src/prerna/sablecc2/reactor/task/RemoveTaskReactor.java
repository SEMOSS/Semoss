package prerna.sablecc2.reactor.task;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class RemoveTaskReactor extends AbstractReactor {
	
	public RemoveTaskReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK_ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get the task id
		String taskId = this.curRow.get(0).toString();
		return new NounMetadata(taskId, PixelDataType.REMOVE_TASK);
	}
}
