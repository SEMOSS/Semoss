package prerna.sablecc2.reactor.task;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;

public class TaskLookupReactor extends AbstractReactor {
	
	public TaskLookupReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK_ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// this just returns the task id
		String taskId = this.curRow.get(0).toString();
		ITask task = this.insight.getTaskStore().getTask(taskId);
		return new NounMetadata(task, PixelDataType.TASK, PixelOperationType.TASK);
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) {
			return outputs;
		}
		
		outputs = new Vector<NounMetadata>();
		// since output is lazy
		// just return the execute
		outputs.add( (NounMetadata) execute());
		return outputs;
	}
}
