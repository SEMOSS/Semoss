package prerna.sablecc2.reactor.task;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.AbstractReactor;

public class ResetTask extends AbstractReactor {
	
	public ResetTask() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK_ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// this just returns the task id
		ITask task = getTask();
		task.reset();
		return new NounMetadata(task, PixelDataType.TASK, PixelOperationType.TASK);
	}
	
	protected ITask getTask() {
		ITask task = null;
		
		GenRowStruct grsTasks = this.store.getNoun(PixelDataType.TASK.toString());
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(grsTasks != null && !grsTasks.isEmpty()) {
			task = (ITask) grsTasks.get(0);
		} else {
			List<Object> tasks = this.curRow.getValuesOfType(PixelDataType.TASK);
			if(tasks != null && !tasks.isEmpty()) {
				task = (ITask) tasks.get(0);
			}
		}
		
		// maybe the user passed in a string
		if(task == null) {
			String taskId = this.curRow.get(0).toString();
			task = this.insight.getTaskStore().getTask(taskId);
		}
		
		return task;
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
