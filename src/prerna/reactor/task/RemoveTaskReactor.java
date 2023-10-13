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
		if(inputNoun.getNounType() == PixelDataType.TASK) {
			taskId = ((ITask) inputNoun.getValue()).getId();
		} else {
			taskId = inputNoun.getValue().toString();
		}
		ITask task = insight.getTaskStore().getTask(taskId);
		if(task == null) {
			throw new IllegalArgumentException("Could not find task id = " + taskId);
		}
		
		// drop now
		if(dropNow()) {
			InsightUtility.removeTask(this.insight, taskId);
			Map<String, String> taskMap = new HashMap<>();
			taskMap.put("taskId", task.getId());
			return new NounMetadata(taskMap, PixelDataType.MAP, PixelOperationType.REMOVE_TASK);
		}
		
		return new NounMetadata(taskId, PixelDataType.REMOVE_TASK, PixelOperationType.REMOVE_TASK);
	}
	
	/**
	 * Determine if we should remove right away or during the stream
	 * @return
	 */
	protected boolean dropNow() {
		if(this.curRow.size() > 1) {
			return Boolean.parseBoolean(this.curRow.get(1).toString());
		}
		return false;
	}
}
