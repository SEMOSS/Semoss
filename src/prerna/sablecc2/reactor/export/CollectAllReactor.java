package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.usertracking.UserTrackerFactory;

public class CollectAllReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	// -1 will get all
	private int limit = -1;
	
	public CollectAllReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.INCLUDE_META_KEY.getKey()};
	}
	
	public NounMetadata execute() {
		this.task = getTask();
		this.task.setNumCollect(this.limit);
		buildTask();
		
		// tracking
		if (this.task instanceof BasicIteratorTask) {
			try {
				// NEW TRACKER
				if(this.task.getTaskOptions() != null && !this.task.getTaskOptions().isEmpty()) {
					UserTrackerFactory.getInstance().trackVizWidget(this.insight, this.task.getTaskOptions(), ((BasicIteratorTask) task).getQueryStruct());
				} else {
					UserTrackerFactory.getInstance().trackQueryData(this.insight, ((BasicIteratorTask) task).getQueryStruct());
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA); //return the data
	}
	
	@Override
	protected void buildTask() {
		// do nothing
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}
	
}
