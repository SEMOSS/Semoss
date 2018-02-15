package prerna.sablecc2.reactor.task;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.reactor.AbstractReactor;

public class ResetPanelTasksReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ResetPanelTasksReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		TaskStore tStore = this.insight.getTaskStore();
		Set<String> taskIds = tStore.getTaskIds();
		List<NounMetadata> taskOutput = new Vector<NounMetadata>();
		for(String id : taskIds) {
			ITask task = tStore.getTask(id);
			// a task is for a panel 
			// if it has task options
			if(task.getTaskOptions() != null && !task.getTaskOptions().isEmpty()) {
				logger.info("Trying to reset task = " + id);
				try {
					task.reset();
					logger.info("Success! Starting to collect data");
					taskOutput.add(new NounMetadata(task.collect(500, true), PixelDataType.TASK, PixelOperationType.TASK));
					logger.info("Done collecting data");
				} catch(Exception e) {
					logger.info("Failed to reset task = " + id);
				}
			} else {
				logger.info("Ignore task = " + id);
			}
		}
		
		return new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.TASK_LIST);
	}
}
