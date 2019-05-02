package prerna.sablecc2.reactor.task;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.AbstractReactor;

public class ResetAllReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ResetAllReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// store the tasks to reset
		List<ITask> tasksToReset = new Vector<ITask>();
		
		TaskStore tStore = this.insight.getTaskStore();
		// note, the ids are ordered
		Set<String> taskIds = tStore.getTaskIds();
		
		// keeping track of processed panels in case tasks
		// were not properly dropped
		List<String> processedPanels = new Vector<String>();
		
		LinkedList<String> list = new LinkedList<String>(taskIds);
		Iterator<String> itr = list.descendingIterator();
		while(itr.hasNext()) {
			String id = itr.next();
			ITask task = tStore.getTask(id);
			// a task is for a panel 
			// if it has task options
			TaskOptions tOptions = task.getTaskOptions();
			if(tOptions != null && !tOptions.isEmpty()) {
				Set<String> panels = tOptions.getPanelIds();
				if(processedPanels.containsAll(panels)) {
					// no new panels, so we can ignore
					// we require all the panels
					continue;
				}
				processedPanels.addAll(panels);
				logger.info("Found task to reset = " + id);
				tasksToReset.add(0, task);
			} else {
				logger.info("Ignore task = " + id);
			}
		}
		
		List<NounMetadata> taskOutput = new Vector<NounMetadata>();
		
		for(ITask task : tasksToReset) {
			String id = task.getId();
			logger.info("Trying to reset task = " + id);
			try {
				task.reset();
				
				// i was able to reset the task
				// let me modify the id so the FE knows 
				// to override the current view
				tStore.renameTask(id);
				
				logger.info("Success! Starting to collect data");
				// we add at index 0 since we are going in reverse
				taskOutput.add(0, new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
				logger.info("Done collecting data");
			} catch(Exception e) {
				logger.info("Failed to reset task = " + id);
			}
		}
		
		return new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.RESET_PANEL_TASKS);
	}
}
