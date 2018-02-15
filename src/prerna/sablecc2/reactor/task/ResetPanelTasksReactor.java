package prerna.sablecc2.reactor.task;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
		List<NounMetadata> taskOutput = new Vector<NounMetadata>();
		
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
			Map<String, Object> tOptions = task.getTaskOptions();
			if(tOptions != null && !tOptions.isEmpty()) {
				Set<String> panels = tOptions.keySet();
				if(processedPanels.containsAll(panels)) {
					// no new panels, so we can ignore
					// we require all the panels
					continue;
				}
				processedPanels.addAll(panels);
				logger.info("Trying to reset task = " + id);
				try {
					task.reset();
					logger.info("Success! Starting to collect data");
					// we add at index 0 since we are going in reverse
					taskOutput.add(0, new NounMetadata(task.collect(500, true), PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
					logger.info("Done collecting data");
				} catch(Exception e) {
					logger.info("Failed to reset task = " + id);
				}
			} else {
				logger.info("Ignore task = " + id);
			}
		}
		
		return new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.RESET_PANEL_TASKS);
	}
}
