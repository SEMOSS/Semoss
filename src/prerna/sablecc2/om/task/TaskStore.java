package prerna.sablecc2.om.task;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.task.options.TaskOptions;

public class TaskStore {

	private static final Logger LOGGER = LogManager.getLogger(TaskStore.class.getName());
	
	// store for the task
	private Map<String, ITask> taskMap = new LinkedHashMap<String, ITask>(); 
	// count when we generate unique job ids
	private long count = 0;

	public String addTask(String taskId, ITask task) {
		LOGGER.info("Adding new task = " + taskId);
		this.taskMap.put(taskId, task);
		++count;
		return taskId;
	}
	
	public String addTask(ITask newTask) {
		String newId = generateID();
		newTask.setId(newId);
		return addTask(newId, newTask);
	}
	
	public ITask getTask(String taskId) {
		return this.taskMap.get(taskId);
	}

	public void removeTask(String taskId) {
		ITask task = this.taskMap.remove(taskId);
		if(task != null) {
			task.cleanUp();
		}
	}
	
	public void renameTask(String taskId) {
		ITask task = this.taskMap.remove(taskId);
		addTask(task);
	}
	
	public void clearAllTasks() {
		for(String taskId : this.taskMap.keySet()) {
			ITask task = this.taskMap.get(taskId);
			task.cleanUp();
		}
		this.taskMap.clear();
	}
	
	public Set<String> getTaskIds() {
		return this.taskMap.keySet();
	}
	
	private String generateID() {
		return "task"+ ++count;
	}
	
	public long getCount() {
		return this.count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
	
	/**
	 * Loop through all the tasks in the store to see where they belong
	 * @return
	 */
	public Map<String, Map<String, String>> getPanelLayerTaskIdMap() {
		Map<String, Map<String, String>> panleLayerTaskIdMap = new HashMap<>();
		for(String taskId : this.taskMap.keySet()) {
			ITask task = this.taskMap.get(taskId);
			TaskOptions taskOptions = task.getTaskOptions();
			if(taskOptions != null) {
				Set<String> panelIds = taskOptions.getPanelIds();
				for(String panelId : panelIds) {
					String layer = taskOptions.getPanelLayerId(panelId);
					// store and override if required
					if(layer == null) {
						layer = "0";
					}

					Map<String, String> layerMap = null;
					if(panleLayerTaskIdMap.containsKey(panelId)) {
						layerMap = panleLayerTaskIdMap.get(panelId);
					} else {
						layerMap = new HashMap<>();
						panleLayerTaskIdMap.put(panelId, layerMap);
					}
					layerMap.put(layer, taskId);
				}
			}
		}

		return panleLayerTaskIdMap;
	}
}
