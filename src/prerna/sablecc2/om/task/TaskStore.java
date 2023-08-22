package prerna.sablecc2.om.task;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;

public class TaskStore {

	private static final Logger classLogger = LogManager.getLogger(TaskStore.class);
	
	// store for the task
	private Map<String, ITask> taskMap = new ConcurrentHashMap<String, ITask>(); 
	// count when we generate unique job ids
	private AtomicInteger count = new AtomicInteger(0);

	public String addTask(String taskId, ITask task) {
		classLogger.info("Adding new task = " + taskId);
		this.taskMap.put(taskId, task);
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
			try {
				task.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}
	
	public void renameTask(String taskId) {
		ITask task = this.taskMap.remove(taskId);
		addTask(task);
	}
	
	public void clearAllTasks() {
		for(String taskId : this.taskMap.keySet()) {
			ITask task = this.taskMap.get(taskId);
			try {
				task.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		this.taskMap.clear();
	}
	
	public Set<String> getTaskIds() {
		return this.taskMap.keySet();
	}
	
	private String generateID() {
		return "task"+ count.incrementAndGet();
	}
	
	public AtomicInteger getCount() {
		return this.count;
	}
	
	public void setCount(AtomicInteger count) {
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
