package prerna.sablecc2.om.task;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
}
