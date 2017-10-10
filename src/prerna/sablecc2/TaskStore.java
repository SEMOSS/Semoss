package prerna.sablecc2;

import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.Task;

public class TaskStore {

	private static final Logger LOGGER = LogManager.getLogger(TaskStore.class.getName());
	
	// store for the jobs
	private Map<String, Task> jobs = new Hashtable<String, Task>(); 
	// count when we generate unique job ids
	private long count = 0;

	public String addTask(String taskId, Task task) {
		LOGGER.info("Adding new task = " + taskId);
		this.jobs.put(taskId, task);
		return taskId;
	}
	
	public String addTask(Task data) {
		String newId = generateID();
		data.setId(newId);
		return addTask(newId, data);
	}
	
	public Task getTask(String jobId) {
		return this.jobs.get(jobId);
	}

	public void removeTask(String jobId) {
		this.jobs.remove(jobId);
	}
	
	private String generateID() {
		return "task"+ ++count;
	}
}
