package prerna.sablecc2;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.Job;

public enum JobStore {

	INSTANCE;
	private static final Logger LOGGER = LogManager.getLogger(JobStore.class.getName());
	private long count = 0;
	
	//TODO : make this thread safe
	private Map<String, Job> jobs = new HashMap<>(); 
	

	/**
	 * Returns the single job store instance in the application
	 * @return
	 */
	public static JobStore getInstance() {
		return INSTANCE;
	}
	
	public String addJob(String jobId,  Job data) {
		jobs.put(jobId, data);
		return jobId;
	}
	
	public String addJob(Job data) {
		String newId = generateID();
		data.setId(newId);
		return addJob(newId, data);
	}
	
	public Job getJob(String jobId) {
		return jobs.get(jobId);
	}

	public void removeJob(String jobId) {
		jobs.remove(jobId);
	}
	
	private String generateID() {
		return "job"+ ++count;
	}
}
