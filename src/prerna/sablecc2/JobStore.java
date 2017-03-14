package prerna.sablecc2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;


public enum JobStore {

	INSTANCE;
	private static final Logger LOGGER = LogManager.getLogger(JobStore.class.getName());
	private long count = 0;
	
	//TODO : make this thread safe
	private Map<String, Iterator> jobs = new HashMap<>(); 
	

	/**
	 * Returns the single job store instance in the application
	 * @return
	 */
	public static JobStore getInstance() {
		return INSTANCE;
	}
	
	public String addJob(String jobId,  Iterator data) {
		jobs.put(jobId, data);
		return jobId;
	}
	
	public String addJob(Iterator data) {
		String newId = generateID();
		return addJob(newId, data);
	}
	
	public  Iterator getJob(String jobId) {
		return jobs.get(jobId);
	}

	public void removeJob(String jobId) {
		jobs.remove(jobId);
	}
	
	private String generateID() {
		return "job"+ ++count;
	}
}
