package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;

import prerna.sablecc2.PixelRunner;

public class JobManager {
	
	static JobManager manager = new JobManager();

	// obviously I assume the user wont run that many jobs to start with
	// I will adjust this to a random number generator later
	
	// hashtable to status
	private Hashtable <String, String> jobStatus = new Hashtable <String, String> ();
	
	// hashtable of job id to stdOut messages
	private Hashtable <String, List<String>> jobStdOut = new Hashtable <String, List<String>>();
	
	//hashtable of job id to offset
	private Hashtable <String, Integer> stdOutOffset = new Hashtable<String, Integer>();
	
	// hashtable of job id to error messages
	private Hashtable <String, List<String>> jobError = new Hashtable <String, List<String>>();

	//hashtable of job id to offset
	private Hashtable <String, Integer> errorOffset = new Hashtable<String, Integer>();

	// output offset - this will eventually be needed for distributed processing
	private Hashtable <String, Integer> outputOffset = new Hashtable<String, Integer>();
	
	// keeps the job to thread
	private Hashtable <String, JobThread> threadPool = new Hashtable<String, JobThread>();
	
	// hashtable of job id to stdOut messages
	private Hashtable <String, StringBuilder> jobPartialOut = new Hashtable <String, StringBuilder>();
	
	//hashtable of job id to offset
	private Hashtable <String, Integer> jobPartialOutOffset = new Hashtable<String, Integer>();
	
	private JobManager() {
		
	}
	
	public static JobManager getManager() {
		if(manager == null) {
			manager = new JobManager();
		}
		return manager;
	}
	
	public JobThread makeJob() {
		String jobId = UUID.randomUUID().toString();
		jobStatus.put(jobId, JobStatus.CREATED+"");
		JobThread jt = new JobThread(jobId);
		threadPool.put(jobId, jt);
		return jt;
	}
	
	public JobThread makeJob(String jobId) {
		jobStatus.put(jobId, JobStatus.CREATED+"");
		JobThread jt = new JobThread(jobId);
		threadPool.put(jobId, jt);
		return jt;
	}
	
	public void removeJob(String jobId) {
		threadPool.remove(jobId);
	}
	
	public JobThread getJob(String jobId) {
		return threadPool.get(jobId);
	}
	
	public void addStdOut(String jobId, String stdOut)	{
		List<String> outputList = new Vector<String>();
		if(jobStdOut.containsKey(jobId)) {
			outputList = jobStdOut.get(jobId);
		} else {
			jobStdOut.put(jobId, outputList);
		}
		synchronized(outputList) {
			outputList.add(stdOut);
		}
	}

	public void addStdErr(String jobId, String stdErr) {
		List<String> outputList = new Vector<String>();
		if(jobError.containsKey(jobId)) {
			outputList = jobError.get(jobId);
		} else {
			jobError.put(jobId, outputList);
		}
		synchronized(outputList) {
			outputList.add(stdErr);
		}
	}
	
	public List<String> getStdOut(String jobId) {
		int curOffset = 0;
		if(stdOutOffset.containsKey(jobId)) {
			curOffset = stdOutOffset.get(jobId);
		}
		return getStdOut(jobId, curOffset);
	}

	public List<String> getError(String jobId) {
		int curOffset = 0;
		if(errorOffset.containsKey(jobId)) {
			curOffset = errorOffset.get(jobId);		
		} 
		return getError(jobId, curOffset);
	}

	public List<String> getStdOut(String jobId, int offset) {
		List<String> outputList = jobStdOut.get(jobId);
		if(outputList == null || outputList.isEmpty()) {
			return new ArrayList<String>();
		}
		synchronized(outputList) {
			int size = outputList.size();
			List<String> output = new Vector<String>(outputList.subList(offset, size));
			int newOffset = offset+output.size();
			// update the offset
			stdOutOffset.put(jobId, newOffset);
			return output;
		}
	}
	
	public List<String> getError(String jobId, int offset) {
		List<String> outputList = jobError.get(jobId);
		if(outputList == null || outputList.isEmpty()) {
			return new ArrayList<String>();
		}
		synchronized(outputList) {
			int size = outputList.size();
			List<String> output =  new Vector<String>(outputList.subList(offset, size));
			int newOffset = offset+output.size();
			// update the offset
			errorOffset.put(jobId, newOffset);
			return output;
		}
	}
	
	public void addPartialOut(String jobId, String stdOut)	{
		StringBuilder builder = null;
		if(jobPartialOut.containsKey(jobId)) {
			builder = jobPartialOut.get(jobId);
		} else {
			builder = new StringBuilder();
			jobPartialOut.put(jobId, builder);
		}
		synchronized(builder) {
			builder.append(stdOut);
		}
	}
	
	public Map<String, String> getPartial(String jobId) {
		int curOffset = 0;
		if(jobPartialOutOffset.containsKey(jobId)) {
			curOffset = jobPartialOutOffset.get(jobId);
		}
		return getPartial(jobId, curOffset);
	}
	
	public Map<String, String> getPartial(String jobId, int offset) {
		StringBuilder builder = jobPartialOut.get(jobId);
		if(builder == null || builder.length() == 0) {
			return new HashMap<>();
		}
		synchronized(builder) {
			Map<String, String> retMap = new HashMap<>();
			int size = builder.length();
			retMap.put("total", builder.toString());
			String newMessage = builder.substring(offset, size);
			retMap.put("new", newMessage);
			// update the offset
			int newOffset = size;
			jobPartialOutOffset.put(jobId, newOffset);
			return retMap;
		}
	}
	
	public String getStatus(String jobId) {
		return threadPool.get(jobId).getStatus();
	}
	
	public void resetJob(String jobId) // trimming operation
	{
		// this is when I want to remove everything until current offset and set offset to zero
		int curOut = errorOffset.get(jobId);		
		int curErr = stdOutOffset.get(jobId);
		
		List<String> errorList = jobError.get(jobId);
		List<String> outputList = jobStdOut.get(jobId);
		
		// trim the error list
		synchronized(errorList)	{
			errorList =  new Vector<String>(errorList.subList(curErr, errorList.size() - 1));
			jobError.put(jobId, errorList);
			errorOffset.put(jobId, 0);
		}
		
		// trim the outputlist
		synchronized(outputList) {
			outputList =  new Vector<String>(outputList.subList(curOut, outputList.size() - 1));
			jobStdOut.put(jobId, outputList);
			stdOutOffset.put(jobId, 0);
		}
	}
	
	public void clearJob(String jobId) {
		jobStatus.remove(jobId);
		jobError.remove(jobId);
		stdOutOffset.remove(jobId);
		errorOffset.remove(jobId);
		jobStdOut.remove(jobId);
		
		// partial as well
		jobPartialOut.remove(jobId);
		jobPartialOutOffset.remove(jobId);
	}
	
	public void flagStatus(String jobId, JobStatus status) {
		threadPool.get(jobId).setStatus(status);
	}
	
	public void interruptThread(String jobId) {
		((Thread)threadPool.get(jobId)).interrupt();
	}
	
	public PixelRunner getOutput(String jobId) {
		JobThread jt = threadPool.get(jobId);
		return jt.getRunner();
	}
}
