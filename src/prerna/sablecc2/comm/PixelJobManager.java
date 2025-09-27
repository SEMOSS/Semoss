package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;

public class PixelJobManager {

	private static PixelJobManager manager = new PixelJobManager();

	// keeps the job to thread
	private Map<String, PixelJobThread> threadPool = new ConcurrentHashMap<>();

	// Map of job id to stdOut messages
	private Map<String, List<String>> jobStdOut = new ConcurrentHashMap<>();

	// Map of job id to offset
	private Map<String, Integer> stdOutOffset = new ConcurrentHashMap<>();

	// Map of job id to error messages
	private Map<String, List<String>> jobError = new ConcurrentHashMap<>();

	// Map of job id to offset
	private Map<String, Integer> errorOffset = new ConcurrentHashMap<>();

	// Map of job id to streaming chunk map
	private Map<String, List<Map<String, Object>>> jobStreamMap = new ConcurrentHashMap<>();

	// Map of job id to streaming chunk map
	private Map<String, Integer> jobStreamMapOffset = new ConcurrentHashMap<>();

	/**
	 * Map of job id to stdOut messages
	 * 
	 * @deprecated for jobStreamMapOffset for returning streaming data
	 */
	@Deprecated
	private Map<String, StringBuilder> jobPartialOut = new ConcurrentHashMap<>();

	/**
	 * Map of job id to offset
	 * 
	 * @deprecated for jobStreamMapOffset for returning streaming data
	 */
	@Deprecated
	private Map<String, Integer> jobPartialOutOffset = new ConcurrentHashMap<>();

	private PixelJobManager() {

	}

	public static PixelJobManager getManager() {
		if (manager != null) {
			return manager;
		}

		synchronized (PixelJobManager.class) {
			if (manager == null) {
				manager = new PixelJobManager();
			}
		}
		return manager;
	}

	public PixelJobThread makeJob(Insight insight, String sessionId, String routeId) {
		String jobId = GUID.v7().toUUID().toString();
		PixelJobThread jt = new PixelJobThread(jobId, insight, sessionId, routeId);
		threadPool.put(jobId, jt);
		return jt;
	}

	public PixelJobThread makeJob(String jobId, Insight insight, String sessionId, String routeId) {
		PixelJobThread jt = new PixelJobThread(jobId, insight, sessionId, routeId);
		threadPool.put(jobId, jt);
		return jt;
	}

	public PixelJobThread removeJob(String jobId) {
		return threadPool.remove(jobId);
	}

	public PixelJobThread getJob(String jobId) {
		return threadPool.get(jobId);
	}

	public void addStdOut(String jobId, String stdOut) {
		List<String> outputList = jobStdOut.get(jobId);
		if (outputList == null) {
			synchronized (jobStdOut) {
				outputList = jobStdOut.get(jobId);
				if (outputList == null) {
					outputList = Collections.synchronizedList(new ArrayList<>());
					jobStdOut.put(jobId, outputList);
				}
			}
		}
		outputList.add(stdOut);
	}

	public List<String> getStdOut(String jobId) {
		int curOffset = stdOutOffset.getOrDefault(jobId, 0);
		return getStdOut(jobId, curOffset);
	}

	public List<String> getStdOut(String jobId, int offset) {
		List<String> outputList = jobStdOut.get(jobId);
		if (outputList == null || outputList.isEmpty()) {
			return new ArrayList<>();
		}
		synchronized (outputList) {
			int size = outputList.size();
			List<String> output = new ArrayList<>(outputList.subList(offset, size));
			int newOffset = offset + output.size();
			// update the offset
			stdOutOffset.put(jobId, newOffset);
			return output;
		}
	}

	public void addStdErr(String jobId, String stdErr) {
		List<String> outputList = jobError.get(jobId);
		if (outputList == null) {
			synchronized (jobError) {
				outputList = jobError.get(jobId);
				if (outputList == null) {
					outputList = Collections.synchronizedList(new ArrayList<>());
					jobError.put(jobId, outputList);
				}
			}
		}
		outputList.add(stdErr);
	}

	public List<String> getError(String jobId) {
		int curOffset = errorOffset.getOrDefault(jobId, 0);
		return getError(jobId, curOffset);
	}

	public List<String> getError(String jobId, int offset) {
		List<String> outputList = jobError.get(jobId);
		if (outputList == null || outputList.isEmpty()) {
			return new ArrayList<>();
		}
		synchronized (outputList) {
			int size = outputList.size();
			List<String> output = new ArrayList<>(outputList.subList(offset, size));
			int newOffset = offset + output.size();
			// update the offset
			errorOffset.put(jobId, newOffset);
			return output;
		}
	}

	/**
	 * @deprecated switch to {@link #addStreamOut(String, Map)} instead.
	 * @param jobId
	 * @param stdOut
	 */
	@Deprecated
	public void addPartialOut(String jobId, String stdOut) {
		StringBuilder builder = jobPartialOut.get(jobId);
		if (builder == null) {
			synchronized (jobPartialOut) {
				builder = jobPartialOut.get(jobId);
				if (builder == null) {
					builder = new StringBuilder();
					jobPartialOut.put(jobId, builder);
				}
			}
		}
		synchronized (builder) {
			builder.append(stdOut);
		}
	}

	/**
	 * @deprecated switch to {@link #getStreamOut(String)} instead.
	 * @param jobId
	 * @param stdOut
	 */
	@Deprecated
	public Map<String, String> getPartial(String jobId) {
		int curOffset = jobPartialOutOffset.getOrDefault(jobId, 0);
		return getPartial(jobId, curOffset);
	}

	/**
	 * @deprecated switch to {@link #getStreamOut(String, int)} instead.
	 * @param jobId
	 * @param stdOut
	 */
	@Deprecated
	public Map<String, String> getPartial(String jobId, int offset) {
		StringBuilder builder = jobPartialOut.get(jobId);
		if (builder == null || builder.length() == 0) {
			return new HashMap<>();
		}
		synchronized (builder) {
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

	/**
	 * 
	 * @param jobId
	 * @param stream
	 */
	public void addStreamOut(String jobId, Map<String, Object> stream) {
		List<Map<String, Object>> outputList = jobStreamMap.get(jobId);
		if (outputList == null) {
			synchronized (jobStreamMap) {
				outputList = jobStreamMap.get(jobId);
				if (outputList == null) {
					outputList = Collections.synchronizedList(new ArrayList<>());
					jobStreamMap.put(jobId, outputList);
				}
			}
		}
		outputList.add(stream);
	}

	/**
	 * 
	 * @param jobId
	 * @return
	 */
	public List<Map<String, Object>> getStreamOut(String jobId) {
		int curOffset = jobStreamMapOffset.getOrDefault(jobId, 0);
		return getStreamOut(jobId, curOffset);
	}

	/**
	 * 
	 * @param jobId
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getStreamOut(String jobId, int offset) {
		List<Map<String, Object>> outputList = jobStreamMap.get(jobId);
		if (outputList == null || outputList.isEmpty()) {
			return new ArrayList<>();
		}
		synchronized (outputList) {
			int size = outputList.size();
			List<Map<String, Object>> output = new ArrayList<>(outputList.subList(offset, size));
			int newOffset = offset + output.size();
			// update the offset
			jobStreamMapOffset.put(jobId, newOffset);
			return output;
		}
	}

	public String getStatus(String jobId) {
		return threadPool.get(jobId).getStatus();
	}

	public void clearJob(String jobId) {
		jobError.remove(jobId);
		stdOutOffset.remove(jobId);
		errorOffset.remove(jobId);
		jobStdOut.remove(jobId);

		// partial as well
		jobPartialOut.remove(jobId);
		jobPartialOutOffset.remove(jobId);

		jobStreamMap.remove(jobId);
		jobStreamMapOffset.remove(jobId);
	}

	public void flagStatus(String jobId, PixelJobStatus status) {
		threadPool.get(jobId).setStatus(status);
	}

	public void interruptThread(String jobId) {
		if (threadPool.get(jobId) != null) {
			PixelJobThread pixelThread = threadPool.get(jobId);
			pixelThread.interrupt();
			pixelThread.setStatus(PixelJobStatus.CANCELED);
		}
	}

	public PixelRunner getOutput(String jobId) {
		PixelJobThread jt = threadPool.get(jobId);
		return jt.getRunner();
	}
}
