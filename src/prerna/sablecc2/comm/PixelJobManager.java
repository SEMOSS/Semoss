package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;

public class PixelJobManager {

	/**
	 * Inner class to hold job output data and its lock
	 * 
	 * @param <T>
	 */
	private static class JobOutputHolder<T> {
		private final List<T> outputList = new ArrayList<>();
		private int offset = 0;
		private final Lock lock = new ReentrantLock();
	}

	/**
	 * Inner class to hold job streaming data and its lock
	 * 
	 * @deprecated once partial is completley switched to
	 *             {@link #getStreamOut(String)} this static class will be removed
	 * @param <T>
	 */
	@Deprecated
	private static class JobTextStreamHolder {
		private final StringBuilder content = new StringBuilder();
		private int offset = 0;
		private final Lock lock = new ReentrantLock();
	}

	private static PixelJobManager manager = new PixelJobManager();

	// keeps the job to thread
	private Map<String, PixelJobThread> threadPool = new ConcurrentHashMap<>();

	// Map of job id to its standard output
	private Map<String, JobOutputHolder<String>> jobStdOut = new ConcurrentHashMap<>();

	// Map of job id to its error output
	private Map<String, JobOutputHolder<String>> jobError = new ConcurrentHashMap<>();

	// Map of job id to streaming chunk map
	private Map<String, JobOutputHolder<Map<String, Object>>> jobStreamMap = new ConcurrentHashMap<>();

	/**
	 * Map of job id to stdOut messages
	 * 
	 * @deprecated for jobStreamMapOffset for returning streaming data
	 */
	@Deprecated
	private Map<String, JobTextStreamHolder> jobPartialOut = new ConcurrentHashMap<>();

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

	/**
	 * 
	 * @param jobId
	 * @param stdOut
	 */
	public void addStdOut(String jobId, String stdOut) {
		JobOutputHolder<String> holder = jobStdOut.computeIfAbsent(jobId, k -> new JobOutputHolder<>());
		holder.lock.lock();
		try {
			holder.outputList.add(stdOut);
		} finally {
			holder.lock.unlock();
		}
	}

	/**
	 * 
	 * @param jobId
	 * @return
	 */
	public List<String> getStdOut(String jobId) {
		JobOutputHolder<String> holder = jobStdOut.get(jobId);
		if (holder == null) {
			return new ArrayList<>();
		}

		holder.lock.lock();
		try {
			if (holder.offset >= holder.outputList.size()) {
				return new ArrayList<>();
			}

			List<String> output = new ArrayList<>(holder.outputList.subList(holder.offset, holder.outputList.size()));
			holder.offset = holder.outputList.size();
			return output;
		} finally {
			holder.lock.unlock();
		}
	}

	/**
	 * 
	 * @param jobId
	 * @param stdErr
	 */
	public void addStdErr(String jobId, String stdErr) {
		JobOutputHolder<String> holder = jobError.computeIfAbsent(jobId, k -> new JobOutputHolder<>());
		holder.lock.lock();
		try {
			holder.outputList.add(stdErr);
		} finally {
			holder.lock.unlock();
		}
	}

	/**
	 * 
	 * @param jobId
	 * @return
	 */
	public List<String> getError(String jobId) {
		JobOutputHolder<String> holder = jobError.get(jobId);
		if (holder == null) {
			return new ArrayList<>();
		}

		holder.lock.lock();
		try {
			if (holder.offset >= holder.outputList.size()) {
				return new ArrayList<>();
			}

			List<String> output = new ArrayList<>(holder.outputList.subList(holder.offset, holder.outputList.size()));
			holder.offset = holder.outputList.size();
			return output;
		} finally {
			holder.lock.unlock();
		}
	}

	/**
	 * 
	 * @param jobId
	 * @param stream
	 */
	public void addStreamOut(String jobId, Map<String, Object> stream) {
		JobOutputHolder<Map<String, Object>> holder = jobStreamMap.computeIfAbsent(jobId, k -> new JobOutputHolder<>());
		holder.lock.lock();
		try {
			holder.outputList.add(stream);
		} finally {
			holder.lock.unlock();
		}
	}

	/**
	 * 
	 * @param jobId
	 * @return
	 */
	public List<Map<String, Object>> getStreamOut(String jobId) {
		JobOutputHolder<Map<String, Object>> holder = jobStreamMap.get(jobId);
		if (holder == null) {
			return new ArrayList<>();
		}
		holder.lock.lock();
		try {
			if (holder.offset >= holder.outputList.size()) {
				return new ArrayList<>();
			}

			List<Map<String, Object>> output = new ArrayList<>(
					holder.outputList.subList(holder.offset, holder.outputList.size()));
			holder.offset = holder.outputList.size();
			return output;
		} finally {
			holder.lock.unlock();
		}
	}

	public String getStatus(String jobId) {
		return threadPool.get(jobId).getStatus();
	}

	public void clearJob(String jobId) {
		jobError.remove(jobId);
		jobStdOut.remove(jobId);
		jobStreamMap.remove(jobId);

		// partial as well
		jobPartialOut.remove(jobId);
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

	/**
	 * @deprecated switch to {@link #addStreamOut(String, Map)} instead.
	 * @param jobId
	 * @param stdOut
	 */
	@Deprecated
	public void addPartialOut(String jobId, String stdOut) {
		JobTextStreamHolder holder = jobPartialOut.computeIfAbsent(jobId, k -> new JobTextStreamHolder());
		holder.lock.lock();
		try {
			holder.content.append(stdOut);
		} finally {
			holder.lock.unlock();
		}
	}

	/**
	 * @deprecated switch to {@link #getStreamOut(String)} instead.
	 * @param jobId
	 * @param stdOut
	 */
	@Deprecated
	public Map<String, String> getPartial(String jobId) {
		JobTextStreamHolder holder = jobPartialOut.get(jobId);
		if (holder == null) {
			return new HashMap<>();
		}
		holder.lock.lock();
		try {
			Map<String, String> retMap = new HashMap<>();
			int size = holder.content.length();
			retMap.put("total", holder.content.toString());

			if (holder.offset >= size) {
				retMap.put("new", "");
			} else {
				String newMessage = holder.content.substring(holder.offset, size);
				retMap.put("new", newMessage);
				// update the offset
				holder.offset = size;
			}
			return retMap;
		} finally {
			holder.lock.unlock();
		}
	}
}
