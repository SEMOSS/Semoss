/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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

public final class PixelJobManager {

	public enum InterruptResult {
		NOT_FOUND, ALREADY_DONE, CANCEL_REQUESTED
	}

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

	private static volatile PixelJobManager manager = new PixelJobManager();

	// keeps the job to thread
	private Map<String, PixelJobRunner> runnerPool = new ConcurrentHashMap<>();

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

	public PixelJobRunner makeJob(Insight insight, String sessionId, String routeId) {
		String jobId = GUID.v7().toUUID().toString();
		PixelJobRunner jobRunner = new PixelJobRunner(jobId, insight, sessionId, routeId);
		runnerPool.put(jobId, jobRunner);
		return jobRunner;
	}

	public PixelJobRunner makeJob(String jobId, Insight insight, String sessionId, String routeId) {
		PixelJobRunner jobRunner = new PixelJobRunner(jobId, insight, sessionId, routeId);
		runnerPool.put(jobId, jobRunner);
		return jobRunner;
	}

	public PixelJobRunner removeJob(String jobId) {
		return runnerPool.remove(jobId);
	}

	public PixelJobRunner getJob(String jobId) {
		return runnerPool.get(jobId);
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
		PixelJobRunner job = runnerPool.get(jobId);
		if (job == null) {
			return PixelJobStatus.UNKNOWN_JOB.getValue();
		}
		return job.getStatus();
	}

	public void clearJob(String jobId) {
		jobError.remove(jobId);
		jobStdOut.remove(jobId);
		jobStreamMap.remove(jobId);

		// partial as well
		jobPartialOut.remove(jobId);
	}

	public void flagStatus(String jobId, PixelJobStatus status) {
		PixelJobRunner job = runnerPool.get(jobId);
		if (job != null) {
			job.setStatus(status);
		}
	}

	public InterruptResult interruptThread(String jobId) {
		if (jobId == null || jobId.isBlank()) {
			return InterruptResult.NOT_FOUND;
		}

		PixelJobRunner pixelThread = runnerPool.get(jobId);
		if (pixelThread == null) {
			return InterruptResult.NOT_FOUND;
		}

		PixelJobStatus curStatus = pixelThread.getPixelJobStatus();
		if (curStatus == PixelJobStatus.COMPLETE || curStatus == PixelJobStatus.PROGRESS_COMPLETE
				|| curStatus == PixelJobStatus.ERROR || curStatus == PixelJobStatus.CANCELED) {
			return InterruptResult.ALREADY_DONE;
		}

		pixelThread.requestCancel();
		return InterruptResult.CANCEL_REQUESTED;
	}

	public PixelRunner getOutput(String jobId) {
		PixelJobRunner jobRunner = runnerPool.get(jobId);
		if (jobRunner == null) {
			return null;
		}
		return jobRunner.getRunner();
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
