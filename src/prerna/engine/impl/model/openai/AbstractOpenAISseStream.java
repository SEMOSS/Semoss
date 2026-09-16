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
package prerna.engine.impl.model.openai;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.sablecc2.comm.PixelJobStatus;

/**
 * Shared driver for the OpenAI SSE streams: pulls the model job's partial
 * output off {@link PixelJobManager} and feeds it to the protocol specific
 * subclass one chunk at a time.
 *
 * <p>
 * The partial content comes from the python side's {@code smss_stream}. The
 * payload shape is set by {@code smss_stream_func} in
 * gaas_tcp_server_handler.py - {@code {"stream_type": ..., "data": ...}} - and
 * the data portion matches the dictionary definitions in
 * semoss_streaming_util.py's StreamUtil class.
 *
 * <p>
 * {@link PixelJobManager#getStreamOut(String)} hands back only what has arrived
 * since the last read, so draining repeatedly never repeats a chunk. That is
 * what lets one conversation be assembled across many separate poll requests.
 */
public abstract class AbstractOpenAISseStream implements IOpenAISseStream {

	protected final String engineId;
	protected final String jobId;
	protected final long creationTimestamp;

	private boolean preludeWritten = false;
	private boolean complete = false;
	private boolean closed = false;
	private volatile long lastDrained = System.currentTimeMillis();

	protected AbstractOpenAISseStream(String engineId, String jobId, long creationTimestamp) {
		this.engineId = engineId;
		this.jobId = jobId;
		this.creationTimestamp = creationTimestamp;
	}

	@Override
	public boolean drain(Writer writer) throws IOException {
		this.lastDrained = System.currentTimeMillis();
		if (this.complete) {
			return true;
		}

		if (!this.preludeWritten) {
			writePrelude(writer);
			this.preludeWritten = true;
		}

		if (writeReadyChunks(writer)) {
			finish(writer);
			return true;
		}

		if (jobStatus() == PixelJobStatus.PROGRESS_COMPLETE) {
			// the status was read after the chunks, so sweep once more for
			// anything that landed in between before calling the stream done
			if (writeReadyChunks(writer)) {
				finish(writer);
				return true;
			}
			writeJobCompleted(writer);
			finish(writer);
			return true;
		}

		return false;
	}

	/**
	 * Hand every chunk that has arrived since the last read to the subclass.
	 *
	 * @param writer the sink for the SSE text
	 * @return true if a chunk ended the conversation
	 * @throws IOException if writing to {@code writer} fails
	 */
	private boolean writeReadyChunks(Writer writer) throws IOException {
		List<Map<String, Object>> chunks = PixelJobManager.getManager().getStreamOut(this.jobId);
		if (chunks == null || chunks.isEmpty()) {
			return false;
		}
		for (Map<String, Object> chunk : chunks) {
			if (writeChunk(chunk, writer)) {
				return true;
			}
		}
		return false;
	}

	private PixelJobStatus jobStatus() {
		PixelJobRunner runner = PixelJobManager.getManager().getJob(this.jobId);
		return runner == null ? PixelJobStatus.UNKNOWN_JOB : runner.getPixelJobStatus();
	}

	private void finish(Writer writer) throws IOException {
		writeEpilogue(writer);
		this.complete = true;
		writer.flush();
	}

	@Override
	public boolean isComplete() {
		return this.complete;
	}

	@Override
	public void close() {
		if (this.closed) {
			return;
		}
		this.closed = true;
		PixelJobManager.getManager().clearJob(this.jobId);
		PixelJobManager.getManager().removeJob(this.jobId);
	}

	/**
	 * @return when this stream was last drained, used to sweep streams whose caller
	 *         walked away
	 */
	public long getLastDrained() {
		return this.lastDrained;
	}

	/**
	 * @return the job feeding this stream
	 */
	public String getJobId() {
		return this.jobId;
	}

	/**
	 * Write any events that open the conversation, before the first chunk. Called
	 * once.
	 *
	 * @param writer the sink for the SSE text
	 * @throws IOException if writing to {@code writer} fails
	 */
	protected void writePrelude(Writer writer) throws IOException {
		// most protocols open with the first chunk
	}

	/**
	 * Translate one chunk of python stream output into SSE.
	 *
	 * @param streamObj the {@code {"stream_type": ..., "data": ...}} payload
	 * @param writer    the sink for the SSE text
	 * @return true if this chunk ends the conversation
	 * @throws IOException if writing to {@code writer} fails
	 */
	protected abstract boolean writeChunk(Map<String, Object> streamObj, Writer writer) throws IOException;

	/**
	 * Write the terminal events for a job that finished without a chunk saying so.
	 * Not called when a chunk already ended the conversation.
	 *
	 * @param writer the sink for the SSE text
	 * @throws IOException if writing to {@code writer} fails
	 */
	protected void writeJobCompleted(Writer writer) throws IOException {
		// protocols whose closing events are the same either way use writeEpilogue
	}

	/**
	 * Write the events that close the conversation, whichever way it ended. Called
	 * once, after any {@link #writeJobCompleted(Writer)}.
	 *
	 * @param writer the sink for the SSE text
	 * @throws IOException if writing to {@code writer} fails
	 */
	protected void writeEpilogue(Writer writer) throws IOException {
		// most protocols close with the chunk that ended them
	}
}
