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
package prerna.reactor.model;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.ModelPixelInvoker;
import prerna.engine.impl.model.openai.AbstractOpenAISseStream;
import prerna.engine.impl.model.openai.OpenAIStreamRegistry;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Hand back the SSE that a streaming {@code OpenAIPassthrough} request has
 * produced since the last call.
 *
 * <p>
 * {@code OpenAIPassthrough} answers a streaming request with a job id rather
 * than a body, because a pixel cannot hold a response open the way the http
 * endpoint can. The caller then calls this until {@code done} comes back true,
 * appending what it gets to its own stream. That is what turns a single
 * blocking call into token by token delivery.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * OpenAIStreamPoll(jobId = "&lt;the job id from OpenAIPassthrough&gt;");
 * </pre>
 *
 * The return describes a slice of the conversation:
 *
 * <pre>
 * { "jobId": "...", "body": "&lt;sse text, possibly empty&gt;", "done": false }
 * </pre>
 *
 * Rather than answering an empty body the instant nothing is ready, this waits
 * briefly for the model to produce something, so a caller can poll in a tight
 * loop without turning one generation into hundreds of round trips.
 */
public class OpenAIStreamPollReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(OpenAIStreamPollReactor.class);

	// one-off key for this reactor, intentionally not in ReactorKeysEnum
	private static final String JOB_ID = "jobId";

	private static final String BODY = "body";
	private static final String DONE = "done";

	/**
	 * How long to wait for the model to produce something before answering with an
	 * empty slice. A few multiples of the poll interval, so a caller looping on
	 * this costs a handful of round trips a second while the model is thinking.
	 */
	private static final long WAIT_FOR_OUTPUT_MS = 250L;

	public OpenAIStreamPollReactor() {
		this.keysToGet = new String[] { JOB_ID };
	}

	@Override
	public String getReactorDescription() {
		return "Return the openai formatted output a streaming model request has produced since the last call";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String jobId = this.keyValue.get(JOB_ID);
		if (jobId == null || (jobId = jobId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Bad Request: the 'jobId' input is required");
		}

		AbstractOpenAISseStream stream = OpenAIStreamRegistry.get(jobId);
		if (stream == null) {
			// the stream finished on an earlier poll, or was swept as abandoned;
			// either way there is nothing left to send and the caller should stop
			return response(jobId, "", true);
		}

		StringWriter writer = new StringWriter();
		boolean done;
		try {
			// one caller at a time: the stream carries protocol state from one
			// chunk to the next, so overlapping polls would interleave its output
			synchronized (stream) {
				done = drainWithWait(stream, writer);
			}
		} catch (IOException e) {
			// the writer here is a StringWriter, so this is a real failure
			classLogger.error("Failed to assemble the streamed response for job '{}'", jobId, e);
			OpenAIStreamRegistry.remove(jobId);
			throw new IllegalArgumentException("Failed to assemble the streamed response: " + e.getMessage());
		}

		if (done) {
			OpenAIStreamRegistry.remove(jobId);
		}
		return response(jobId, writer.toString(), done);
	}

	/**
	 * Drain the stream, giving the model a moment to produce something before
	 * giving up on this round.
	 *
	 * @param stream the stream being assembled
	 * @param writer the sink for this slice of the conversation
	 * @return whether the conversation is finished
	 * @throws IOException if writing to {@code writer} fails
	 */
	private boolean drainWithWait(AbstractOpenAISseStream stream, StringWriter writer) throws IOException {
		long deadline = System.currentTimeMillis() + WAIT_FOR_OUTPUT_MS;
		while (true) {
			if (stream.drain(writer)) {
				return true;
			}
			if (writer.getBuffer().length() > 0 || System.currentTimeMillis() >= deadline) {
				return false;
			}
			try {
				Thread.sleep(ModelPixelInvoker.STREAM_POLL_INTERVAL_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
	}

	private NounMetadata response(String jobId, String body, boolean done) {
		Map<String, Object> response = new HashMap<>();
		response.put(JOB_ID, jobId);
		response.put(BODY, body);
		response.put(DONE, done);
		return new NounMetadata(response, PixelDataType.MAP);
	}
}
