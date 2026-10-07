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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Holds the SSE streams that are being assembled across separate requests.
 *
 * <p>
 * A caller holding an open http response keeps its stream on the stack. A
 * caller that polls - the reactor serving python - cannot, because each poll is
 * its own pixel execution, so the stream lives here between them, keyed by the
 * model job it is draining.
 *
 * <p>
 * Streams are removed when they complete. A caller that walks away mid stream
 * (a cancelled cell, a dead python process) would otherwise leave its stream
 * and its job behind, so anything left untouched past {@link #STALE_AFTER_MS}
 * is closed on the next registration.
 */
public class OpenAIStreamRegistry {

	private static final Logger classLogger = LogManager.getLogger(OpenAIStreamRegistry.class);

	/**
	 * How long a stream can go undrained before it is treated as abandoned. Longer
	 * than any sane gap between polls, short enough that a dead caller's job does
	 * not sit around for the life of the server.
	 */
	public static final long STALE_AFTER_MS = 10 * 60 * 1000L;

	private static final Map<String, AbstractOpenAISseStream> STREAMS = new ConcurrentHashMap<>();

	/**
	 * Register a stream so later polls can find it, and sweep any abandoned ones.
	 *
	 * @param stream the stream to hold
	 */
	public static void register(AbstractOpenAISseStream stream) {
		sweepStale();
		STREAMS.put(stream.getJobId(), stream);
	}

	/**
	 * Look up a stream to keep draining.
	 *
	 * @param jobId the job being drained
	 * @return the stream for that job, or null when it is unknown or already
	 *         finished
	 */
	public static AbstractOpenAISseStream get(String jobId) {
		return jobId == null ? null : STREAMS.get(jobId);
	}

	/**
	 * Drop a finished stream and release its job.
	 *
	 * @param jobId the job being drained
	 */
	public static void remove(String jobId) {
		AbstractOpenAISseStream stream = STREAMS.remove(jobId);
		if (stream != null) {
			stream.close();
		}
	}

	/**
	 * Close every stream nobody has drained in a while.
	 */
	private static void sweepStale() {
		long cutoff = System.currentTimeMillis() - STALE_AFTER_MS;
		Iterator<Map.Entry<String, AbstractOpenAISseStream>> entries = STREAMS.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry<String, AbstractOpenAISseStream> entry = entries.next();
			AbstractOpenAISseStream stream = entry.getValue();
			if (stream.getLastDrained() < cutoff) {
				classLogger.warn("Closing the abandoned openai stream for job '{}'", entry.getKey());
				entries.remove();
				stream.close();
			}
		}
	}

	private OpenAIStreamRegistry() {
	}
}
