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
package prerna.engine.impl.model;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobRunner;

/**
 * Central dispatch point for executing the {@code LLM(...)} pixel on behalf of
 * a caller that already holds an {@link Insight} and a {@link Room}.
 *
 * <p>
 * Every caller funnels through here so the server-side {@code LLMReactor} (room
 * / parent-room resolution, image copying, use_history defaulting, inference
 * logging, etc.) always runs. Callers must never call {@code room.ask(...)}
 * directly - any behavior change to how the model is invoked belongs here (or
 * in {@code LLMReactor}) so it applies to every code path in one place.
 *
 * <p>
 * The callers today are the provider-compatible web endpoints in Monolith
 * (OpenAI / Anthropic / Ollama), which wrap these methods with their servlet
 * concerns through {@code ModelPixelExecutor}, and
 * {@code OpenAIPassthroughReactor}, which serves the same protocol to python
 * processes over the insight socket instead of over http.
 */
public class ModelPixelInvoker {

	private static final Logger classLogger = LogManager.getLogger(ModelPixelInvoker.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * How long to wait between checks for new partial output while draining a
	 * streaming model job.
	 *
	 * <p>
	 * Every provider's streaming path polls {@link PixelJobManager} the same way,
	 * so they share this interval rather than each picking their own. It sets the
	 * latency floor: a token the model has already produced waits up to this long
	 * before it is written out, and dropping it costs a map lookup per check
	 * against an empty list.
	 */
	public static final long STREAM_POLL_INTERVAL_MS = 50L;

	/**
	 * Build the {@code LLM(...)} pixel string. This is the single source of truth
	 * for how callers invoke the model; {@code command} is {@code 'ignore'} because
	 * the full prompt and all parameters are carried in {@code paramValues}.
	 *
	 * @param engine  the model engine being invoked
	 * @param room    the room the message belongs to
	 * @param dataMap the parameter map (full_prompt, tools, temperature, etc.)
	 * @return the pixel expression to execute
	 */
	public static String buildModelPixel(IModelEngine engine, Room room, Map<String, Object> dataMap) {
		return "LLM(engine='" + engine.getEngineId() + "',roomId='" + room.getId() + "',command='ignore'"
				+ ",paramValues=[" + GSON.toJson(dataMap) + "]);";
	}

	/**
	 * Dispatch the model pixel asynchronously on a virtual thread and return the
	 * job id. Used by streaming callers, which then poll {@link PixelJobManager}
	 * for partial stream output.
	 *
	 * @param engine    the model engine being invoked
	 * @param insight   the insight context for the request
	 * @param room      the room the message belongs to
	 * @param dataMap   the parameter map carried in the pixel's paramValues
	 * @param sessionId the session id for the job
	 * @return the job id that can be polled on {@link PixelJobManager}
	 */
	public static String startAsyncModelRequest(IModelEngine engine, Insight insight, Room room,
			Map<String, Object> dataMap, String sessionId) {
		try {
			PixelJobManager manager = PixelJobManager.getManager();
			PixelJobRunner jobRunner = manager.makeJob(insight, sessionId, null);
			String jobId = jobRunner.getJobId();

			String modelPixel = buildModelPixel(engine, room, dataMap);
			classLogger.info("Dispatching async model pixel: {}", modelPixel);
			jobRunner.addPixel(modelPixel);
			Thread.ofVirtual().start(jobRunner);
			return jobId;
		} catch (Exception e) {
			classLogger.error("Failed to start async model request for engine '{}': {}",
					engine == null ? "unknown" : engine.getEngineId(), e.getMessage(), e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	/**
	 * Dispatch the model pixel synchronously on the current thread and return the
	 * model response. Used by non-streaming callers: this runs the exact same pixel
	 * as the streaming path, but simply waits for the final payload instead of
	 * polling for partial stream chunks.
	 *
	 * @param engine  the model engine being invoked
	 * @param insight the insight context for the request
	 * @param room    the room the message belongs to
	 * @param dataMap the parameter map carried in the pixel's paramValues
	 * @return the reconstructed model response
	 */
	public static AskModelEngineResponse<?> askModelSync(IModelEngine engine, Insight insight, Room room,
			Map<String, Object> dataMap) {
		String modelPixel = buildModelPixel(engine, room, dataMap);
		classLogger.info("Dispatching sync model pixel: {}", modelPixel);

		PixelRunner runner = insight.runPixel(modelPixel);
		if (runner.getResults() == null || runner.getResults().isEmpty()) {
			throw new IllegalStateException(
					"Model request returned no output for engine '" + engine.getEngineId() + "'");
		}

		Object payload = runner.getResults().get(0).getValue();
		AskModelEngineResponse<?> response = AskModelEngineResponse.fromObject(payload);

		// fromObject rebuilds the response from the pixel payload but does not carry
		// over messageId/roomId, which the response processors rely on - restore them.
		if (payload instanceof Map) {
			Map<?, ?> payloadMap = (Map<?, ?>) payload;
			Object messageId = payloadMap.get(AskModelEngineResponse.MESSAGE_ID);
			if (messageId != null) {
				response.setMessageId(messageId.toString());
			}
			Object roomId = payloadMap.get(AskModelEngineResponse.ROOM_ID);
			if (roomId != null) {
				response.setRoomId(roomId.toString());
			}
		}

		return response;
	}

	/**
	 * Seed the {@link ThreadStore} for the current thread so downstream pixel
	 * execution (and the {@code LLMReactor}) can resolve the insight, session, job
	 * and user without them being threaded through every call.
	 *
	 * @param insight   the insight context for the request
	 * @param sessionId the session id the request belongs to
	 * @param jobId     the job id assigned to this request
	 */
	public static void initializeThreadStore(Insight insight, String sessionId, String jobId) {
		ThreadStore.setInsightId(insight.getInsightId());
		ThreadStore.setSessionId(sessionId);
		ThreadStore.setJobId(jobId);
		ThreadStore.setUser(insight.getUser());
	}

	/**
	 * Private constructor - this class exposes only static helpers and must not be
	 * instantiated.
	 */
	private ModelPixelInvoker() {
	}
}
