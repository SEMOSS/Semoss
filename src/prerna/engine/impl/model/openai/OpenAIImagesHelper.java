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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.impl.model.ModelPixelInvoker;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.responses.AskModelEngineResponse;

/**
 * Helper class for formatting image model responses into the OpenAI
 * {@code /v1/images/generations} wire format, both the single payload and the
 * partial-frame SSE conversation.
 */
public final class OpenAIImagesHelper {

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * Writes a single SSE event block: "event: <type>\ndata: <json>\n\n"
	 */
	private static void writeSSE(Writer w, String eventType, Map<String, Object> data) throws IOException {
		w.write("event: " + eventType + "\n");
		w.write("data: " + GSON.toJson(data) + "\n\n");
		w.flush();
	}

	/**
	 * Emits an {@code image_generation.partial_image} SSE event carrying a
	 * progressive base64 frame. Matches the wire shape the {@code openai} Python
	 * SDK reads: {@code b64_json}, {@code partial_image_index}, plus optional
	 * generation metadata.
	 */
	public static void writePartialImageEvent(Writer w, String b64Json, int partialIdx, String model, long createdAt,
			String outputFormat, String quality, String size) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("b64_json", b64Json);
		event.put("partial_image_index", partialIdx);
		if (model != null) {
			event.put("model", model);
		}
		event.put("created_at", createdAt);
		if (outputFormat != null) {
			event.put("output_format", outputFormat);
		}
		if (quality != null) {
			event.put("quality", quality);
		}
		if (size != null) {
			event.put("size", size);
		}
		writeSSE(w, "image_generation.partial_image", event);
	}

	/**
	 * Emits an {@code image_generation.completed} SSE event carrying the final
	 * base64 image and optional token usage.
	 */
	public static void writeCompletedEvent(Writer w, String b64Json, String model, long createdAt, String outputFormat,
			String quality, String size, Integer inputTokens, Integer outputTokens) throws IOException {
		Map<String, Object> event = new HashMap<>();
		event.put("b64_json", b64Json);
		if (model != null) {
			event.put("model", model);
		}
		event.put("created_at", createdAt);
		if (outputFormat != null) {
			event.put("output_format", outputFormat);
		}
		if (quality != null) {
			event.put("quality", quality);
		}
		if (size != null) {
			event.put("size", size);
		}
		if (inputTokens != null || outputTokens != null) {
			Map<String, Object> usage = new HashMap<>();
			if (inputTokens != null) {
				usage.put("input_tokens", inputTokens);
			}
			if (outputTokens != null) {
				usage.put("output_tokens", outputTokens);
			}
			if (inputTokens != null && outputTokens != null) {
				usage.put("total_tokens", inputTokens + outputTokens);
			}
			event.put("usage", usage);
		}
		writeSSE(w, "image_generation.completed", event);
	}

	/**
	 * Builds the non-streaming JSON response from a completed model response.
	 * Shape: {@code { "created": <ts>, "data": [{ "b64_json": "..." }] }} Falls
	 * back to {@code url} for URL-only images.
	 */
	public static Map<String, Object> buildNonStreamingResponse(long createdAt, AskModelEngineResponse<?> llmResponse) {
		List<Map<String, Object>> dataList = new ArrayList<>();
		for (MessagePart part : llmResponse.getParts()) {
			if (!(part instanceof MediaMessagePart)) {
				continue;
			}
			MediaMessagePart mediaPart = (MediaMessagePart) part;
			if (mediaPart.getMediaInfo() == null) {
				continue;
			}
			String b64 = mediaPart.getMediaInfo().getBase64Data();
			String url = mediaPart.getMediaInfo().getSourceUrl();
			Map<String, Object> item = new HashMap<>();
			if (b64 != null && !b64.isEmpty()) {
				item.put("b64_json", b64);
			} else if (url != null && !url.isEmpty()) {
				item.put("url", url);
			}
			if (!item.isEmpty()) {
				dataList.add(item);
			}
		}
		Map<String, Object> response = new HashMap<>();
		response.put("created", createdAt);
		response.put("data", dataList);
		return response;
	}

	/**
	 * Drain an already-dispatched image model job and write the
	 * {@code image_generation.partial_image} frames, the terminal
	 * {@code image_generation.completed} event and the {@code [DONE]} marker into
	 * {@code writer}, blocking until the model is done.
	 *
	 * <p>
	 * This is the driver for a caller holding an open http response. A caller that
	 * cannot hold the response open - the reactor serving python - drives the same
	 * {@link OpenAIImagesStream} itself, one drain per request, and gets identical
	 * bytes.
	 *
	 * <p>
	 * The job is cleared and removed before returning, so callers only own the
	 * writer. {@link IOException} is left to the caller: an http caller reads it as
	 * a possible client disconnect, an in-process caller as a real failure.
	 *
	 * @param engineId          the model engine id, echoed on every event
	 * @param creationTimestamp the epoch second stamped on every event
	 * @param jobId             the job id returned by
	 *                          {@code ModelPixelInvoker.startAsyncModelRequest}
	 * @param outputFormat      the requested output format, echoed when present
	 * @param quality           the requested quality, echoed when present
	 * @param size              the requested size, echoed when present
	 * @param writer            the sink for the SSE conversation
	 * @throws IOException if writing to {@code writer} fails
	 */
	public static void streamJobToWriter(String engineId, long creationTimestamp, String jobId, String outputFormat,
			String quality, String size, Writer writer) throws IOException {
		OpenAIImagesStream stream = new OpenAIImagesStream(engineId, creationTimestamp, jobId, outputFormat, quality,
				size);
		try {
			while (!stream.drain(writer)) {
				try {
					Thread.sleep(ModelPixelInvoker.STREAM_POLL_INTERVAL_MS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}
		} finally {
			stream.close();
		}
	}

	private OpenAIImagesHelper() {
	}
}
