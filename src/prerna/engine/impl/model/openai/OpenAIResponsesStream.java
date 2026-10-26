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
import java.util.HashMap;
import java.util.Map;

import com.github.f4b6a3.uuid.alt.GUID;

/**
 * Assembles the Responses API SSE conversation for one model job: the
 * {@code response.created} / {@code response.in_progress} handshake, the
 * message / function_call / image items with their deltas, and the terminal
 * {@code response.completed} carrying usage.
 *
 * <p>
 * Item ids and indices are tracked across chunks so the openai SDK sees each
 * item from added through done. That state is what makes this a stream object
 * rather than a set of static calls: a poll driven caller resumes mid item on
 * the next request, with the sequence number still counting up from where the
 * previous request stopped.
 */
public class OpenAIResponsesStream extends AbstractOpenAISseStream {

	private final String responseId;

	private int seq = 0;

	// --- STATE TRACKING ---
	// These persist across chunks
	private String currentItemId = null;
	private String currentItemType = null;
	private String currentToolName = null;
	private boolean isContentPartOpen = false;

	// Image-generation item id; non-null while partial frames are
	// streaming for the same image. Cleared after the completed/done
	// pair so a subsequent image opens a fresh item.
	private String imgItemId = null;

	private int outputIndex = 0;
	private int contentIndex = 0;
	private final StringBuilder currentAccumulator = new StringBuilder();

	// Token usage forwarded by Python via stream_type="usage".
	// Attached to response.completed.response.usage at end of stream.
	private Integer capturedInputTokens = null;
	private Integer capturedOutputTokens = null;
	private Integer capturedCachedTokens = null;
	private Integer capturedReasoningTokens = null;

	/**
	 * @param engineId          the model engine id, echoed as the response's
	 *                          "model"
	 * @param responseId        the {@code resp_} id carried by every event
	 * @param creationTimestamp the epoch second stamped on the response object
	 * @param jobId             the job id returned by
	 *                          {@code ModelPixelInvoker.startAsyncModelRequest}
	 */
	public OpenAIResponsesStream(String engineId, String responseId, long creationTimestamp, String jobId) {
		super(engineId, jobId, creationTimestamp);
		this.responseId = responseId;
	}

	@Override
	protected void writePrelude(Writer writer) throws IOException {
		OpenAIResponsesHelper.writeSSEEvent(OpenAIResponsesHelper.createBaseEvent("response.created", this.seq++,
				this.responseId, this.engineId, this.creationTimestamp), writer);
		OpenAIResponsesHelper.writeSSEEvent(OpenAIResponsesHelper.createBaseEvent("response.in_progress", this.seq++,
				this.responseId, this.engineId, this.creationTimestamp), writer);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean writeChunk(Map<String, Object> streamObj, Writer writer) throws IOException {
		String streamType = (String) streamObj.get("stream_type");
		Map<String, Object> streamData = (Map<String, Object>) streamObj.get("data");

		// Media (image) chunks. The Responses API protocol expects:
		// output_item.added (status=in_progress) - once
		// image_generation_call.partial_image - zero or more, all under
		// the same item_id, each carrying partial_image_b64 and an
		// incrementing partial_image_index
		// image_generation_call.completed - once (bare lifecycle event)
		// output_item.done (status=completed, item.result=<base64>) - once
		// We open the item lazily on the first media chunk and close it on
		// the final (partial_image_index == null), so the openai SDK sees
		// one image item from added -> done.
		if ("media".equalsIgnoreCase(streamType)) {
			closeCurrentItem(writer);

			Map<String, Object> mediaInfo = (Map<String, Object>) streamData.get("media_info");
			Object partialIdx = streamData.get("partial_image_index");

			if (this.imgItemId == null) {
				this.imgItemId = "img_" + GUID.v7().toUUID().toString();
				Map<String, Object> imgItem = new HashMap<>();
				imgItem.put("id", this.imgItemId);
				imgItem.put("type", "image_generation_call");
				imgItem.put("status", "in_progress");
				Map<String, Object> addedEvent = new HashMap<>();
				addedEvent.put("type", "response.output_item.added");
				addedEvent.put("sequence_number", this.seq++);
				addedEvent.put("response_id", this.responseId);
				addedEvent.put("output_index", this.outputIndex);
				addedEvent.put("item", imgItem);
				OpenAIResponsesHelper.writeSSEEvent(addedEvent, writer);
			}

			if (partialIdx != null) {
				OpenAIResponsesHelper.sendImageGenerationPartialImage(writer, this.seq++, this.responseId,
						this.imgItemId, this.outputIndex, mediaInfo, partialIdx);
			} else {
				OpenAIResponsesHelper.sendImageGenerationCompleted(writer, this.seq++, this.responseId, this.imgItemId,
						this.outputIndex);

				Map<String, Object> doneItem = new HashMap<>();
				doneItem.put("id", this.imgItemId);
				doneItem.put("type", "image_generation_call");
				doneItem.put("status", "completed");
				if (mediaInfo != null) {
					Object b64 = mediaInfo.get("base64Data");
					if (b64 instanceof String && !((String) b64).isEmpty()) {
						doneItem.put("result", b64);
					}
				}
				Map<String, Object> doneEvent = new HashMap<>();
				doneEvent.put("type", "response.output_item.done");
				doneEvent.put("sequence_number", this.seq++);
				doneEvent.put("response_id", this.responseId);
				doneEvent.put("output_index", this.outputIndex);
				doneEvent.put("item", doneItem);
				OpenAIResponsesHelper.writeSSEEvent(doneEvent, writer);

				this.outputIndex++;
				this.imgItemId = null;
			}

			return streamData.containsKey("finish_reason");
		}

		if ("usage".equalsIgnoreCase(streamType)) {
			captureUsage(streamData);
			return false;
		}

		boolean isChunkTool = "tool".equalsIgnoreCase(streamType) || "function_call".equalsIgnoreCase(streamType);
		String targetType = isChunkTool ? "function_call" : "message";

		String incomingId = (String) streamData.get("id");

		// --- TRANSITION LOGIC ---
		// Rule 1: Nothing started yett
		// Rule 2: Type switched (Thnking --->> Tool)
		// Rule 3: A NEW Tool ID appeared (Tool A -> Tool B)
		boolean shouldSwitch = (this.currentItemId == null) || (!targetType.equals(this.currentItemType))
				|| (isChunkTool && incomingId != null && !incomingId.equals(this.currentItemId));

		if (shouldSwitch) {
			// A. close previous item if it exists
			if (this.currentItemId != null) {
				if ("message".equals(this.currentItemType) && this.isContentPartOpen) {
					OpenAIResponsesHelper.sendTextDone(writer, this.seq++, this.responseId, this.currentItemId,
							this.outputIndex, this.contentIndex, this.currentAccumulator.toString());
					OpenAIResponsesHelper.sendContentPartDone(writer, this.seq++, this.responseId, this.currentItemId,
							this.outputIndex, this.contentIndex, this.currentAccumulator.toString());
					this.isContentPartOpen = false;
				}
				OpenAIResponsesHelper.sendItemDone(writer, this.seq++, this.responseId, this.currentItemId,
						this.outputIndex, this.currentItemType, this.currentAccumulator.toString(),
						this.currentToolName);

				this.outputIndex++;
				this.currentAccumulator.setLength(0);
			}

			// B. Setup NEW Item
			this.currentItemType = targetType;
			this.currentItemId = (incomingId != null) ? incomingId : "msg_" + GUID.v7().toUUID().toString();

			if (isChunkTool) {
				this.currentToolName = (String) streamData.get("name");

				if (this.currentToolName == null) {
					Map<String, Object> functionObj = (Map<String, Object>) streamData.get("function");
					if (functionObj != null) {
						this.currentToolName = (String) functionObj.get("name");
					}
				}

				if (this.currentToolName == null) {
					this.currentToolName = "shell";
				}
			}

			// C. Send ADDED event
			OpenAIResponsesHelper.sendItemAdded(writer, this.seq++, this.responseId, this.currentItemId,
					this.outputIndex, this.currentItemType, this.currentToolName);

			if ("message".equals(this.currentItemType)) {
				OpenAIResponsesHelper.sendContentPartAdded(writer, this.seq++, this.responseId, this.currentItemId,
						this.outputIndex, this.contentIndex);
				this.isContentPartOpen = true;
			}
		}

		// --- DELTA LOGIC: stream the actual data ---
		if ("message".equals(this.currentItemType)) {
			String content = (String) streamData.get("content");
			if (content != null && !content.isEmpty()) {
				this.currentAccumulator.append(content);
				OpenAIResponsesHelper.sendTextDelta(writer, this.seq++, this.responseId, this.currentItemId,
						this.outputIndex, this.contentIndex, content);
			}
		} else {
			Object argsObj = streamData.get("arguments");

			if (argsObj == null) {
				Map<String, Object> functionObj = (Map<String, Object>) streamData.get("function");
				if (functionObj != null) {
					argsObj = functionObj.get("arguments");

					if (this.currentToolName == null || "shell".equals(this.currentToolName)) {
						String nestedName = (String) functionObj.get("name");
						if (nestedName != null) {
							this.currentToolName = nestedName;
						}
					}
				}
			}

			if (argsObj == null) {
				argsObj = streamData.get("content");
			}

			if (argsObj != null) {
				String argsString = (argsObj instanceof String) ? (String) argsObj
						: OpenAIResponsesHelper.toJson(argsObj);
				if (!argsString.isEmpty()) {
					this.currentAccumulator.append(argsString);
					OpenAIResponsesHelper.sendToolDelta(writer, this.seq++, this.responseId, this.currentItemId,
							this.outputIndex, argsString);
				}
			}
		}

		return streamData.containsKey("finish_reason");
	}

	@Override
	protected void writeEpilogue(Writer writer) throws IOException {
		if (this.currentItemId != null) {
			if ("message".equals(this.currentItemType) && this.isContentPartOpen) {
				OpenAIResponsesHelper.sendTextDone(writer, this.seq++, this.responseId, this.currentItemId,
						this.outputIndex, this.contentIndex, this.currentAccumulator.toString());
				OpenAIResponsesHelper.sendContentPartDone(writer, this.seq++, this.responseId, this.currentItemId,
						this.outputIndex, this.contentIndex, this.currentAccumulator.toString());
			}
			OpenAIResponsesHelper.sendItemDone(writer, this.seq++, this.responseId, this.currentItemId,
					this.outputIndex, this.currentItemType, this.currentAccumulator.toString(), this.currentToolName);
		}

		Map<String, Object> completedEvent = OpenAIResponsesHelper.createBaseEvent("response.completed", this.seq++,
				this.responseId, this.engineId, this.creationTimestamp);
		// status lives on the nested response object, not the event root;
		// createBaseEvent seeds it as "in_progress" so flip it here
		Object completedRespObj = completedEvent.get("response");
		if (completedRespObj instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> completedResp = (Map<String, Object>) completedRespObj;
			completedResp.put("status", "completed");
		}
		OpenAIResponsesHelper.attachUsage(completedEvent, this.capturedInputTokens, this.capturedOutputTokens,
				this.capturedCachedTokens, this.capturedReasoningTokens);
		OpenAIResponsesHelper.writeSSEEvent(completedEvent, writer);
	}

	/**
	 * Close whatever item is open so a media item can start on a clean index.
	 */
	private void closeCurrentItem(Writer writer) throws IOException {
		if (this.currentItemId == null) {
			return;
		}
		if ("message".equals(this.currentItemType) && this.isContentPartOpen) {
			OpenAIResponsesHelper.sendTextDone(writer, this.seq++, this.responseId, this.currentItemId,
					this.outputIndex, this.contentIndex, this.currentAccumulator.toString());
			OpenAIResponsesHelper.sendContentPartDone(writer, this.seq++, this.responseId, this.currentItemId,
					this.outputIndex, this.contentIndex, this.currentAccumulator.toString());
			this.isContentPartOpen = false;
		}
		OpenAIResponsesHelper.sendItemDone(writer, this.seq++, this.responseId, this.currentItemId, this.outputIndex,
				this.currentItemType, this.currentAccumulator.toString(), this.currentToolName);
		this.outputIndex++;
		this.currentAccumulator.setLength(0);
		this.currentItemId = null;
		this.currentItemType = null;
		this.currentToolName = null;
	}

	private void captureUsage(Map<String, Object> streamData) {
		Object inT = streamData.get("input_tokens");
		if (inT instanceof Number) {
			this.capturedInputTokens = ((Number) inT).intValue();
		}
		Object outT = streamData.get("output_tokens");
		if (outT instanceof Number) {
			this.capturedOutputTokens = ((Number) outT).intValue();
		}
		Object crT = streamData.get("cache_read_input_tokens");
		if (crT instanceof Number) {
			this.capturedCachedTokens = ((Number) crT).intValue();
		}
		Object rT = streamData.get("reasoning_tokens");
		if (rT instanceof Number) {
			this.capturedReasoningTokens = ((Number) rT).intValue();
		}
	}
}
