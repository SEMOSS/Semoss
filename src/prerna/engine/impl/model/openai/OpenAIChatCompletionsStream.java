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

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Assembles the {@code chat.completion.chunk} SSE conversation for one model
 * job: the content and tool deltas, the finish chunk, the optional usage chunk
 * and the terminal {@code [DONE]}.
 *
 * <p>
 * Whether the {@code role} has already been sent and the usage counts seen so
 * far are the state that spans chunks, so a poll driven caller picks up exactly
 * where the last request left off.
 */
public class OpenAIChatCompletionsStream extends AbstractOpenAISseStream {

	private final String messageId;

	private boolean started = false;

	// Token usage forwarded by Python via stream_type="usage".
	// Surfaced as a usage-only chunk before the terminal [DONE].
	// Names follow Anthropic/Responses-API spelling on the wire
	// from Python; we translate to Chat-Completions wire fields
	// (prompt/completion) inside writeFinishReason.
	private Integer capturedPromptTokens = null;
	private Integer capturedCompletionTokens = null;
	private Integer capturedCachedTokens = null;
	private Integer capturedReasoningTokens = null;

	/**
	 * @param engineId          the model engine id, echoed as the chunk's "model"
	 * @param messageId         the {@code chatcmpl-} id carried by every chunk
	 * @param creationTimestamp the epoch second stamped on every chunk
	 * @param jobId             the job id returned by
	 *                          {@code ModelPixelInvoker.startAsyncModelRequest}
	 */
	public OpenAIChatCompletionsStream(String engineId, String messageId, long creationTimestamp, String jobId) {
		super(engineId, jobId, creationTimestamp);
		this.messageId = messageId;
	}

	@Override
	protected boolean writeChunk(Map<String, Object> streamObj, Writer writer) throws IOException {
		String streamType = (String) streamObj.get("stream_type");
		@SuppressWarnings("unchecked")
		Map<String, Object> dataMap = (Map<String, Object>) streamObj.get("data");
		if ("usage".equalsIgnoreCase(streamType)) {
			captureUsage(dataMap);
			return false;
		}

		if (streamType.equalsIgnoreCase("content")) {
			if (dataMap.containsKey("finish_reason")) {
				String finishReason = (String) dataMap.get("finish_reason");
				// this is a map only on finish reason
				writeFinishReason(finishReason, writer);
				return true;
			}
			String newContent = (String) dataMap.get("content");
			if (newContent != null && !newContent.isEmpty()) {
				OpenAIChatCompletionsHelper.writeContentChunk(this.engineId, this.messageId, this.creationTimestamp,
						newContent, this.started, writer);
				this.started = true;
			}
			// TODO: handle "thinking" stream type from models like Gemini/Claude
			// that emit reasoning chunks separately. These would need to be
			// forwarded as content chunks since OpenAI chat completions format
			// has no dedicated thinking field.
			return false;
		}

		// assuming only other type is tool at the moment
		if (dataMap.containsKey("finish_reason")) {
			// send the finish chunk
			String finishReason = (String) dataMap.get("finish_reason");
			writeFinishReason(finishReason, writer);
			return true;
		}
		OpenAIChatCompletionsHelper.writeToolChunk(this.engineId, this.messageId, this.creationTimestamp, dataMap,
				this.started, writer);
		this.started = true;
		return false;
	}

	@Override
	protected void writeJobCompleted(Writer writer) throws IOException {
		if (this.started) {
			// send final chunk with empty delta && finish_reason="stop"
			writeFinishReason("stop", writer);
			return;
		}

		// we didn't start
		// and there is no output
		// lets check the result
		// ... most likely this is a tool output
		PixelRunner finalOutput = PixelJobManager.getManager().getOutput(this.jobId);
		NounMetadata finalNoun = finalOutput.getResults().get(0);
		Object finalObject = finalNoun.getValue();
		String messageType = null;
		Map<String, Object> resultOutput = null;
		if (finalObject instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> finalMap = (Map<String, Object>) finalObject;
			resultOutput = finalMap;
			messageType = (String) resultOutput.get("messageType");
		}

		// grab token usage if present
		if (resultOutput != null) {
			if (this.capturedPromptTokens == null) {
				this.capturedPromptTokens = toInteger(resultOutput.get("numberOfTokensInPrompt"));
			}
			if (this.capturedCompletionTokens == null) {
				this.capturedCompletionTokens = toInteger(resultOutput.get("numberOfTokensInResponse"));
			}
			if (this.capturedCachedTokens == null) {
				this.capturedCachedTokens = toInteger(resultOutput.get("numberOfCacheReadTokens"));
			}
			if (this.capturedReasoningTokens == null) {
				this.capturedReasoningTokens = toInteger(resultOutput.get("numberOfThinkingTokens"));
			}
		}

		if ("TOOL".equals(messageType)) {
			// this is a function call request that was not streamed
			// maybe the model doesn't support streaming of tools
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> response = (List<Map<String, Object>>) resultOutput.get("response");

			if (response != null && !response.isEmpty()) {
				OpenAIChatCompletionsHelper.writeFullToolResponseAsChunk(this.engineId, this.messageId,
						this.creationTimestamp, response, writer);
			}
			writeFinishReason("tool_calls", writer);
			return;
		}

		// Handle regular text response
		if (resultOutput != null) {
			String content = (String) resultOutput.get("response");
			if (content != null && !content.isEmpty()) {
				OpenAIChatCompletionsHelper.writeContentChunk(this.engineId, this.messageId, this.creationTimestamp,
						content, true, writer);
			}
		}

		// send final chunk with empty delta && finish_reason="stop"
		writeFinishReason("stop", writer);
	}

	private void writeFinishReason(String finishReason, Writer writer) throws IOException {
		OpenAIChatCompletionsHelper.writeFinishReason(this.engineId, this.messageId, this.creationTimestamp,
				finishReason, this.capturedPromptTokens, this.capturedCompletionTokens, this.capturedCachedTokens,
				this.capturedReasoningTokens, writer);
	}

	private void captureUsage(Map<String, Object> dataMap) {
		Object inT = dataMap.get("input_tokens");
		if (inT instanceof Number) {
			this.capturedPromptTokens = ((Number) inT).intValue();
		}
		Object outT = dataMap.get("output_tokens");
		if (outT instanceof Number) {
			this.capturedCompletionTokens = ((Number) outT).intValue();
		}
		Object crT = dataMap.get("cache_read_input_tokens");
		if (crT instanceof Number) {
			this.capturedCachedTokens = ((Number) crT).intValue();
		}
		Object rT = dataMap.get("reasoning_tokens");
		if (rT instanceof Number) {
			this.capturedReasoningTokens = ((Number) rT).intValue();
		}
	}

	/**
	 * Coerce a loosely-typed token count (Integer, Long, Double from JSON
	 * deserialization) into an Integer, or null if it is absent / not numeric.
	 */
	private static Integer toInteger(Object value) {
		return (value instanceof Number) ? ((Number) value).intValue() : null;
	}
}
