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
package prerna.engine.impl.model.usage;

import java.time.Duration;
import java.time.ZonedDateTime;

import prerna.engine.impl.model.responses.AbstractModelEngineResponse;

/**
 * What one call used, as the provider reported it. Prompt tokens include cache
 * reads and writes, and response tokens include thinking, which is how the
 * OpenAI, Anthropic and Gemini clients report them. Audio tokens are the part
 * of the prompt or the response that was audio.
 */
public class ModelUsageReport {

	private final long promptTokens;
	private final long responseTokens;
	private final long cacheReadTokens;
	private final long cacheWriteTokens;
	private final long thinkingTokens;
	private final long responseTimeMillis;
	private long audioInputTokens;
	private long audioOutputTokens;
	private boolean embeddings;

	/**
	 * Records what one call used. A negative count is read as 0.
	 *
	 * @param promptTokens       prompt tokens, cache reads and writes included
	 * @param responseTokens     response tokens, thinking included
	 * @param cacheReadTokens    cache read tokens
	 * @param cacheWriteTokens   cache write tokens
	 * @param thinkingTokens     thinking tokens
	 * @param responseTimeMillis the time the model spent answering, in milliseconds
	 */
	public ModelUsageReport(long promptTokens, long responseTokens, long cacheReadTokens, long cacheWriteTokens,
			long thinkingTokens, long responseTimeMillis) {
		this.promptTokens = Math.max(0, promptTokens);
		this.responseTokens = Math.max(0, responseTokens);
		this.cacheReadTokens = Math.max(0, cacheReadTokens);
		this.cacheWriteTokens = Math.max(0, cacheWriteTokens);
		this.thinkingTokens = Math.max(0, thinkingTokens);
		this.responseTimeMillis = Math.max(0, responseTimeMillis);
	}

	/**
	 * Reads what a call used from the token counts on the model's response. A count
	 * the response does not have is read as 0.
	 *
	 * @param response           the model's response, or null when it reported
	 *                           nothing
	 * @param responseTimeMillis the time the model spent answering
	 * @return what the response says was used
	 */
	public static ModelUsageReport of(AbstractModelEngineResponse<?> response, long responseTimeMillis) {
		if (response == null) {
			return new ModelUsageReport(0, 0, 0, 0, 0, responseTimeMillis);
		}
		return new ModelUsageReport(value(response.getNumberOfTokensInPrompt()),
				value(response.getNumberOfTokensInResponse()), value(response.getNumberOfCacheReadTokens()),
				value(response.getNumberOfCacheCreationTokens()), value(response.getNumberOfThinkingTokens()),
				responseTimeMillis);
	}

	/**
	 * Reads what a call used from the model's response, timed from when the call
	 * started to when it ended.
	 *
	 * @param response   the model's response
	 * @param inputTime  when the call started
	 * @param outputTime when it ended
	 * @return what the response says was used, with the time between the two
	 */
	public static ModelUsageReport of(AbstractModelEngineResponse<?> response, ZonedDateTime inputTime,
			ZonedDateTime outputTime) {
		return of(response, millisBetween(inputTime, outputTime));
	}

	/**
	 * Works out how long a call took, for the compute time its rules count.
	 *
	 * @param inputTime  when a call started
	 * @param outputTime when it ended
	 * @return the milliseconds between them, or 0 when either is unknown
	 */
	public static long millisBetween(ZonedDateTime inputTime, ZonedDateTime outputTime) {
		if (inputTime == null || outputTime == null) {
			return 0;
		}
		return Math.max(0, Duration.between(inputTime, outputTime).toMillis());
	}

	/**
	 * Records how many of the prompt and response tokens were audio, which are
	 * priced at the audio rates when the price entry has them.
	 *
	 * @param audioInputTokens  the prompt tokens that were audio
	 * @param audioOutputTokens the response tokens that were audio
	 * @return this report
	 */
	public ModelUsageReport withAudio(long audioInputTokens, long audioOutputTokens) {
		this.audioInputTokens = Math.max(0, audioInputTokens);
		this.audioOutputTokens = Math.max(0, audioOutputTokens);
		return this;
	}

	/**
	 * Marks the report as an embeddings call, which is priced per input token.
	 *
	 * @return this report
	 */
	public ModelUsageReport asEmbeddings() {
		this.embeddings = true;
		return this;
	}

	/**
	 * Cache and thinking tokens are already inside the prompt and response tokens,
	 * so they are not added again.
	 *
	 * @return every token, prompt and response, as token rules count them
	 */
	public long totalTokens() {
		return promptTokens + responseTokens;
	}

	public long getPromptTokens() {
		return promptTokens;
	}

	public long getResponseTokens() {
		return responseTokens;
	}

	public long getCacheReadTokens() {
		return cacheReadTokens;
	}

	public long getCacheWriteTokens() {
		return cacheWriteTokens;
	}

	public long getThinkingTokens() {
		return thinkingTokens;
	}

	public long getResponseTimeMillis() {
		return responseTimeMillis;
	}

	public long getAudioInputTokens() {
		return audioInputTokens;
	}

	public long getAudioOutputTokens() {
		return audioOutputTokens;
	}

	public boolean isEmbeddings() {
		return embeddings;
	}

	private static long value(Integer tokens) {
		return tokens == null ? 0 : tokens.longValue();
	}
}
