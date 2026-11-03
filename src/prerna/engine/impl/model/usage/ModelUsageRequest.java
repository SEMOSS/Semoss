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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * What a call is about to ask for, so its ceiling can be worked out and
 * reserved before it starts.
 *
 * <p>
 * The prompt estimate reads the whole request, media included, so it is worked
 * out the first time it is asked for, which is only when a token or cost rule
 * applies to the call. The output maximum is always bounded: the caller's, else
 * the model's, else {@link #DEFAULT_MAX_OUTPUT_TOKENS}.
 * </p>
 */
public class ModelUsageRequest {

	/**
	 * The output maximum of a call whose caller names none on a model whose own is
	 * not known, such as a router model
	 */
	public static final long DEFAULT_MAX_OUTPUT_TOKENS = 8_192L;
	/**
	 * How long a call's reservation holds at most, such as the call's timeout, so a
	 * call lost with its server stops holding usage once it expires
	 */
	public static final Duration CALL_RESERVATION = Duration.ofMinutes(15);
	/** How long a batch's reservation holds at most, a provider's batch window */
	public static final Duration BATCH_RESERVATION = Duration.ofHours(25);
	/** What an image counts as in the prompt estimate */
	public static final long IMAGE_TOKENS = 3_000L;
	/** What a page of a PDF counts as in the prompt estimate */
	public static final long PDF_PAGE_TOKENS = 3_000L;
	/** What an audio clip counts as in the prompt estimate */
	public static final long AUDIO_TOKENS = 5_000L;
	/** What a video counts as in the prompt estimate */
	public static final long VIDEO_TOKENS = 20_000L;
	/** The least another embedded file counts as in the prompt estimate */
	public static final long FILE_TOKENS = 5_000L;

	private static final Gson GSON = new Gson();
	private static final long PDF_BYTES_PER_PAGE = 50_000L;
	/** What each payload counts for beyond its contents */
	private static final long PAYLOAD_OVERHEAD_TOKENS = 16L;
	/**
	 * A media field shorter than this is counted as text, being too short to hold a
	 * file
	 */
	private static final int MIN_BASE64_LENGTH = 256;
	private static final Pattern DATA_URL = Pattern.compile("data:([A-Za-z0-9.+/-]+);base64,([A-Za-z0-9+/=]+)");
	private static final Pattern BASE64 = Pattern.compile("[A-Za-z0-9+/=_\\-\\r\\n]+");
	private static final Set<String> MEDIA_KEYS = Set.of("base64Data", "base64", "data", "file_data", "bytes");
	private static final List<String> MIME_KEYS = List.of("mimeType", "mime_type", "media_type", "mediaType", "format");

	private final LongSupplier promptEstimate;
	private final long maxOutputTokens;
	private final boolean embeddings;
	private boolean router;
	private Duration reservation = CALL_RESERVATION;
	// the prompt estimate, worked out by promptEstimate on first use and kept
	private boolean estimated;
	private long promptEstimateTokens;

	/**
	 * Makes a request whose prompt estimate is already worked out.
	 *
	 * @param promptEstimateTokens a generous estimate of the prompt
	 * @param maxOutputTokens      the most output the call asks for: the caller's
	 *                             maximum, or else the model's; null when neither
	 *                             is known
	 * @param embeddings           whether the call embeds, so it has no output
	 */
	public ModelUsageRequest(long promptEstimateTokens, Long maxOutputTokens, boolean embeddings) {
		this(() -> promptEstimateTokens, maxOutputTokens, embeddings);
	}

	/**
	 * Makes a request whose prompt estimate is worked out the first time it is
	 * asked for. The output maximum is bounded here, and is 0 for embeddings.
	 *
	 * @param promptEstimate  works out a generous estimate of the prompt, once and
	 *                        only when it is needed
	 * @param maxOutputTokens the most output the call asks for: the caller's
	 *                        maximum, or else the model's; null when neither is
	 *                        known
	 * @param embeddings      whether the call embeds, so it has no output
	 */
	public ModelUsageRequest(LongSupplier promptEstimate, Long maxOutputTokens, boolean embeddings) {
		this.promptEstimate = promptEstimate == null ? () -> 0L : promptEstimate;
		// bounded here, so every call reserves for a known output maximum
		this.maxOutputTokens = embeddings ? 0 : boundedMaxOutput(maxOutputTokens);
		this.embeddings = embeddings;
	}

	/**
	 * Makes the request for an ask whose prompt estimate is already worked out.
	 *
	 * @param promptEstimateTokens a generous estimate of the prompt
	 * @param maxOutputTokens      the most output the call asks for, or null
	 * @return a request for an ask
	 */
	public static ModelUsageRequest ask(long promptEstimateTokens, Long maxOutputTokens) {
		return new ModelUsageRequest(promptEstimateTokens, maxOutputTokens, false);
	}

	/**
	 * Makes the request for an ask whose prompt estimate is worked out the first
	 * time it is asked for.
	 *
	 * @param promptEstimate  works out a generous estimate of the prompt when it is
	 *                        needed
	 * @param maxOutputTokens the most output the call asks for, or null
	 * @return a request for an ask
	 */
	public static ModelUsageRequest ask(LongSupplier promptEstimate, Long maxOutputTokens) {
		return new ModelUsageRequest(promptEstimate, maxOutputTokens, false);
	}

	/**
	 * Makes the request for embeddings whose estimate is already worked out.
	 *
	 * @param promptEstimateTokens a generous estimate of what is embedded
	 * @return a request for embeddings
	 */
	public static ModelUsageRequest embeddings(long promptEstimateTokens) {
		return new ModelUsageRequest(promptEstimateTokens, null, true);
	}

	/**
	 * Makes the request for embeddings whose estimate is worked out the first time
	 * it is asked for.
	 *
	 * @param promptEstimate works out a generous estimate of what is embedded when
	 *                       it is needed
	 * @return a request for embeddings
	 */
	public static ModelUsageRequest embeddings(LongSupplier promptEstimate) {
		return new ModelUsageRequest(promptEstimate, null, true);
	}

	/**
	 * Bounds an output maximum, so every call reserves for a known one; a maximum
	 * of 0 or less counts as none.
	 *
	 * @param maxOutputTokens an output maximum, or null when none is known
	 * @return the maximum, or {@link #DEFAULT_MAX_OUTPUT_TOKENS} when none is known
	 */
	public static long boundedMaxOutput(Long maxOutputTokens) {
		return maxOutputTokens == null || maxOutputTokens <= 0 ? DEFAULT_MAX_OUTPUT_TOKENS : maxOutputTokens;
	}

	/**
	 * Marks the call as a router model's own call. Its counting leaves out the
	 * all-models and platform counters and its team's total, since the model it
	 * routes to counts the call there.
	 *
	 * @return this request
	 */
	public ModelUsageRequest forRouter() {
		this.router = true;
		return this;
	}

	/**
	 * Sets how long the call's reservation holds, such as
	 * {@link #BATCH_RESERVATION} for a batch. A null, zero or negative duration
	 * leaves it as it is, {@link #CALL_RESERVATION} by default.
	 *
	 * @param reservation how long the reservation holds at most
	 * @return this request
	 */
	public ModelUsageRequest reservingFor(Duration reservation) {
		if (reservation != null && !reservation.isNegative() && !reservation.isZero()) {
			this.reservation = reservation;
		}
		return this;
	}

	/**
	 * A generous estimate of the tokens in what a call sends, made the way the
	 * Python clients estimate what their tokenizers cannot count: a token for every
	 * three bytes of UTF-8 text, which is above what tokenizers count for English,
	 * code and CJK text alike; {@link #IMAGE_TOKENS} for each image;
	 * {@link #PDF_PAGE_TOKENS} for each page of a PDF; and for other embedded
	 * files, the larger of {@link #FILE_TOKENS} and a token for every four bytes.
	 * Media is found as base64 data in data URLs and in fields such as
	 * {@code base64Data}, with its type from a field such as {@code mimeType} or
	 * from its first bytes.
	 *
	 * @param payloads what is sent, such as the message history and the tools;
	 *                 nulls are skipped, and anything that is not text is counted
	 *                 as JSON
	 * @return the estimate
	 */
	public static long estimateTokens(Object... payloads) {
		long tokens = 0;
		if (payloads == null) {
			return tokens;
		}
		for (Object payload : payloads) {
			if (payload == null) {
				continue;
			}
			JsonElement tree = treeOf(payload);
			tokens += (tree == null ? textTokens(String.valueOf(payload)) : treeTokens(tree)) + PAYLOAD_OVERHEAD_TOKENS;
		}
		return tokens;
	}

	private static JsonElement treeOf(Object payload) {
		try {
			if (payload instanceof String) {
				String text = ((String) payload).trim();
				if (!text.startsWith("[") && !text.startsWith("{")) {
					return new JsonPrimitive((String) payload);
				}
				return JsonParser.parseString(text);
			}
			return GSON.toJsonTree(payload);
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static long treeTokens(JsonElement element) {
		if (element == null || element.isJsonNull()) {
			return 0;
		}
		if (element.isJsonPrimitive()) {
			JsonPrimitive primitive = element.getAsJsonPrimitive();
			return primitive.isString() ? textTokens(primitive.getAsString()) : textTokens(primitive.toString());
		}
		long tokens = 0;
		if (element.isJsonArray()) {
			for (JsonElement child : element.getAsJsonArray()) {
				tokens += treeTokens(child);
			}
			return tokens;
		}
		JsonObject object = element.getAsJsonObject();
		String mimeType = mimeTypeOf(object);
		for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
			JsonElement value = entry.getValue();
			if (MEDIA_KEYS.contains(entry.getKey()) && value != null && value.isJsonPrimitive()
					&& value.getAsJsonPrimitive().isString() && isBase64(value.getAsString())) {
				tokens += mediaTokens(mimeType, value.getAsString());
			} else {
				tokens += textTokens(entry.getKey()) + treeTokens(value);
			}
		}
		return tokens;
	}

	/**
	 * Text, with any data URL in it counted as the media it holds.
	 */
	private static long textTokens(String text) {
		long tokens = 0;
		Matcher matcher = DATA_URL.matcher(text);
		StringBuilder rest = new StringBuilder();
		int last = 0;
		while (matcher.find()) {
			tokens += mediaTokens(matcher.group(1), matcher.group(2));
			rest.append(text, last, matcher.start());
			last = matcher.end();
		}
		// a token for every three bytes, rounded up
		if (last == 0) {
			return (text.getBytes(StandardCharsets.UTF_8).length + 2) / 3;
		}
		rest.append(text, last, text.length());
		return tokens + (rest.toString().getBytes(StandardCharsets.UTF_8).length + 2) / 3;
	}

	private static long mediaTokens(String mimeType, String base64) {
		// base64 holds three bytes in every four characters
		long bytes = (long) base64.length() * 3 / 4;
		String type = mimeType == null ? sniffMimeType(base64) : mimeType.trim().toLowerCase(Locale.ROOT);
		if (type == null) {
			type = "";
		}
		if (type.startsWith("image/")) {
			return IMAGE_TOKENS;
		}
		if (type.equals("application/pdf")) {
			return Math.max(1, (bytes + PDF_BYTES_PER_PAGE - 1) / PDF_BYTES_PER_PAGE) * PDF_PAGE_TOKENS;
		}
		if (type.startsWith("audio/")) {
			return AUDIO_TOKENS;
		}
		if (type.startsWith("video/")) {
			return VIDEO_TOKENS;
		}
		return Math.max(FILE_TOKENS, bytes / 4);
	}

	private static String mimeTypeOf(JsonObject object) {
		for (String key : MIME_KEYS) {
			JsonElement value = object.get(key);
			if (value != null && value.isJsonPrimitive() && value.getAsString().contains("/")) {
				return value.getAsString();
			}
		}
		return null;
	}

	/**
	 * The type from the base64 of a file's first bytes: PNG, JPEG, GIF and WebP
	 * images, and PDF.
	 */
	private static String sniffMimeType(String base64) {
		if (base64.startsWith("iVBOR") || base64.startsWith("/9j/") || base64.startsWith("R0lGOD")
				|| base64.startsWith("UklGR")) {
			return "image/*";
		}
		if (base64.startsWith("JVBER")) {
			return "application/pdf";
		}
		return null;
	}

	private static boolean isBase64(String value) {
		return value.length() >= MIN_BASE64_LENGTH && BASE64.matcher(value).matches();
	}

	/**
	 * The prompt estimate the call's reservation and cost estimate are worked out
	 * from, kept once it is worked out; a negative estimate counts as 0.
	 *
	 * @return a generous estimate of the prompt, worked out the first time it is
	 *         asked for
	 */
	public long getPromptEstimateTokens() {
		if (!estimated) {
			promptEstimateTokens = Math.max(0, promptEstimate.getAsLong());
			estimated = true;
		}
		return promptEstimateTokens;
	}

	/**
	 * The output maximum the call reserves for, which the Python call is also told
	 * in its ceiling.
	 *
	 * @return the most output the call asks for: the caller's maximum, else the
	 *         model's, else {@link #DEFAULT_MAX_OUTPUT_TOKENS}; 0 for embeddings
	 */
	public long getMaxOutputTokens() {
		return maxOutputTokens;
	}

	public boolean isEmbeddings() {
		return embeddings;
	}

	public boolean isRouter() {
		return router;
	}

	public Duration getReservation() {
		return reservation;
	}

	/**
	 * The estimate a call reserves for when it is under what is left. Since the
	 * output is bounded, no call reserves the whole of what is left.
	 *
	 * @return the most tokens the call can use, prompt and output
	 */
	public long estimatedTokens() {
		return getPromptEstimateTokens() + maxOutputTokens;
	}
}
