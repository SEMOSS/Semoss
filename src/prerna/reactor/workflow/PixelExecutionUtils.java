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
package prerna.reactor.workflow;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

/**
 * Shared utility for executing Pixel expressions and returning clean, serializable results.
 *
 * <p>Handles the common pitfalls of raw {@code insight.runPixel()} calls:
 * <ul>
 *   <li>{@link ITask} materialization — query reactors return lazy cursors that must be collected</li>
 *   <li>Timeout enforcement — prevents hung queries from blocking pipelines indefinitely</li>
 *   <li>Error extraction — detects {@link PixelOperationType#ERROR} in results</li>
 *   <li>Null-safe return — always returns a usable value</li>
 * </ul>
 *
 * <p>Used by {@link TriggerWorkflowReactor} and potentially by the notebook executor.
 */
public final class PixelExecutionUtils {

	private static final Logger classLogger = LogManager.getLogger(PixelExecutionUtils.class);
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	private PixelExecutionUtils() {
		// static utility — no instantiation
	}

	/**
	 * Execute a pixel expression and return a clean, serializable result.
	 *
	 * <p>This method:
	 * <ol>
	 *   <li>Runs the pixel with an optional timeout</li>
	 *   <li>Checks for ERROR in the pixel operation type</li>
	 *   <li>If the result is an {@link ITask}, materializes it via {@code collect(false)}</li>
	 *   <li>Extracts the raw value from {@link NounMetadata}</li>
	 * </ol>
	 *
	 * @param insight        the execution context (carries user, varstore, etc.)
	 * @param pixel          the fully resolved pixel string to execute
	 * @param timeoutSeconds max execution time; 0 or negative means no timeout
	 * @return the pixel result as a serializable object (Map, List, String, Number, etc.), or null
	 * @throws WorkflowNodeTimeoutException if execution exceeds the timeout
	 * @throws WorkflowPixelException if the pixel returns an error result
	 * @throws IllegalStateException if an unexpected execution error occurs
	 */
	public static Object runAndCollect(Insight insight, String pixel, int timeoutSeconds) {
		if (pixel == null || pixel.isBlank()) {
			return null;
		}

		classLogger.debug("Executing pixel (timeout={}s): {}", timeoutSeconds,
				pixel.length() > 200 ? pixel.substring(0, 200) + "..." : pixel);

		NounMetadata result;

		if (timeoutSeconds > 0) {
			result = executeWithTimeout(insight, pixel, timeoutSeconds);
		} else {
			result = executeDirectly(insight, pixel);
		}

		if (result == null) {
			return null;
		}

		// Check for error in the pixel result
		checkForError(result, pixel);

		// Extract and materialize the value
		return materializeValue(result);
	}

	/**
	 * Overload with default timeout from {@link WorkflowConstants#DEFAULT_TIMEOUT_SECONDS}.
	 */
	public static Object runAndCollect(Insight insight, String pixel) {
		return runAndCollect(insight, pixel, WorkflowConstants.DEFAULT_TIMEOUT_SECONDS);
	}

	/**
	 * Serialize a pixel result to a JSON string for DB storage.
	 *
	 * @param result the object returned by {@link #runAndCollect}
	 * @return JSON string, or empty string if result is null
	 */
	public static String serializeResult(Object result) {
		if (result == null) {
			return "";
		}
		if (result instanceof String) {
			return (String) result;
		}
		return GSON.toJson(result);
	}

	/**
	 * Generate a preview string (first N chars of serialized output) for quick UI display.
	 *
	 * @param serializedOutput the full serialized output
	 * @return truncated preview, or null if input is null
	 */
	public static String generatePreview(String serializedOutput) {
		if (serializedOutput == null) {
			return null;
		}
		int maxLength = WorkflowConstants.OUTPUT_PREVIEW_MAX_LENGTH;
		if (serializedOutput.length() <= maxLength) {
			return serializedOutput;
		}
		return serializedOutput.substring(0, maxLength);
	}

	// ── Private Implementation ────────────────────────────────────────────────────

	private static NounMetadata executeWithTimeout(Insight insight, String pixel, int timeoutSeconds) {
		ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "workflow-pixel-exec");
			t.setDaemon(true);
			return t;
		});

		try {
			Callable<NounMetadata> task = () -> executeDirectly(insight, pixel);
			Future<NounMetadata> future = executor.submit(task);

			try {
				return future.get(timeoutSeconds, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				future.cancel(true);
				throw new WorkflowNodeTimeoutException(pixel, timeoutSeconds);
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof RuntimeException) {
					throw (RuntimeException) cause;
				}
				throw new IllegalStateException("Pixel execution failed: " + cause.getMessage(), cause);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Pixel execution interrupted", e);
			}
		} finally {
			executor.shutdownNow();
		}
	}

	private static NounMetadata executeDirectly(Insight insight, String pixel) {
		List<NounMetadata> results = insight.runPixel(pixel).getResults();
		if (results == null || results.isEmpty()) {
			return null;
		}
		// For multi-statement pixels (e.g., "LoadApp(...); ListLocalFiles(...)"),
		// the meaningful result is the last one. Earlier statements are setup/context.
		return results.get(results.size() - 1);
	}

	private static void checkForError(NounMetadata result, String pixel) {
		if (result.getOpType() != null && result.getOpType().contains(PixelOperationType.ERROR)) {
			String errorMsg = result.getValue() != null ? result.getValue().toString() : "Unknown pixel error";
			throw new WorkflowPixelException(pixel, errorMsg);
		}
	}

	private static Object materializeValue(NounMetadata result) {
		Object value = result.getValue();
		if (value == null) {
			return null;
		}

		// SqlQuery and other query reactors return a lazy ITask cursor.
		// We must collect it to get materialized data.
		if (value instanceof ITask) {
			classLogger.debug("Materializing ITask result");
			try {
				return ((ITask) value).collect(false);
			} catch (Exception e) {
				classLogger.error("Failed to materialize ITask: {}", e.getMessage(), e);
				throw new IllegalStateException("Failed to materialize query result: " + e.getMessage(), e);
			}
		}

		return value;
	}

	// ── Exception Types ───────────────────────────────────────────────────────────

	/**
	 * Thrown when a pixel execution exceeds its configured timeout.
	 */
	public static class WorkflowNodeTimeoutException extends RuntimeException {
		private final int timeoutSeconds;

		public WorkflowNodeTimeoutException(String pixel, int timeoutSeconds) {
			super("Pixel execution timed out after " + timeoutSeconds + " seconds: " +
					(pixel.length() > 100 ? pixel.substring(0, 100) + "..." : pixel));
			this.timeoutSeconds = timeoutSeconds;
		}

		public int getTimeoutSeconds() {
			return timeoutSeconds;
		}
	}

	/**
	 * Thrown when a pixel returns an ERROR operation type.
	 */
	public static class WorkflowPixelException extends RuntimeException {
		private final String pixel;

		public WorkflowPixelException(String pixel, String errorMessage) {
			super(errorMessage);
			this.pixel = pixel;
		}

		public String getPixel() {
			return pixel;
		}
	}
}
