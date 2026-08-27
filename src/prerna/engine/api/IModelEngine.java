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
package prerna.engine.api;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.BatchListResponse;
import prerna.engine.impl.model.responses.BatchResultsResponse;
import prerna.engine.impl.model.responses.BatchStatusResponse;
import prerna.engine.impl.model.responses.BatchSubmissionResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.MultiModalEmbeddingsModelEngineResponse;
import prerna.logging.IgnoreEngineLogging;
import prerna.om.Insight;

public interface IModelEngine extends IEngine {

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String MODEL_TYPE = "MODEL_TYPE";

	// main class that is responsible for controlling everything models
	// hosting modes - embedded, inference_engine, FastChat, OpenAI
	// start server
	// connect client
	// disconnect client
	// stop server ?

	// reactors
	// ModelDeployMatchFinder - finds it based on GPU memory etc.
	// StopModel

	/**
	 * Gets the type of the model inference engine. The model engine type is often
	 * used to determine what client to use while running questions
	 * 
	 * @return the type of the database
	 */
	@IgnoreEngineLogging
	ModelTypeEnum getModelType();

	/**
	 * Passes the string question along with other parameters such as context and
	 * temperature to the python client and
	 * 
	 * @param question   The question being asked to the LLM
	 * @param context    (Optional) The context passed in by the user
	 * @param insight    The insight from where the call is being made. The insight
	 *                   holds user credentials, project information and
	 *                   conversation history tied to the insightId
	 * @param parameters Additional parameters such as temperature, top_k,
	 *                   max_new_tokens etc
	 * @return creates a map response with the following keys - response : The
	 *         actual string response from the LLM/model - messageId : The unique
	 *         identifier of a message (the user's input and the model response) -
	 *         roomId: The insightId that the runPixel endpoint is being called from
	 */

	@Deprecated
	AskModelEngineResponse ask(String question, String context, Insight insight, Map<String, Object> parameters);

	/**
	 * Passes the string question along with other parameters such as context and
	 * temperature to the python client and
	 * 
	 * @param question   The question being asked to the LLM
	 * @param context    (Optional) The context passed in by the user
	 * @param room       The room from where the call is being made. The room holder
	 *                   the insight and user, along with other room properties like
	 *                   tools, vec dbs, system prompts.
	 * @param parameters Additional parameters such as temperature, top_k,
	 *                   max_new_tokens etc
	 * @return creates a map response with the following keys - response : The
	 *         actual string response from the LLM/model - messageId : The unique
	 *         identifier of a message (the user's input and the model response) -
	 *         roomId: The insightId that the runPixel endpoint is being called from
	 */
	AskModelEngineResponse askRoom(String question, Room room, AbstractMessage inputMessage,
			Map<String, Object> parameters);

	/**
	 * Validate that the given outbound messages only carry media this model's
	 * configured input modalities allow. Callers building a provider payload
	 * outside the askRoom flow (batch submission, routing) invoke this with the
	 * exact message list they are about to serialize.
	 *
	 * This is a no-op by default. Engines backed by MODELMETADATA input
	 * modalities throw IllegalArgumentException when a message carries media of
	 * a disallowed modality.
	 *
	 * @param outboundMessages the messages about to be sent to the provider
	 */
	default void validateInputModalities(List<AbstractMessage> outboundMessages) {
	}

	/**
	 * Passes a list of strings to the model client to be embedded. Each string in
	 * the {@code stringsToEmbed} will be returned as its own vector.
	 * 
	 * @param stringsToEmbed The string that needs to be encoded
	 * @param insight        The insight from where the call is being made. The
	 *                       insight holds user credentials, project information and
	 *                       conversation history tied to the insightId
	 * @param parameters     Additional parameters such as temperature, top_k,
	 *                       max_new_tokens etc
	 * @return A list of embeddings
	 */
	EmbeddingsModelEngineResponse embeddings(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters);

	/**
	 * Passes text, image, and/or video inputs to the model client to be embedded
	 * together. Unlike {@link #embeddings}, the result is broken out by modality so
	 * each returned embedding (and any per-input error) lines up with the input that
	 * produced it.
	 *
	 * This is an optional capability. Engines whose underlying client does not
	 * implement multi modal embeddings inherit this default, which reports that the
	 * operation is not implemented rather than throwing.
	 *
	 * @param text       Text string(s) to embed. May be null/empty.
	 * @param image      Image input(s) to embed - base64, data URL, or remote URL. May be null/empty.
	 * @param video      Video input(s) to embed - base64, data URL, or remote URL. May be null/empty.
	 * @param insight    The insight from where the call is being made.
	 * @param parameters Additional parameters passed through to the model client.
	 * @return The embeddings response broken out by modality.
	 */
	default MultiModalEmbeddingsModelEngineResponse multiModalEmbeddings(List<String> text, List<String> image,
			List<String> video, Insight insight, Map<String, Object> parameters) {
		return MultiModalEmbeddingsModelEngineResponse
				.notImplemented("This model does not support multi modal embeddings.");
	}

	/**
	 *
	 * @return
	 */
	@IgnoreEngineLogging
	boolean keepsConversationHistory();

	/**
	 *
	 * @return
	 */
	@IgnoreEngineLogging
	int getContextWindow();

	// ------------------------------------------------------------------
	// Batch model calls (native provider Batch APIs).
	//
	// These are optional capabilities. Engines that do not support a native
	// provider batch API inherit the defaults below, which report
	// supportsBatch()==false and throw on use. Submit -> provider batch id ->
	// poll status -> fetch results. There is no SEMOSS-side state: the provider
	// is the source of truth and the engine is the security boundary.
	// ------------------------------------------------------------------

	/**
	 * Whether this engine supports submitting native provider batch jobs.
	 */
	@IgnoreEngineLogging
	default boolean supportsBatch() {
		return false;
	}

	/**
	 * Submit a batch of requests to the provider's batch API.
	 *
	 * @param requests   each entry is {@code {custom_id, body}} where body is the
	 *                   provider-native per-request payload
	 * @param parameters optional submission params (e.g. completion_window, endpoint)
	 * @return the provider batch id and initial status
	 */
	default BatchSubmissionResponse submitBatch(List<Map<String, Object>> requests, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Batch model calls are not supported for this model");
	}

	/**
	 * Fetch the live status of a previously submitted batch.
	 */
	default BatchStatusResponse getBatchStatus(String providerBatchId, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Batch model calls are not supported for this model");
	}

	/**
	 * Fetch the per-request results of a batch (mapped back by custom_id).
	 */
	default BatchResultsResponse getBatchResults(String providerBatchId, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Batch model calls are not supported for this model");
	}

	/**
	 * List the batches visible to this engine's provider credentials.
	 */
	default BatchListResponse listBatches(Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Batch model calls are not supported for this model");
	}

	/**
	 * Best-effort cancel of a submitted batch.
	 */
	default BatchStatusResponse cancelBatch(String providerBatchId, Map<String, Object> parameters) {
		throw new UnsupportedOperationException("Batch model calls are not supported for this model");
	}

}