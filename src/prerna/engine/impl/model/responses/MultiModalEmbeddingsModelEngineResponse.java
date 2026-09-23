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
package prerna.engine.impl.model.responses;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response object for a multi modal embeddings call. Unlike
 * {@link EmbeddingsModelEngineResponse}, the embeddings are broken out by
 * modality ({@code text}, {@code image}, {@code video}) and each entry is kept
 * at its original input index, so a per-input error lines up with the input
 * that produced it.
 *
 * The object also represents the two non-embedding outcomes the Python layer
 * can return: an engine that does not implement multi modal embeddings
 * ({@code implemented == false}), and a hard model/API failure (an error map is
 * carried through verbatim so callers still see the message and traceback).
 */
public class MultiModalEmbeddingsModelEngineResponse implements Serializable {

	private static final long serialVersionUID = 6903489547668401277L;

	public static final String TEXT = "text";
	public static final String IMAGE = "image";
	public static final String VIDEO = "video";
	public static final String NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt";
	public static final String METADATA = "metadata";
	public static final String IMPLEMENTED = "implemented";
	public static final String RESPONSE = "response";
	public static final String MESSAGE_TYPE = "messageType";
	public static final String ERROR_MESSAGE_TYPE = "ERROR";

	// per-item keys
	public static final String POSITION = "position";
	public static final String EMBEDDING = "embedding";
	public static final String ERROR = "error";
	public static final String TRUNCATED = "truncated";

	/**
	 * A single per-input embedding result kept at its original input index. On
	 * success {@code embedding} is populated; when the input failed {@code error}
	 * is populated instead.
	 */
	public static class ModalityEmbedding implements Serializable {

		private static final long serialVersionUID = 8874556231455129601L;

		private int position;
		private List<Double> embedding;
		private String error;
		private Boolean truncated;

		public ModalityEmbedding() {
		}

		public int getPosition() {
			return position;
		}

		public void setPosition(int position) {
			this.position = position;
		}

		public List<Double> getEmbedding() {
			return embedding;
		}

		public void setEmbedding(List<Double> embedding) {
			this.embedding = embedding;
		}

		public String getError() {
			return error;
		}

		public void setError(String error) {
			this.error = error;
		}

		public Boolean getTruncated() {
			return truncated;
		}

		public void setTruncated(Boolean truncated) {
			this.truncated = truncated;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> map = new HashMap<>();
			map.put(POSITION, this.position);
			map.put(EMBEDDING, this.embedding);
			map.put(ERROR, this.error);
			map.put(TRUNCATED, this.truncated);
			return map;
		}

		@SuppressWarnings("unchecked")
		public static ModalityEmbedding fromMap(Map<String, Object> map) {
			ModalityEmbedding item = new ModalityEmbedding();
			Object positionObj = map.get(POSITION);
			if (positionObj instanceof Number) {
				item.position = ((Number) positionObj).intValue();
			}
			item.embedding = toDoubleList(map.get(EMBEDDING));
			Object errorObj = map.get(ERROR);
			item.error = errorObj != null ? errorObj.toString() : null;
			Object truncatedObj = map.get(TRUNCATED);
			if (truncatedObj instanceof Boolean) {
				item.truncated = (Boolean) truncatedObj;
			}
			return item;
		}

		private static List<Double> toDoubleList(Object valuesObj) {
			if (!(valuesObj instanceof List)) {
				return null;
			}
			List<?> raw = (List<?>) valuesObj;
			List<Double> values = new ArrayList<>(raw.size());
			for (Object v : raw) {
				if (v instanceof Number) {
					values.add(((Number) v).doubleValue());
				}
			}
			return values;
		}
	}

	private List<ModalityEmbedding> text = new ArrayList<>();
	private List<ModalityEmbedding> image = new ArrayList<>();
	private List<ModalityEmbedding> video = new ArrayList<>();
	private Integer numberOfTokensInPrompt = 0;
	private Map<String, Object> metadata;

	private boolean implemented = true;
	private String message;
	private Map<String, Object> errorDetails;

	public MultiModalEmbeddingsModelEngineResponse() {
	}

	public List<ModalityEmbedding> getText() {
		return text;
	}

	public void setText(List<ModalityEmbedding> text) {
		this.text = text;
	}

	public List<ModalityEmbedding> getImage() {
		return image;
	}

	public void setImage(List<ModalityEmbedding> image) {
		this.image = image;
	}

	public List<ModalityEmbedding> getVideo() {
		return video;
	}

	public void setVideo(List<ModalityEmbedding> video) {
		this.video = video;
	}

	public Integer getNumberOfTokensInPrompt() {
		return numberOfTokensInPrompt;
	}

	public void setNumberOfTokensInPrompt(Integer numberOfTokensInPrompt) {
		this.numberOfTokensInPrompt = numberOfTokensInPrompt;
	}

	public Map<String, Object> getMetadata() {
		return metadata;
	}

	public void setMetadata(Map<String, Object> metadata) {
		this.metadata = metadata;
	}

	public boolean isImplemented() {
		return implemented;
	}

	public void setImplemented(boolean implemented) {
		this.implemented = implemented;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Map<String, Object> getErrorDetails() {
		return errorDetails;
	}

	public void setErrorDetails(Map<String, Object> errorDetails) {
		this.errorDetails = errorDetails;
	}

	public Map<String, Object> toMap() {
		if (this.errorDetails != null) {
			return this.errorDetails;
		}
		if (!this.implemented) {
			Map<String, Object> map = new HashMap<>();
			map.put(RESPONSE, this.message);
			map.put(IMPLEMENTED, false);
			return map;
		}
		Map<String, Object> map = new HashMap<>();
		map.put(TEXT, itemsToMaps(this.text));
		map.put(IMAGE, itemsToMaps(this.image));
		map.put(VIDEO, itemsToMaps(this.video));
		map.put(NUMBER_OF_TOKENS_IN_PROMPT, this.numberOfTokensInPrompt);
		if (this.metadata != null && !this.metadata.isEmpty()) {
			map.put(METADATA, this.metadata);
		}
		return map;
	}

	private static List<Map<String, Object>> itemsToMaps(List<ModalityEmbedding> items) {
		List<Map<String, Object>> maps = new ArrayList<>();
		if (items != null) {
			for (ModalityEmbedding item : items) {
				maps.add(item.toMap());
			}
		}
		return maps;
	}

	public static MultiModalEmbeddingsModelEngineResponse notImplemented(String message) {
		MultiModalEmbeddingsModelEngineResponse response = new MultiModalEmbeddingsModelEngineResponse();
		response.implemented = false;
		response.message = message;
		return response;
	}

	@SuppressWarnings("unchecked")
	public static MultiModalEmbeddingsModelEngineResponse fromObject(Object responseObject) {
		if (!(responseObject instanceof Map)) {
			throw new IllegalArgumentException("Expected map output. Instead received value: " + responseObject);
		}
		return fromMap((Map<String, Object>) responseObject);
	}

	@SuppressWarnings("unchecked")
	public static MultiModalEmbeddingsModelEngineResponse fromMap(Map<String, Object> modelResponse) {
		MultiModalEmbeddingsModelEngineResponse response = new MultiModalEmbeddingsModelEngineResponse();

		if (ERROR_MESSAGE_TYPE.equals(modelResponse.get(MESSAGE_TYPE))) {
			response.errorDetails = modelResponse;
			return response;
		}

		boolean hasModality = modelResponse.containsKey(TEXT) || modelResponse.containsKey(IMAGE)
				|| modelResponse.containsKey(VIDEO);
		if (Boolean.FALSE.equals(modelResponse.get(IMPLEMENTED)) || !hasModality) {
			response.implemented = false;
			Object messageObj = modelResponse.get(RESPONSE);
			response.message = messageObj != null ? messageObj.toString()
					: "This model does not support multi modal embeddings.";
			return response;
		}

		response.text = parseItems(modelResponse.get(TEXT));
		response.image = parseItems(modelResponse.get(IMAGE));
		response.video = parseItems(modelResponse.get(VIDEO));
		response.numberOfTokensInPrompt = AbstractModelEngineResponse
				.getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
		Object metadataObj = modelResponse.get(METADATA);
		if (metadataObj instanceof Map) {
			response.metadata = (Map<String, Object>) metadataObj;
		}
		return response;
	}

	@SuppressWarnings("unchecked")
	private static List<ModalityEmbedding> parseItems(Object itemsObj) {
		List<ModalityEmbedding> items = new ArrayList<>();
		if (itemsObj instanceof List) {
			for (Object itemObj : (List<Object>) itemsObj) {
				if (itemObj instanceof Map) {
					items.add(ModalityEmbedding.fromMap((Map<String, Object>) itemObj));
				}
			}
		}
		return items;
	}
}
