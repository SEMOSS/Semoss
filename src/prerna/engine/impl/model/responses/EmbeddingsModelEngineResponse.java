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

import java.util.List;
import java.util.Map;

import org.json.JSONObject;

public class EmbeddingsModelEngineResponse extends AbstractModelEngineResponse<List<List<Double>>> {
	
	private static final long serialVersionUID = 4408956306133085964L;

	public static final String METADATA = "metadata";

	protected Map<String, Object> metadata;

	/**
	 *
	 * @param response
	 * @param numberOfTokensInPrompt
	 * @param numberOfTokensInResponse
	 */
	public EmbeddingsModelEngineResponse(List<List<Double>> response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

	public Map<String, Object> getMetadata() {
		return this.metadata;
	}

	public void setMetadata(Map<String, Object> metadata) {
		this.metadata = metadata;
	}

	@Override
	public Map<String, Object> toMap() {
		Map<String, Object> responseMap = super.toMap();
		if (this.metadata != null && !this.metadata.isEmpty()) {
			responseMap.put(METADATA, this.metadata);
		}
		return responseMap;
	}

	@SuppressWarnings("unchecked")
	public static EmbeddingsModelEngineResponse fromMap(Map<String, Object> modelResponse) {
		Object responseObj = modelResponse.get(RESPONSE);
		if(responseObj instanceof String) {
			throw new IllegalArgumentException((String) responseObj);
		}
		List<List<Double>> responseObject = (List<List<Double>>) modelResponse.get(RESPONSE);
        Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
        Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

        EmbeddingsModelEngineResponse embeddingsResponse = new EmbeddingsModelEngineResponse(responseObject, tokensInPrompt, tokensInResponse);

        Object metadataObj = modelResponse.get(METADATA);
        if (metadataObj instanceof Map) {
        	embeddingsResponse.setMetadata((Map<String, Object>) metadataObj);
        }

        return embeddingsResponse;
    }
	
	@SuppressWarnings("unchecked")
	public static EmbeddingsModelEngineResponse fromObject(Object responseObject) {
		if(!(responseObject instanceof Map)) {
			throw new IllegalArgumentException("Expected map output. Instead received value: " + responseObject);
		}
		Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
		return fromMap(modelResponse);
    }
	
	/**
	 * 
	 * @param jsonResponse
	 * @return
	 */
	public static EmbeddingsModelEngineResponse fromJson(JSONObject jsonResponse) {
	    if (jsonResponse == null) {
	        return null;
	    }
	    
	    List<List<Double>> embeddings = null;
	    Integer promptTokens = 0;
	    Integer responseTokens = 0;
	    
	    if (jsonResponse.has("output")) {
	        Object outputObj = jsonResponse.get("output");
	        
	        // Handle the case where output is a JSONArray containing a single JSON string
	        if (outputObj instanceof org.json.JSONArray) {
	            org.json.JSONArray outputArray = (org.json.JSONArray) outputObj;
	            if (outputArray.length() > 0) {
	                String jsonString = outputArray.getString(0);
	                try {
	                    JSONObject embeddingsJson = new JSONObject(jsonString);
	                    
	                    if (embeddingsJson.has("embeddings")) {
	                        org.json.JSONArray embeddingsArray = embeddingsJson.getJSONArray("embeddings");
	                        embeddings = new java.util.ArrayList<>();
	                        
	                        for (int i = 0; i < embeddingsArray.length(); i++) {
	                            org.json.JSONArray vectorArray = embeddingsArray.getJSONArray(i);
	                            List<Double> vector = new java.util.ArrayList<>();
	                            
	                            for (int j = 0; j < vectorArray.length(); j++) {
	                                vector.add(vectorArray.getDouble(j));
	                            }
	                            
	                            embeddings.add(vector);
	                        }
	                    }
	                } catch (Exception e) {
	                    throw new IllegalArgumentException("Failed to parse embeddings JSON: " + e.getMessage());
	                }
	            }
	        } else if (outputObj instanceof JSONObject) {
	            JSONObject embeddingsJson = (JSONObject) outputObj;
	            
	            if (embeddingsJson.has("embeddings")) {
	                org.json.JSONArray embeddingsArray = embeddingsJson.getJSONArray("embeddings");
	                embeddings = new java.util.ArrayList<>();
	                
	                for (int i = 0; i < embeddingsArray.length(); i++) {
	                    org.json.JSONArray vectorArray = embeddingsArray.getJSONArray(i);
	                    List<Double> vector = new java.util.ArrayList<>();
	                    
	                    for (int j = 0; j < vectorArray.length(); j++) {
	                        vector.add(vectorArray.getDouble(j));
	                    }
	                    
	                    embeddings.add(vector);
	                }
	            }
	        } else if (outputObj instanceof String) {
	            String jsonString = (String) outputObj;
	            try {
	                JSONObject embeddingsJson = new JSONObject(jsonString);
	                
	                if (embeddingsJson.has("embeddings")) {
	                    org.json.JSONArray embeddingsArray = embeddingsJson.getJSONArray("embeddings");
	                    embeddings = new java.util.ArrayList<>();
	                    
	                    for (int i = 0; i < embeddingsArray.length(); i++) {
	                        org.json.JSONArray vectorArray = embeddingsArray.getJSONArray(i);
	                        List<Double> vector = new java.util.ArrayList<>();
	                        
	                        for (int j = 0; j < vectorArray.length(); j++) {
	                            vector.add(vectorArray.getDouble(j));
	                        }
	                        
	                        embeddings.add(vector);
	                    }
	                }
	            } catch (Exception e) {
	                throw new IllegalArgumentException("Failed to parse embeddings JSON: " + e.getMessage());
	            }
	        }
	    }
	    
	    if (embeddings == null) {
	        embeddings = new java.util.ArrayList<>();
	    }
	    
	    if (jsonResponse.has("input_tokens")) {
	        promptTokens = jsonResponse.getInt("input_tokens");
	    }
	    
	    if (jsonResponse.has("output_tokens")) {
	        responseTokens = jsonResponse.getInt("output_tokens");
	    }
	    
	    return new EmbeddingsModelEngineResponse(embeddings, promptTokens, responseTokens);
	}
}
