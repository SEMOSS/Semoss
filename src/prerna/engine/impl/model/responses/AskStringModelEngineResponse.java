/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model.responses;

import org.json.JSONObject;

public class AskStringModelEngineResponse extends AskModelEngineResponse<String> {

  private static final long serialVersionUID = 1L;

  public AskStringModelEngineResponse(
      String response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
    super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
  }

  @Override
  public String getStringResponse() {
    return this.getResponse();
  }

  /**
   * Creates an AskModelEngineResponse from a JSONObject returned by the KServe adapter
   *
   * @param jsonResponse The JSONObject from makeModelRequest
   * @return AskModelEngineResponse constructed from the JSONObject
   */
  public static AskStringModelEngineResponse fromJson(JSONObject jsonResponse) {
    if (jsonResponse == null) {
      return null;
    }

    String responseText;
    if (jsonResponse.has("output")) {
      Object outputObj = jsonResponse.get("output");
      if (outputObj instanceof JSONObject || outputObj instanceof org.json.JSONArray) {
        responseText = outputObj.toString();
      } else {
        responseText = jsonResponse.getString("output");
      }
    } else {
      responseText = "";
    }

    Integer promptTokens = 0;
    Integer responseTokens = 0;

    if (jsonResponse.has("input_tokens")) {
      promptTokens = jsonResponse.getInt("input_tokens");
    }

    if (jsonResponse.has("output_tokens")) {
      responseTokens = jsonResponse.getInt("output_tokens");
    }

    AskStringModelEngineResponse response =
        new AskStringModelEngineResponse(responseText, promptTokens, responseTokens);

    return response;
  }

  // Additional methods specific to string responses can be added here
}
