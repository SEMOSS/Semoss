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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class NerModelEngineResponse extends AbstractModelEngineResponse<Map<String, Object>> {

  private static final Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);
  private static final long serialVersionUID = 1L;

  public static final String MESSAGE_ID = "messageId";
  public static final String ROOM_ID = "roomId";

  private String messageId;
  private String roomId;

  /**
   * @param response
   * @param numberOfTokensInPrompt
   * @param numberOfTokensInResponse
   */
  public NerModelEngineResponse(
      Map<String, Object> response,
      Integer numberOfTokensInPrompt,
      Integer numberOfTokensInResponse) {
    super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public String getMessageId() {
    return this.messageId;
  }

  public void setRoomId(String roomId) {
    this.roomId = roomId;
  }

  public String getRoomId() {
    return this.roomId;
  }

  /**
   * @param response
   * @return
   */
  public static NerModelEngineResponse fromJson(JSONObject response) {
    Logger classLogger = LogManager.getLogger(NerModelEngineResponse.class);

    if (response != null) {
      Map<String, Object> responseMap = new HashMap<>();

      if (response.has("mask_values")) {
        JSONObject maskValues = response.getJSONObject("mask_values");
        Map<String, String> maskValuesMap = new HashMap<>();
        for (String key : maskValues.keySet()) {
          maskValuesMap.put(key, maskValues.getString(key));
        }
        responseMap.put("mask_values", maskValuesMap);
      }

      if (response.has("entities")) {
        JSONArray entitiesArray = response.getJSONArray("entities");
        List<String> entitiesList = new ArrayList<>();
        for (int i = 0; i < entitiesArray.length(); i++) {
          entitiesList.add(entitiesArray.getString(i));
        }
        responseMap.put("entities", entitiesList);
      }

      if (response.has("raw_output")) {
        JSONArray rawOutput = response.getJSONArray("raw_output");
        List<Map<String, Object>> rawOutputList = new ArrayList<>();
        for (int i = 0; i < rawOutput.length(); i++) {
          JSONObject entity = rawOutput.getJSONObject(i);
          Map<String, Object> entityMap = new HashMap<>();
          entityMap.put("start", entity.getInt("start"));
          entityMap.put("end", entity.getInt("end"));
          entityMap.put("text", entity.getString("text"));
          entityMap.put("label", entity.getString("label"));
          entityMap.put("score", entity.getDouble("score"));
          rawOutputList.add(entityMap);
        }
        responseMap.put("raw_output", rawOutputList);
      }

      if (response.has("output")) {
        responseMap.put("output", response.getString("output"));
      }

      if (response.has("input")) {
        responseMap.put("input", response.getString("input"));
      }

      if (response.has("status")) {
        responseMap.put("status", response.getString("status"));
      } else {
        responseMap.put("status", "success");
      }

      if (response.has("message")) {
        responseMap.put("message", response.getString("message"));
      } else {
        responseMap.put("message", "");
      }

      return new NerModelEngineResponse(responseMap, 0, 0);
    } else {
      classLogger.error("Null response from model request");
      Map<String, Object> errorMap = new HashMap<>();
      errorMap.put("status", "error");
      errorMap.put("message", "Null response from model request");

      return new NerModelEngineResponse(errorMap, 0, 0);
    }
  }
}
