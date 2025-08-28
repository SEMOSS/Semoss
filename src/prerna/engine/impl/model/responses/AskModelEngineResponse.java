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

public abstract class AskModelEngineResponse<T> extends AbstractModelEngineResponse<T> {

  private static final long serialVersionUID = 1L;

  public static final String MESSAGE_ID = "messageId";
  public static final String ROOM_ID = "roomId";
  public static final String MESSAGE_TYPE = "messageType";
  public static final String CHAT = "CHAT";
  public static final String TOOL = "TOOL";
  public static final String IMAGE = "IMAGE";
  public static final String TTS = "TTS";

  protected String messageId;
  protected String roomId;
  protected String messageType = CHAT;

  public AskModelEngineResponse(
      T response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
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

  public String getMessageType() {
    return this.messageType;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> responseMap = super.toMap();
    responseMap.put(MESSAGE_ID, this.messageId);
    responseMap.put(ROOM_ID, this.roomId);
    responseMap.put(MESSAGE_TYPE, this.messageType);
    return responseMap;
  }

  public abstract String getStringResponse();

  // Factory method to create the appropriate response type
  @SuppressWarnings("unchecked")
  public static AskModelEngineResponse<?> fromMap(Object responseObject) {
    Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
    Object response = modelResponse.get(RESPONSE);

    Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
    Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

    // Set default messageType
    String messageType = CHAT;

    // Check if MESSAGE_TYPE is present and valid
    Object messageTypeObject = modelResponse.get(MESSAGE_TYPE);
    if (messageTypeObject != null) {
      if (messageTypeObject instanceof String) {
        messageType = (String) messageTypeObject;
      } else {
        throw new IllegalArgumentException("MESSAGE_TYPE is not a String");
      }
    }

    // Adjust logic based on messageType
    if (TOOL.equals(messageType)) {
      if (response instanceof List) {
        // TODO: why are we grabbing only 1 tool???
        // TODO: why are we grabbing only 1 tool???
        // TODO: why are we grabbing only 1 tool???
        // TODO: why are we grabbing only 1 tool???
        // TODO: why are we grabbing only 1 tool???
        // TODO: why are we grabbing only 1 tool???
        List<?> responseList = (List<?>) response;
        if (!responseList.isEmpty()) {
          return new AskToolModelEngineResponse(
              (List<Map<String, Object>>) responseList, tokensInPrompt, tokensInResponse);
        } else {
          throw new IllegalArgumentException("Tool list is empty or not valid");
        }
      } else {
        throw new IllegalArgumentException("Expected a List response for Tool messageType");
      }
    } else if (CHAT.equals(messageType)) {
      if (response instanceof String) {
        return new AskStringModelEngineResponse(
            (String) response, tokensInPrompt, tokensInResponse);
      } else {
        throw new IllegalArgumentException("Expected a String response for Chat messageType");
      }
    } else if (IMAGE.equals(messageType)) {
      if (response instanceof List) {
        List<?> responseList = (List<?>) response;

        // Validate that all items in the list are strings (base64 or URLs)
        for (Object item : responseList) {
          if (!(item instanceof String)) {
            throw new IllegalArgumentException(
                "Expected List<String> for Image messageType, but found non-String item: "
                    + item.getClass().getSimpleName());
          }
        }

        // Cast to List<String> since we've validated all items are strings
        @SuppressWarnings("unchecked")
        List<String> imageList = (List<String>) responseList;

        // Use the OpenAI factory method
        return AskImageModelEngineResponse.getOpenAIImageResponse(
            imageList, tokensInPrompt, tokensInResponse);

      } else {
        throw new IllegalArgumentException(
            "Expected a List<String> response for Image messageType, but received: "
                + response.getClass().getSimpleName());
      }
    } else {
      throw new IllegalArgumentException("Unsupported message type: " + messageType);
    }
  }

  @SuppressWarnings("unchecked")
  public static AskModelEngineResponse fromObject(Object responseObject) {
    if (!(responseObject instanceof Map)) {
      throw new IllegalArgumentException(
          "Expected map output. Instead received value: " + responseObject);
    }
    Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
    return fromMap(modelResponse);
  }
}
