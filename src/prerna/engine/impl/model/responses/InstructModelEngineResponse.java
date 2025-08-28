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

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InstructModelEngineResponse
    extends AbstractModelEngineResponse<List<Map<String, String>>> {

  private static final Logger classLogger = LogManager.getLogger(InstructModelEngineResponse.class);
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
  public InstructModelEngineResponse(
      List<Map<String, String>> response,
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

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> responseMap = super.toMap();
    responseMap.put(MESSAGE_ID, this.messageId);
    responseMap.put(ROOM_ID, this.roomId);
    return responseMap;
  }

  public static InstructModelEngineResponse fromMap(Map<String, Object> modelResponse) {
    Object responseObject = modelResponse.get(RESPONSE);
    List<Map<String, String>> responseList = null;

    if (responseObject instanceof List) {
      responseList = (List<Map<String, String>>) responseObject;
    } else {
      throw new IllegalArgumentException("Invalid response type: " + responseObject.getClass());
    }

    Integer tokensInPrompt = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_PROMPT));
    Integer tokensInResponse = getTokens(modelResponse.get(NUMBER_OF_TOKENS_IN_RESPONSE));

    return new InstructModelEngineResponse(responseList, tokensInPrompt, tokensInResponse);
  }

  @SuppressWarnings("unchecked")
  public static InstructModelEngineResponse fromObject(Object responseObject) {
    if (!(responseObject instanceof Map)) {
      throw new IllegalArgumentException(
          "Expected map output. Instead received value: " + responseObject);
    }
    Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
    return fromMap(modelResponse);
  }
}
