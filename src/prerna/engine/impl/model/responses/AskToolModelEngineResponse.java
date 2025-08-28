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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.util.Constants;

public class AskToolModelEngineResponse extends AskModelEngineResponse<List<Map<String, Object>>> {

  private static final Logger classLogger = LogManager.getLogger(AskToolModelEngineResponse.class);

  private static final long serialVersionUID = 1L;
  private static final String ID_KEY = "id";
  private static final String NAME_KEY = "name";
  private static final String TYPE_KEY = "type";
  private static final String ARGUMENTS_KEY = "arguments";
  List<Map<String, Object>> toolResponse;
  private List<ToolResponse> tools;

  /**
   * @param response
   * @param numberOfTokensInPrompt
   * @param numberOfTokensInResponse
   */
  public AskToolModelEngineResponse(
      List<Map<String, Object>> response,
      Integer numberOfTokensInPrompt,
      Integer numberOfTokensInResponse) {
    super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    this.toolResponse = response;
    this.tools = new ArrayList<>();
    for (Map<String, Object> toolResponse : response) {
      String id = null;
      String type = null;
      String name = null;
      Map<String, Object> arguments = null;

      if (toolResponse.containsKey(ID_KEY) && toolResponse.get(ID_KEY) instanceof String) {
        id = (String) toolResponse.get(ID_KEY);
      }

      if (toolResponse.containsKey(TYPE_KEY) && toolResponse.get(TYPE_KEY) instanceof String) {
        type = (String) toolResponse.get(TYPE_KEY);
      }

      if (toolResponse.containsKey(NAME_KEY) && toolResponse.get(NAME_KEY) instanceof String) {
        name = (String) toolResponse.get(NAME_KEY);
      }

      if (toolResponse.containsKey(ARGUMENTS_KEY)
          && toolResponse.get(ARGUMENTS_KEY) instanceof String) {
        String argumentsJson = (String) toolResponse.get(ARGUMENTS_KEY);
        try {
          arguments =
              new GsonBuilder()
                  .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
                  .disableHtmlEscaping()
                  .create()
                  .fromJson(argumentsJson, Map.class);
        } catch (Exception e) {
          classLogger.error(Constants.STACKTRACE, e);
        }
      }

      ToolResponse tool = new ToolResponse(id, type, name, arguments);
      this.tools.add(tool);
    }

    this.messageType = TOOL;
  }

  @Deprecated
  public String getToolCallId() {
    return this.tools.get(0).getId();
  }

  @Deprecated
  public String getToolCallArgumentsAsString() {
    Map<String, Object> arguments = this.tools.get(0).getArguments();
    if (arguments == null) {
      return "{}";
    }
    return new Gson().toJson(arguments);
  }

  @Deprecated
  public String getToolCallName() {
    return this.tools.get(0).getName();
  }

  @Override
  public String getStringResponse() {
    if (this.response != null) {
      return new Gson().toJson(this.response);
    }
    return "[]";
  }

  /**
   * @return
   */
  public List<ToolResponse> getTools() {
    return tools;
  }

  /**
   * @return
   */
  public List<Map<String, Object>> getToolResponse() {
    return toolResponse;
  }

  /** */
  public class ToolResponse {

    private String id;
    private String type;
    private String name;
    private Map<String, Object> arguments;

    public ToolResponse(String id, String type, String name, Map<String, Object> arguments) {
      this.id = id;
      this.type = type;
      this.name = name;
      this.arguments = arguments;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public Map<String, Object> getArguments() {
      return arguments;
    }

    //		public void setId(String id) {
    //			this.id = id;
    //		}
    //
    //		public void setName(String name) {
    //			this.name = name;
    //		}
    //
    //		public void setType(String type) {
    //			this.type = type;
    //		}
    //
    //		public void setArguments(Map<String, Object> arguments) {
    //			this.arguments = arguments;
    //		}
  }
}
