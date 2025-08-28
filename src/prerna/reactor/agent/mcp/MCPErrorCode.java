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
package prerna.reactor.agent.mcp;

public enum MCPErrorCode {

  // Standard JSON-RPC 2.0 Error Codes
  PARSE_ERROR(-32700, "Parse error - Invalid JSON was received by the server"),
  INVALID_REQUEST(-32600, "Invalid Request - The JSON sent is not a valid Request object"),
  METHOD_NOT_FOUND(-32601, "Method not found - The method does not exist / is not available"),
  INVALID_PARAMS(-32602, "Invalid params - Invalid method parameter(s)"),
  INTERNAL_ERROR(-32603, "Internal error - Internal JSON-RPC error"),

  // MCP-Specific Error Codes (-32000 to -32099)
  SERVER_ERROR(-32000, "Server error - Generic server-side error"),
  RESOURCE_NOT_FOUND(-32001, "Resource not found - Requested resource doesn't exist"),
  RESOURCE_ACCESS_DENIED(-32002, "Resource access denied - Access to resource is forbidden"),
  RESOURCE_UNAVAILABLE(
      -32003, "Resource unavailable - Resource exists but is temporarily unavailable"),
  TOOL_EXECUTION_FAILED(-32004, "Tool execution failed - Tool ran but failed during execution"),
  INVALID_TOOL_RESULT(-32005, "Invalid tool result - Tool returned malformed or invalid result");

  private final int code;
  private final String description;

  MCPErrorCode(int code, String description) {
    this.code = code;
    this.description = description;
  }

  public int getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }

  public static MCPErrorCode fromCode(int code) {
    for (MCPErrorCode errorCode : values()) {
      if (errorCode.code == code) {
        return errorCode;
      }
    }
    throw new IllegalArgumentException("Unknown error code: " + code);
  }
}
