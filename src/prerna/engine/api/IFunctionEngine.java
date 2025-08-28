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
import prerna.engine.impl.function.FunctionParameter;

public interface IFunctionEngine extends IEngine {

  // this is what the FE sends for the type of storage we are creating
  // as a result, cannot be a key in the smss file
  String FUNCTION_TYPE = "FUNCTION_TYPE";

  String NAME_KEY = "FUNCTION_NAME";
  String DESCRIPTION_KEY = "FUNCTION_DESCRIPTION";
  String PARAMETER_KEY = "FUNCTION_PARAMETERS";
  String REQUIRED_PARAMETER_KEY = "FUNCTION_REQUIRED_PARAMETERS";
  String PYTHON_FILE_NAME = "PYTHON_FILE_NAME";

  /**
   * @param args
   * @return
   */
  Object execute(Map<String, Object> parameterValues);

  /**
   * Unique name of the function
   *
   * @return
   */
  String getFunctionName();

  /** */
  void setFunctionName(String functionName);

  /**
   * Description of what this function does
   *
   * @return
   */
  String getFunctionDescription();

  /**
   * @param description
   */
  void setFunctionDescription(String description);

  /**
   * @return
   */
  List<FunctionParameter> getParameters();

  /**
   * @param parameters
   */
  void setParameters(List<FunctionParameter> parameters);

  /**
   * @return
   */
  List<String> getRequiredParameters();

  /**
   * @param requiredParameters
   */
  void setRequiredParameters(List<String> requiredParameters);

  /**
   * @return
   */
  org.json.JSONObject getFunctionDefintionJson();

  /**
   * @return json representation of function
   */
  Map<String, Object> buildFunctionEngineToolMap();
}
