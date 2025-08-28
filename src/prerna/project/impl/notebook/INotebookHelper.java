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
package prerna.project.impl.notebook;

import com.google.gson.JsonElement;
import java.util.Map;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.sablecc2.NotebookExecution;

public interface INotebookHelper {

  String UNDEFINED_VALUE = "undefined";

  /**
   * @return
   */
  JsonElement getBlocksFileJson();

  /**
   * @param blocksFileJson
   */
  void setBlocksFileJson(JsonElement blocksFileJson);

  /**
   * @param insight
   * @param inputReplacements
   * @return
   */
  NotebookExecution executeNotebook(Insight insight, Map<String, String> inputReplacements);

  /**
   * Gets only engine deps listed in the blocks.json file in the project
   *
   * @return Map of the variable name to the engine id
   */
  Map<String, String> getBlocksEngineDependencies();

  /**
   * @return
   */
  Map<String, String> getNotebookVariables();

  /**
   * @param filePath
   * @param model
   * @param insight
   */
  void createMcpJson(String filePath, IModelEngine model, Insight insight);
}
