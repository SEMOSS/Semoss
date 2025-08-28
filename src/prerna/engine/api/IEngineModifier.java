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
package prerna.engine.api;

public interface IEngineModifier {

  /**
   * Set the engine for the modifier class
   *
   * @param engine
   */
  void setEngine(IDatabaseEngine engine);

  /**
   * Add a property to an existing concept
   *
   * @param existingConcept
   * @param newColumn
   * @param dataType
   */
  void addProperty(String existingConcept, String newColumn, String dataType) throws Exception;

  /**
   * Remove a property on an existing concept
   *
   * @param existingConcept
   * @param existingColumn
   * @throws Exception
   */
  void removeProperty(String existingConcept, String existingColumn) throws Exception;

  /**
   * Renames a property for an existing concept
   *
   * @param existingConcept
   * @param existingColumn
   * @param newColumn
   * @throws Exception
   */
  void renameProperty(String existingConcept, String existingColumn, String newColumn)
      throws Exception;

  /**
   * Edit the data type of an existing property
   *
   * @param existingConcept
   * @param existingColumn
   * @param newDataType
   * @throws Exception
   */
  void editProperty(String existingConcept, String existingColumn, String newDataType)
      throws Exception;

  /**
   * Add an index to a specific concept/column
   *
   * @param existingConcept
   * @param existingColumn
   * @param indexName
   * @param addIfExists
   * @throws Exception
   */
  void addIndex(
      String existingConcept, String existingColumn, String indexName, boolean addIfExists)
      throws Exception;

  /**
   * Renames an existing concept
   *
   * @param existingConcept
   * @param newConcept
   * @throws Exception
   */
  void renameConcept(String existingConcept, String newConcept) throws Exception;
}
