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
package prerna.ui.components.playsheets.datamakers;

import java.util.List;
import java.util.Map;

/**
 * This Interface defines responsibilities of a data maker Data makers are used to generate the data
 * necessary for a view Data makers are fed data maker components one by one and the maker performs
 * necessary actions to consume component
 *
 * @author bisutton
 */
@Deprecated
public interface IDataMaker {

  @Deprecated
  void processDataMakerComponent(DataMakerComponent component);

  @Deprecated
  void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms);

  @Deprecated
  void processPostTransformations(
      DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame);

  @Deprecated
  Map<String, Object> getDataMakerOutput(String... selectors);

  //	@Deprecated
  //	List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker...
  // dataMaker);
  //
  //	@Deprecated
  //	List<Object> getActionOutput();

  @Deprecated
  Map<String, String> getScriptReactors();

  /** Used to update the data id when data has changed within the frame */
  @Deprecated
  void updateDataId();

  /**
   * Returns the current data id
   *
   * @return
   */
  @Deprecated
  int getDataId();

  /** reset the dataId to be 0 */
  @Deprecated
  void resetDataId();

  /**
   * Sets the name of the user who created this instance of the data maker
   *
   * @param userId
   */
  @Deprecated
  void setUserId(String userId);

  /**
   * Returns the name of the user who created this instance of the data maker
   *
   * @return
   */
  @Deprecated
  String getUserId();

  /**
   * Returns the name of the data maker This name must match that which is defined within RDF_MAP
   *
   * @return
   */
  @Deprecated
  String getDataMakerName();
}
