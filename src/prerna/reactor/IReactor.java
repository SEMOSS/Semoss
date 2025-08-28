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
package prerna.reactor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public interface IReactor {

  enum STATUS {
    STARTED,
    INPROGRESS,
    COMPLETED,
    FAILED
  };

  // is this a map or a reduce
  enum TYPE {
    MAP,
    FLATMAP,
    REDUCE
  };

  String SIBLING = "SIBLING";
  String PARENT = "PARENT";
  String CHILD = "CHILD";
  String LAST_KEY = "LAST_KEY";

  String MERGE_INTO_QS_FORMAT = "qsMergeFormat";
  String MERGE_INTO_QS_FORMAT_SCALAR = "scalar";
  String MERGE_INTO_QS_DATATYPE = "qsMergeDataType";

  /*
   *
   * To Be implemented by each reactor
   *
   */

  /**
   * Execute method - GREEDY translation
   *
   * @return
   */
  NounMetadata execute();

  /** */
  void In();

  /**
   * @return
   */
  Object Out();

  /**
   * @return
   */
  List<NounMetadata> getOutputs();

  /** */
  void mergeUp();

  /** */
  void updatePlan();

  /**
   * @return
   */
  Map<String, List<Map>> getStoreMap();

  /**
   * Sets the name of the operation and the signature full operation includes the nouns
   *
   * @param operation
   * @param fullOperation
   */
  void setPixel(String operation, String fullOperation);

  /**
   * @return
   */
  String[] getPixel();

  /**
   * set the parent reactor for a composition, we start here for a pipeline this will become the
   * child
   *
   * @param parentReactor
   */
  void setParentReactor(IReactor parentReactor);

  /**
   * @return
   */
  IReactor getParentReactor();

  /**
   * @param childReactor
   */
  void setChildReactor(IReactor childReactor);

  /**
   * @return
   */
  List<IReactor> getChildReactors();

  /**
   * sets the current noun it is working through
   *
   * @param noun
   */
  void curNoun(String noun);

  /**
   * returns the current row
   *
   * @return
   */
  GenRowStruct getCurRow();

  /**
   * completes the noun
   *
   * @param noun
   */
  void closeNoun(String noun);

  /**
   * gets the nounstore
   *
   * @return
   */
  NounStore getNounStore();

  /**
   * @param store
   */
  void setNounStore(NounStore store);

  // gets all the inputs i.e. the noun names
  // the second string is the meaning
  // gives JSON with the following values
  // name of the noun
  // Meaning of the noun
  // Optional / Mandatory
  // Single Value or multiple values
  // are projections the output ?
  List<NounMetadata> getInputs();

  /**
   * @return
   */
  STATUS getStatus();

  /**
   * @return
   */
  TYPE getType();

  /**
   * @return
   */
  String getName();

  /**
   * @param name
   */
  void setName(String name);

  /**
   * @param planner
   */
  void setPixelPlanner(PixelPlanner planner);

  /**
   * @return
   */
  PixelPlanner getPixelPlanner();

  /**
   * sets the string for alias i.e. as
   *
   * @param asName
   */
  void setAs(String[] asName);

  @Deprecated
  void setProp(String key, Object value);

  @Deprecated
  Object getProp(String key);

  @Deprecated
  boolean hasProp(String key);

  /**
   * map call implement if your type is map
   *
   * @param row
   * @return
   */
  IHeadersDataRow map(IHeadersDataRow row);

  /**
   * reduce call implement if the type is reduce
   *
   * @param iterator
   * @return
   */
  Object reduce(Iterator iterator);

  /**
   * @return
   */
  String getSignature();

  /**
   * @return
   */
  String getOriginalSignature();

  /**
   * @param stringToFind
   * @param stringReplacement
   */
  void modifySignature(String stringToFind, String stringReplacement);

  /** */
  void modifySignatureFromLambdas();

  /**
   * @param insight
   */
  void setInsight(Insight insight);

  /**
   * @param name
   * @return
   */
  Logger getLogger(String name);

  /**
   * @param name
   * @param partial
   * @return
   */
  Logger getLogger(String name, boolean partial);

  /**
   * @return
   */
  String getHelp();

  /**
   * @return
   */
  String getReactorDescription();

  /**
   * Determine if this reactor should be merged up to be put into a QS as is vs. executed directly
   *
   * @return
   */
  boolean canMergeIntoQs();

  /**
   * @return
   */
  Map<String, Object> mergeIntoQsMetadata();

  /**
   * @return
   */
  JSONObject asMcpTool();
}
