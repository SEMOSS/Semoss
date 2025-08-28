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
package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public interface ISemossException {

  /**
   * Get if this exception should stop the thread and return to the FE or if we should continue and
   * try to run through the other steps
   *
   * @return
   */
  boolean isContinueThreadOfExecution();

  /**
   * Set if this exception should stop the thread and return to the FE or if we should continue and
   * try to run through the other steps
   *
   * @return
   */
  void setContinueThreadOfExecution(boolean continueThreadOfExecution);

  /**
   * Get additional metadata to send to the FE when this exception occurs
   *
   * @return
   */
  NounMetadata getNoun();
}
