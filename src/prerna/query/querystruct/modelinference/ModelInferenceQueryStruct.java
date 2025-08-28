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
package prerna.query.querystruct.modelinference;

import java.util.Map;
import prerna.query.querystruct.AbstractQueryStruct;

public class ModelInferenceQueryStruct extends AbstractQueryStruct {

  protected String context = null;
  protected Map<String, Object> hyperParameters;

  public ModelInferenceQueryStruct() {
    this.qsType = QUERY_STRUCT_TYPE.ENGINE;
  }

  public void setHyperParameters(Map<String, Object> hyperParameters) {
    this.hyperParameters = hyperParameters;
  }

  public Map<String, Object> getHyperParameters() {
    return this.hyperParameters;
  }

  public void setContext(String context) {
    this.context = context;
  }

  public String getContext() {
    return this.context;
  }

  // TODO create methods for function on input
  // TODO create methods for function on input
  // TODO create methods for function on input
  // TODO create methods for function on input

  // TODO create methods for function on output
  // TODO create methods for function on output
  // TODO create methods for function on output
  // TODO create methods for function on output

  // TODO create methods for vector jsons?
  // TODO create methods for vector jsons?
  // TODO create methods for vector jsons?
}
