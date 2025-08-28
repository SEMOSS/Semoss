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
package prerna.reactor.insights.recipemanagement;

import java.util.List;
import java.util.Map;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractInsightParameterReactor extends AbstractReactor {

  /**
   * Get the input map
   *
   * @return
   */
  protected Map<String, Object> getParamMap() {
    GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PARAM_STRUCT.getKey());
    if (grs != null && !grs.isEmpty()) {
      Map<String, Object> mapInput = (Map<String, Object>) grs.get(0);
      return mapInput;
    }

    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      return (Map<String, Object>) mapInputs.get(0).getValue();
    }

    throw new NullPointerException("Could not find the input for the param struct map");
  }
}
