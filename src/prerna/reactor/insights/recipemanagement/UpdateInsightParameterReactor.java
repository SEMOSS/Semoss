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
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateInsightParameterReactor extends AbstractInsightParameterReactor {

  public UpdateInsightParameterReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PARAM_STRUCT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // get the parameter as a map of key=value pairs
    Map<String, Object> paramMap = getParamMap();
    // turn this into a param struct object
    ParamStruct paramStruct = ParamStruct.generateParamStruct(paramMap);
    // validate the paramStruct
    // right now - only check is based on the data types
    List<ParamStructDetails> details = paramStruct.getDetailsList();
    PixelDataType curDataType = null;
    for (ParamStructDetails detail : details) {
      // null case if the first time
      if (curDataType == null) {
        curDataType = detail.getType();
      }
      // need to compare and ensure they are the same
      else if (curDataType != detail.getType()) {
        throw new IllegalArgumentException(
            "Cannot append to existing because the data type of '"
                + detail.getColumnName()
                + "' is '"
                + detail.getType()
                + "' and does not match the existing data type of '"
                + curDataType
                + "'");
      }
    }
    String paramName = paramStruct.getParamName();
    // parameter name must be defined
    if (paramName == null || paramName.isEmpty()) {
      throw new IllegalArgumentException("Parameter name is not defined");
    }
    // parameter shouldn't already exists
    String variableName = VarStore.PARAM_STRUCT_PREFIX + paramName;
    if (!this.insight.getVarStore().getInsightParameterKeys().contains(variableName)) {
      throw new IllegalArgumentException("Could not find parameter with name = " + paramName);
    }

    NounMetadata pStructNoun = new NounMetadata(paramStruct, PixelDataType.PARAM_STRUCT);
    this.insight.getVarStore().put(variableName, pStructNoun);
    return pStructNoun;
  }
}
