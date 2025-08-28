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
package prerna.reactor.utils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class RemoveFrameReactor extends RemoveVariableReactor {

  private static final String DROP_NOW_KEY = "dropNow";

  public RemoveFrameReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.VARIABLE.getKey(), DROP_NOW_KEY};
  }

  @Override
  public NounMetadata execute() {
    String variableName = this.curRow.get(0).toString();
    if (dropNow()) {
      return InsightUtility.removeFrameVaraible(this.insight.getVarStore(), variableName);
    }

    NounMetadata noun =
        new NounMetadata(
            variableName,
            PixelDataType.REMOVE_VARIABLE,
            PixelOperationType.REMOVE_FRAME,
            PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);

    // make sure it is a valid removal
    NounMetadata var = this.insight.getVarStore().get(variableName);
    if (var == null) {
      noun.addAdditionalReturn(
          NounMetadata.getWarningNounMessage("Could not find variable to remove"));
    } else if (var.getNounType() != PixelDataType.FRAME) {
      noun.addAdditionalReturn(
          NounMetadata.getWarningNounMessage("Trying to remove a variable that is not a frame"));
    }

    return noun;
  }
}
